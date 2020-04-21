/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#include "Scheduler.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <chrono> // std::chrono::seconds

#include <distributed/common/SendOps.h>
#include <distributed/common/NetUtil.h>

#include <glog/logging.h>

namespace distributed {
namespace scheduler {

bool Scheduler::init() {

  if (!start_listen())
    return false;

  connections_ptr_ = std::make_shared<connections_type>();
  if (!connections_ptr_) {
    LOG(ERROR) << "create Connections failed.";
    return false;
  }

  bigdata_ptr_ = std::make_unique<BigData>();
  if (!bigdata_ptr_) {
    LOG(ERROR) << "create BigData failed.";
    return false;
  }

  engine_ptr_ = std::make_unique<qmf::WALSEngineLite>(bigdata_ptr_);
  if (!engine_ptr_) {
    LOG(ERROR) << "create WALSEngineLite failed.";
    return false;
  }

  return true;
}

// tcp backlog size
static const size_t kBacklog = 10;

bool Scheduler::start_listen() {

  int socketfd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (socketfd < 0) {
    LOG(ERROR) << "create socket error: " << ::strerror(errno);
    return false;
  }

  bool success = false;
  do {

    /* Enable the socket to reuse the address */
    int reuseaddr = 1;
    if (::setsockopt(
          socketfd, SOL_SOCKET, SO_REUSEADDR, &reuseaddr, sizeof(int)) < 0) {
      LOG(ERROR) << "reuse address failed: " << ::strerror(errno);
      break;
    }

    NetUtil::optimize_send_recv_buff(socketfd);

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<short>(port_));
    addr.sin_addr.s_addr = inet_addr(addr_.c_str());

    if (::bind(socketfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
      LOG(ERROR) << "bind error: " << ::strerror(errno);
      break;
    }

    select_ptr_ = std::make_unique<Select>(socketfd);
    if (!select_ptr_) {
      LOG(ERROR) << "create Select object failed.";
      break;
    }

    if (::listen(socketfd, kBacklog) < 0) {
      LOG(ERROR) << "bind error: " << ::strerror(errno);
      break;
    }

    // start up task thread
    task_thread_ = std::thread(std::bind(&Scheduler::task_run, this));

    success = true;

  } while (0);

  if (!success) {
    close(socketfd);
    return false;
  }

  LOG(INFO) << "scheduler listen to " << addr_ << ":" << port_
            << " successfully!";
  return true;
}

void Scheduler::select_loop() {

  LOG(INFO) << "start select loop thread ...";

  struct timeval tv;

  while (!terminate_) {

    // every second wake up
    tv.tv_sec = 1;
    tv.tv_usec = 0;

    int maxfd = select_ptr_->maxfd_;
    fd_set rfds = select_ptr_->readfds_;
    int retval = ::select(maxfd + 1, &rfds, NULL, NULL, &tv);

    if (retval < 0) {
      LOG(ERROR) << "select error, critical problem: " << strerror(errno);

      // Terminate ??
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;

    } else if (retval == 0) {

      // LOG(INFO) << "select timeout";
      // VLOG(3) << "select timeout";

    } else {

      for (int ss = 0; ss <= maxfd; ++ss) {

        if (!FD_ISSET(ss, &rfds))
          continue;

        if (ss == select_ptr_->listenfd_) {

          // handle new client

          struct sockaddr_in peer_addr;
          socklen_t sz = sizeof(struct sockaddr_in);
          int sock =
            ::accept(select_ptr_->listenfd_, (struct sockaddr*)&peer_addr, &sz);
          if (sock == -1) {

            LOG(ERROR) << "accept new client failed: " << strerror(errno);

          } else {

            std::string addr = inet_ntoa(peer_addr.sin_addr);
            int port = htons(peer_addr.sin_port);
            LOG(INFO) << "accept new client from " << addr << ":" << port;

            NetUtil::optimize_send_recv_buff(sock);

            auto connection =
              std::make_shared<Connection>(*this, addr, port, sock);
            if (connection) {

              {
                const std::lock_guard<std::mutex> lock(connections_mutex_);
                (*connections_ptr_)[sock] = connection;
              }

              select_ptr_->add_fd(sock);
              LOG(INFO) << "add new Connection successfully.";
            } else {
              LOG(ERROR) << "create Connection instance failed.";
            }
          }

          continue;
        }

        // handle normal socket event
        handle_read(ss);

      } // end for
    }
  }

  LOG(INFO) << "terminate select loop thread ...";
}

void Scheduler::handle_read(int socket) {

  connections_ptr_type copy_connections = share_connections_ptr();

  auto iter = copy_connections->find(socket);
  if (iter == copy_connections->end()) {
    LOG(ERROR) << "socket not found in connections.";
    select_ptr_->del_fd(socket);
    return;
  }

  auto connection = iter->second;
  if (!connection->event()) {

    // not select anymore
    select_ptr_->del_fd(socket);

    {
      const std::lock_guard<std::mutex> lock(connections_mutex_);
      connections_ptr_->erase(socket);
    }

    LOG(INFO) << "critical error, destroy the connection: " << socket
              << std::endl
              << "remote address: " << connection->addr_ << ":"
              << connection->port_;
  }
}

bool Scheduler::push_all_rating_matrix() {

  connections_ptr_type copy_connections = share_connections_ptr();

  // TODO: 今后如果没有没有发现labor，则scheduler执行单机计算
  if (copy_connections->empty()) {
    LOG(ERROR) << "no labor available now.";
    return false;
  }

  for (auto iter = copy_connections->begin(); iter != copy_connections->end();
       ++iter) {

    auto connection = iter->second;
    if (!connection->is_labor_)
      continue;

    if (connection->lock_socket_.test_and_set()) {
      LOG(INFO) << "connection socket used by other ..." << connection->addr();
      continue;
    }

    const auto& dataset = bigdata_ptr_->rating_vec_;

    const char* dat = reinterpret_cast<const char*>(dataset.data());
    uint64_t len = sizeof(dataset[0]) * dataset.size();

    connection->touch();
    if (!SendOps::send_bulk(connection->socket_, OpCode::kPushRate, dat, len,
                            bigdata_ptr_->taskid(), bigdata_ptr_->epchoid(),
                            bigdata_ptr_->nfactors(), 0, bigdata_ptr_->lambda(),
                            bigdata_ptr_->confidence())) {
      LOG(ERROR) << "sending rating to " << connection->addr() << " failed.";
    }

    connection->lock_socket_.clear();
  }

  return true;
}

bool Scheduler::push_all_fixed_factors() {

  connections_ptr_type copy_connections = share_connections_ptr();

  // TODO: 今后如果没有没有发现labor，则scheduler执行单机计算
  if (copy_connections->empty()) {
    LOG(ERROR) << "no labor available now.";
    return false;
  }

  for (auto iter = copy_connections->begin(); iter != copy_connections->end();
       ++iter) {

    auto connection = iter->second;
    if (!connection->is_labor_)
      continue;

    if (connection->lock_socket_.test_and_set()) {
      LOG(INFO) << "connection socket used by other ..." << connection->addr();
      continue;
    }

    // epcho_id_ = 1, 3, 5, ... fix item, cal user
    // epcho_id_ = 2, 4, 6, ... fix user, cal item

    const char* dat = nullptr;
    uint64_t len = 0;
    if (bigdata_ptr_->epchoid() % 2) {
      const qmf::Matrix& matrix = bigdata_ptr_->item_factor_ptr_->getFactors();
      dat =
        reinterpret_cast<const char*>(const_cast<qmf::Matrix&>(matrix).data());
      len = sizeof(qmf::Matrix::value_type) * matrix.nrows() * matrix.ncols();
      LOG(INFO) << "{taskid:" << bigdata_ptr_->taskid()
                << ", epchoid:" << bigdata_ptr_->epchoid()
                << "} transform itemFactors with size " << len;
    } else {
      const qmf::Matrix& matrix = bigdata_ptr_->user_factor_ptr_->getFactors();
      dat =
        reinterpret_cast<const char*>(const_cast<qmf::Matrix&>(matrix).data());
      len = sizeof(qmf::Matrix::value_type) * matrix.nrows() * matrix.ncols();
      LOG(INFO) << "{taskid:" << bigdata_ptr_->taskid()
                << ", epchoid:" << bigdata_ptr_->epchoid()
                << "} transform userFactors with size " << len;
    }

    connection->touch();
    if (!SendOps::send_bulk(connection->socket_, OpCode::kPushFixed, dat, len,
                            bigdata_ptr_->taskid(), bigdata_ptr_->epchoid(),
                            bigdata_ptr_->nfactors(), 0, bigdata_ptr_->lambda(),
                            bigdata_ptr_->confidence())) {
      LOG(ERROR) << "sending fixed factors to " << connection->addr()
                 << " failed.";
    }

    connection->lock_socket_.clear();
  }

  return true;
}

// already lock the socketfd outside
bool Scheduler::push_bucket(uint32_t bucket_idx, int socketfd) {

  const std::string msg = "CA";
  if (!SendOps::send_message(
        socketfd, OpCode::kCalc, msg, bigdata_ptr_->taskid(),
        bigdata_ptr_->epchoid(), bigdata_ptr_->nfactors(), bucket_idx,
        bigdata_ptr_->lambda(), bigdata_ptr_->confidence())) {
    LOG(ERROR) << "sending fixed to " << socketfd << " failed.";
    return false;
  }

  return true;
}

void Scheduler::push_heartbeat(std::shared_ptr<Connection>& connection) {

  const std::string msg = "HB";

  if (connection->lock_socket_.test_and_set()) {
    LOG(INFO) << "connection socket used by other ..." << connection->addr();
    return;
  }

  connection->touch();
  if (!SendOps::send_message(
        connection->socket_, OpCode::kHeartBeat, msg, bigdata_ptr_->taskid(),
        bigdata_ptr_->epchoid(), bigdata_ptr_->nfactors(), 0,
        bigdata_ptr_->lambda(), bigdata_ptr_->confidence())) {
    LOG(ERROR) << "sending heartbeat to " << connection->addr() << " failed.";
  }

  connection->lock_socket_.clear();
}

size_t Scheduler::connections_count(bool check) {

  connections_ptr_type copy_connections = share_connections_ptr();
  size_t count = 0;

  for (auto iter = copy_connections->begin(); iter != copy_connections->end();
       ++iter) {

    auto connection = iter->second;
    if (!connection->is_labor_)
      continue;

    if (!check) {
      ++count;
    } else if (connection->taskid_ == bigdata_ptr_->taskid() &&
               connection->epchoid_ == bigdata_ptr_->epchoid()) {
      ++count;
    } else {

      // need check, but connection status is old
      // stale detection
      time_t timeout = kHeartBeatInternal;
      if (connection->is_stale(timeout)) {
        push_heartbeat(connection);
        LOG(INFO) << "connection " << connection->addr() << " is stale for "
                  << timeout << " seconds, send kHeartBeat message.";
      }
    }
  }

  return count;
}

void Scheduler::task_run() {

  LOG(INFO) << "start task loop thread ...";

  while (!terminate_) {

    std::shared_ptr<TaskDef> task_instance{};
    if (!task_queue_.POP(task_instance, 1000 /*1s*/) || !task_instance) {
      // ---
      continue;
    }

    if (RunOneTask(task_instance)) {
      LOG(INFO) << "RunOneTask of " << task_instance->train_set()
                << " successfully.";
    } else {
      LOG(ERROR) << "RunOneTask of " << task_instance->train_set()
                 << " failed.";
    }
  }

  LOG(INFO) << "terminate task loop thread ...";
}

} // end namespace scheduler
} // end namespace distributed