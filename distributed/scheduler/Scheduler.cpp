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

#include <glog/logging.h>

namespace distributed {
namespace scheduler {

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

    size_t maxfd = select_ptr_->maxfd_;
    fd_set rfds = select_ptr_->readfds_;
    int retval = ::select(select_ptr_->maxfd_ + 1, &rfds, NULL, NULL, &tv);

    if (retval < 0) {
      LOG(ERROR) << "select error, critical problem: " << strerror(errno);

      // Terminate ??
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;

    } else if (retval == 0) {

      // LOG(INFO) << "select timeout";
      VLOG(3) << "select timeout";

    } else {

      for (int ss = 0; ss <= select_ptr_->maxfd_; ++ss) {

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

        VLOG(3) << "normal socket event :" << ss;
        handle_read(ss);

      } // end for
    }
  }

  LOG(INFO) << "terminate select loop thread ...";
}

void Scheduler::handle_read(int socket) {

  connections_ptr_type copy_connections_ptr{};
  {
    const std::lock_guard<std::mutex> lock(connections_mutex_);
    copy_connections_ptr = connections_ptr_;
  }

  auto iter = copy_connections_ptr->find(socket);
  if (iter == copy_connections_ptr->end()) {
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

bool Scheduler::push_all_rating(const std::shared_ptr<TaskDef>& taskdef) {

  connections_ptr_type connections{};
  {
    const std::lock_guard<std::mutex> lock(connections_mutex_);
    connections = connections_ptr_;
  }

  // TODO: 今后如果没有没有发现labor，则scheduler执行单机计算
  if (connections->empty()) {
    LOG(ERROR) << "no labor available now.";
    return false;
  }

  for (auto iter = connections->begin(); iter != connections->end(); ++iter) {

    auto connection = iter->second;
    if (!connection->is_labor_)
      continue;

    const auto& dataset = bigdata_ptr_->rating_vec_;

    const char* dat = reinterpret_cast<const char*>(dataset.data());
    uint64_t len = sizeof(dataset[0]) * dataset.size();

    if (!SendOps::send_bulk(connection->socket_, OpCode::kPushRate, dat, len,
                            bigdata_ptr_->taskid(), bigdata_ptr_->epchoid())) {
      LOG(ERROR) << "sending rating to " << connection->self() << " failed.";
    }
  }

  return true;
}

bool Scheduler::push_all_fixed(const std::shared_ptr<TaskDef>& taskdef) {

  connections_ptr_type connections{};
  {
    const std::lock_guard<std::mutex> lock(connections_mutex_);
    connections = connections_ptr_;
  }

  // TODO: 今后如果没有没有发现labor，则scheduler执行单机计算
  if (connections->empty()) {
    LOG(ERROR) << "no labor available now.";
    return false;
  }

  for (auto iter = connections->begin(); iter != connections->end(); ++iter) {

    auto connection = iter->second;
    if (!connection->is_labor_)
      continue;

    // epcho_id_ = 1, 3, 5, ... fix item, cal user
    // epcho_id_ = 2, 4, 6, ... fix user, cal item

    const char* dat = nullptr;
    uint64_t len = 0;
    if (bigdata_ptr_->epchoid() % 2) {
      const qmf::Matrix& matrix = bigdata_ptr_->item_factor_ptr_->getFactors();
      dat =
        reinterpret_cast<const char*>(const_cast<qmf::Matrix&>(matrix).data());
      len = sizeof(qmf::Matrix::value_type) * matrix.nrows() * matrix.ncols();
      LOG(INFO) << "epcho_id " << bigdata_ptr_->epchoid()
                << " transform itemFactors with size " << len;
    } else {
      const qmf::Matrix& matrix = bigdata_ptr_->user_factor_ptr_->getFactors();
      dat =
        reinterpret_cast<const char*>(const_cast<qmf::Matrix&>(matrix).data());
      len = sizeof(qmf::Matrix::value_type) * matrix.nrows() * matrix.ncols();
      LOG(INFO) << "epcho_id " << bigdata_ptr_->epchoid()
                << " transform userFactors with size " << len;
    }

    if (!SendOps::send_bulk(connection->socket_, OpCode::kPushFixed, dat, len,
                            bigdata_ptr_->taskid(), bigdata_ptr_->epchoid(),
                            bigdata_ptr_->nfactors())) {
      LOG(ERROR) << "sending fixed to " << connection->self() << " failed.";
    }
  }

  return true;
}

size_t Scheduler::connections_count(bool check) {

  connections_ptr_type connections{};
  {
    const std::lock_guard<std::mutex> lock(connections_mutex_);
    connections = connections_ptr_;
  }

  size_t count = 0;

  for (auto iter = connections->begin(); iter != connections->end(); ++iter) {

    auto connection = iter->second;
    if (!connection->is_labor_)
      continue;

    if (!check) {
      ++count;
    } else if (connection->task_id_ == bigdata_ptr_->taskid() &&
               connection->epcho_id_ == bigdata_ptr_->epchoid()) {
      ++count;
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