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

      for (int s = 0; s <= select_ptr_->maxfd_; ++s) {

        if (!FD_ISSET(s, &rfds))
          continue;

        if (s == select_ptr_->listenfd_) {

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
              connections_ptr_[sock] = connection;
              select_ptr_->add_fd(sock);
              LOG(INFO) << "add new Connection successfully.";
            } else {
              LOG(ERROR) << "create Connection instance failed.";
            }
          }

          continue;
        }

        VLOG(3) << "normal socket event :" << s;
        handle_read(s);

      } // end for
    }
  }

  LOG(INFO) << "terminate select loop thread ...";
}

void Scheduler::handle_read(int socket) {

  auto iter = connections_ptr_.find(socket);
  if (iter == connections_ptr_.end()) {
    LOG(ERROR) << "socket not found in connections.";
    select_ptr_->del_fd(socket);
    return;
  }

  auto connection = iter->second;
  if (!connection->event()) {
    select_ptr_->del_fd(socket);
    connections_ptr_.erase(socket);
    LOG(INFO) << "critical error, destroy the connection: " << socket
              << std::endl
              << "remote address: " << connection->addr_ << ":"
              << connection->port_;
  }
}

bool Scheduler::push_all_rating(const std::shared_ptr<TaskDef>& taskdef) {

  // TODO: 今后如果没有没有发现labor，则scheduler执行单机计算
  if (connections_ptr_.empty()) {
    LOG(ERROR) << "no labor available now.";
    return false;
  }

  for (auto iter = connections_ptr_.begin(); iter != connections_ptr_.end();
       ++iter) {

    auto connection = iter->second;
    if (!connection->is_labor_)
      continue;

    connection->status_ = LaborStatus::kAttach;
    const char* dat =
      reinterpret_cast<const char*>(bigdata_ptr_->rating_vec_.data());
    uint64_t len =
      sizeof(bigdata_ptr_->rating_vec_[0]) * bigdata_ptr_->rating_vec_.size();
    bool retval = SendOps::send_bulk(connection->socket_, OpCode::kPushRate,
                                     bigdata_ptr_->task_id(),
                                     bigdata_ptr_->epcho_id(), dat, len);

    if (!retval) {
      LOG(ERROR) << "sending rating to " << connection->self() << " failed.";
    } else {
      LOG(INFO) << "sending to " << connection->self();
    }
  }

  return true;
}

bool Scheduler::push_all_fixed(const std::shared_ptr<TaskDef>& taskdef) {

  // TODO: 今后如果没有没有发现labor，则scheduler执行单机计算
  if (connections_ptr_.empty()) {
    LOG(ERROR) << "no labor available now.";
    return false;
  }

  for (auto iter = connections_ptr_.begin(); iter != connections_ptr_.end();
       ++iter) {

    auto connection = iter->second;
    if (!connection->is_labor_)
      continue;

    // TODO: 后续补发Rate
    if (connection->status_ == LaborStatus::kAttach) {
      continue;
    }

    const char* dat = nullptr;
    uint64_t len = 0;
    if (bigdata_ptr_->epcho_id() % 2) {
      const qmf::Matrix& matrix = bigdata_ptr_->user_factor_ptr_->getFactors();
      dat = reinterpret_cast<const char*>(const_cast<qmf::Matrix&>(matrix).data());
      len = sizeof(qmf::Matrix::value_type) * matrix.nrows() * matrix.ncols();
      LOG(INFO) << "epcho_id " << bigdata_ptr_->epcho_id() << " transform userFactors with size " << len;
    } else {
      const qmf::Matrix& matrix = bigdata_ptr_->user_factor_ptr_->getFactors();
      dat = reinterpret_cast<const char*>(const_cast<qmf::Matrix&>(matrix).data());
      len = sizeof(qmf::Matrix::value_type) * matrix.nrows() * matrix.ncols();
      LOG(INFO) << "epcho_id " << bigdata_ptr_->epcho_id() << " transform itemFactors with size " << len;
    }

    bool retval = SendOps::send_bulk(connection->socket_, OpCode::kPushFixed,
                                     bigdata_ptr_->task_id(),
                                     bigdata_ptr_->epcho_id(), dat, len);

    if (!retval) {
      LOG(ERROR) << "sending fixed to " << connection->self() << " failed.";
    } else {
      LOG(INFO) << "sending to " << connection->self();
    }
  }

  return true;
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