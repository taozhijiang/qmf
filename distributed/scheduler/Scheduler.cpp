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

#include <glog/logging.h>

namespace distributed {
namespace scheduler {

static const size_t kBacklog = 10;

bool Scheduler::start_listen() {

  int socketfd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (socketfd < 0) {
    LOG(ERROR) << "create socket error: " << ::strerror(errno);
    return -1;
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
    addr.sin_addr.s_addr = inet_addr(ip_.c_str());

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

    // start loop thread
    select_thread_ptr_ =
      std::make_unique<std::thread>(&Scheduler::select_loop_run, this);
    if (!select_thread_ptr_) {
      LOG(ERROR) << "create select thread failed.";
      break;
    }

    success = true;

  } while (0);

  if (!success) {
    close(socketfd);
    return false;
  }

  LOG(INFO) << "scheduler listen to " << ip_ << ":" << port_
            << " successfully!";
  return true;
}

void Scheduler::select_loop_run() {

  LOG(INFO) << "start select loop thread run.";

  struct timeval tv;

  while (!terminate_) {

    // every 5 second wake up
    tv.tv_sec = 5;
    tv.tv_usec = 0;

    fd_set rfds = select_ptr_->readfds_;
    int retval = ::select(select_ptr_->maxfd_ + 1, &rfds, NULL, NULL, &tv);
    if (retval < 0) {
      LOG(ERROR) << "select error, critical problem: " << strerror(errno);

      // Terminate ??
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;

    } else if (retval == 0) {

      // LOG(INFO) << "select timeout";

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

            auto connection = std::make_shared<Connection>(addr, port, sock);
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

        handle_read(s);

      } // end for
    }
  }

  LOG(INFO) << "terminate select loop thread run.";
}

void Scheduler::handle_read(int socket) {

  auto iter = connections_ptr_.find(socket);
  if (iter == connections_ptr_.end()) {
    LOG(ERROR) << "socket now found in connections.";
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

} // end namespace scheduler
} // end namespace distributed