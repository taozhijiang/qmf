/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#ifndef __DISTRIBUTED_SCHEDULER_CONNECTION_H__
#define __DISTRIBUTED_SCHEDULER_CONNECTION_H__

#include <sys/select.h>

#include <sstream>
#include <string>
#include <vector>

#include <distributed/common/Common.h>
#include <distributed/common/Message.h>

namespace distributed {
namespace scheduler {

struct Select {

  Select(int socket) : listenfd_(socket) {
    maxfd_ = listenfd_;
    FD_ZERO(&readfds_);
    FD_SET(listenfd_, &readfds_);
  }

  void add_fd(int socketfd) {
    maxfd_ = socketfd > maxfd_ ? socketfd : maxfd_;
    FD_SET(socketfd, &readfds_);
  }

  void del_fd(int socketfd) {

    FD_CLR(socketfd, &readfds_);

    int n = 0;
    for (size_t i = 0; i < maxfd_; ++i) {
      if (FD_ISSET(i, &readfds_) && i > n)
        n = i;
    }
    maxfd_ = n;
  }

  int listenfd_ = 0;
  int maxfd_ = 0;
  fd_set readfds_;
};

class Connection {

 public:
  Connection(const std::string& addr, int port, int socket)
    : addr_(addr), port_(port), socket_(socket) {

    // first initial status
    status_ = LaborStatus::kAttach;

    stage_ = Stage::kHead;
    head_idx_ = 0;
  }

  // critical error return false;
  bool event();

  bool handle_head();
  bool handle_body();

  std::string self() const {
    std::stringstream ss;
    ss << "(" << socket_ << ") " << addr_ << ":" << port_;
    return ss.str();
  }

  void reset() {
    head_idx_ = 0;
    data_idx_ = 0;
    stage_ = Stage::kHead;
  }

 public:
  const std::string addr_;
  const int port_;
  const int socket_;

 private:
  LaborStatus status_;

  enum class Stage {
    kHead = 1, // 读取头阶段
    kBody = 2, // 读取Body阶段
    kDone = 3, // 等待处理数据
  } stage_;

  Head head_;
  int head_idx_;

  // use vector try to reuse mem
  std::vector<char> data_;
  int data_idx_;
};

} // end namespace scheduler
} // end namespace distributed

#endif // __DISTRIBUTED_SCHEDULER_CONNECTION_H__
