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
#include <atomic> // std::atomic_flag

#include <distributed/common/Common.h>
#include <distributed/common/Message.h>

#include <glog/logging.h>

namespace distributed {
namespace scheduler {

class Scheduler;

struct Select {

  Select(int socket) : listenfd_(socket) {
    maxfd_ = listenfd_;
    FD_ZERO(&readfds_);
    FD_SET(listenfd_, &readfds_);
  }

  void add_fd(int socketfd) {
    maxfd_ = socketfd > maxfd_ ? socketfd : maxfd_;
    FD_SET(socketfd, &readfds_);
    VLOG(3) << "add fd " << socketfd << ", maxfd " << maxfd_;
  }

  void del_fd(int socketfd) {

    FD_CLR(socketfd, &readfds_);

    int n = 0;
    for (size_t i = 0; i <= maxfd_; ++i) {
      if (FD_ISSET(i, &readfds_) && i > n)
        n = i;

      if (FD_ISSET(i, &readfds_)) {
        VLOG(3) << "current active: " << i;
      }
    }
    maxfd_ = n;
    VLOG(3) << "del fd " << socketfd << ", maxfd " << maxfd_;
  }

  int listenfd_ = 0;
  int maxfd_ = 0;
  fd_set readfds_;
};

class Connection {

  friend class Scheduler;

 public:
  Connection(Scheduler& scheduler,
             const std::string& addr,
             int port,
             int socket)
    : scheduler_(scheduler),
      addr_(addr),
      port_(port),
      socket_(socket),
      taskid_(0),
      epchoid_(0) {

    stage_ = Stage::kHead;
    head_idx_ = 0;
    timestamp_ = ::time(NULL);
  }

  // critical error return false;
  bool event();

  bool handle_head();
  bool handle_body();

  std::string addr() const {
    std::stringstream ss;
    ss << "(" << socket_ << ") " << addr_ << ":" << port_;
    return ss.str();
  }

  void reset() {

    head_idx_ = 0;
    data_idx_ = 0;

    stage_ = Stage::kHead;
    is_busy_ = false;
  }

  // when Labor has some problem and Scheduler need compute resources, the
  // Scheduler will check this and send kHeartBeat if need.
  void touch() {
    timestamp_ = ::time(NULL);
  }

  bool is_stale(time_t period) {
    return ::time(NULL) - timestamp_ > period;
  }

 public:
  // back pointer
  Scheduler& scheduler_;

  const std::string addr_;
  const int port_;
  const int socket_;
  std::atomic_flag lock_socket_ = ATOMIC_FLAG_INIT;
  // latest action of this connection
  time_t timestamp_;
  time_t bucket_start_;

  // indicats when the corresponding Labor has already been distributed a
  // calcuate task
  bool is_busy_ = false;

 private:
  // because of the Scheduler uniform "select" designe, the wals_submit will
  // also be legal client, we should avoid send task to them even just in some
  // critical case
  bool is_labor_ = false;

  // the taskid_ and epchoid_ somehow indicates the client's status, but
  // remember in distributed situation, it is not in strong consensus.
  uint32_t taskid_ = 0;
  uint32_t epchoid_ = 0;

  enum class Stage {
    kHead = 1, // in reading head period
    kBody = 2, // in reading body period
    kDone = 3, // already processed
  } stage_;

  Head head_;
  int head_idx_ = 0;

  // use vector try to reuse mem
  std::vector<char> data_;
  int data_idx_ = 0;
};

} // end namespace scheduler
} // end namespace distributed

#endif // __DISTRIBUTED_SCHEDULER_CONNECTION_H__
