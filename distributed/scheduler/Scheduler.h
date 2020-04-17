/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#ifndef __DISTRIBUTED_SCHEDULER_SCHEDULER_H__
#define __DISTRIBUTED_SCHEDULER_SCHEDULER_H__

#include <sys/select.h>

#include <string>
#include <thread>
#include <map>

#include <distributed/scheduler/Connection.h>

namespace distributed {
namespace scheduler {

class Scheduler {

 public:
  Scheduler(const std::string& ip, int32_t port) : ip_(ip), port_(port) {
  }

  bool init() {

    if(!start_listen()){
      return false;
    }

    return true;
  }

 private:
  bool start_listen();
  void handle_read(int socket);

  std::unique_ptr<Select> select_ptr_;
  std::unique_ptr<std::thread> select_thread_ptr_;
  void select_loop_run();

  // 保留所有客户端的连接
  std::map<int, std::shared_ptr<Connection>> connections_ptr_;

 private:
  bool terminate_ = false;

  const std::string ip_;
  const int32_t port_;
};

} // end namespace scheduler
} // end namespace distributed

#endif // __DISTRIBUTED_SCHEDULER_SCHEDULER_H__