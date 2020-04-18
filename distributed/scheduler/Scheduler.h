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
#include <distributed/proto/task.pb.h>

#include <distributed/common/EQueue.h>
#include <distributed/common/BigData.h>

#include <glog/logging.h>

namespace distributed {
namespace scheduler {


class Scheduler {

 public:
  Scheduler(const std::string& addr, int32_t port) : addr_(addr), port_(port) {
  }

  bool init() {

    if (!start_listen())
      return false;

    bigdata_ptr_ = std::make_unique<BigData>();
    if (!bigdata_ptr_) {
      LOG(ERROR) << "create BigData failed.";
      return false;
    }

    return true;
  }

  void select_loop();
  void terminate() {
    terminate_ = true;
  }

  void add_task(const std::shared_ptr<TaskDef>& task) {
    task_queue_.PUSH(task);
  }

 private:
  void handle_read(int socket);

  std::unique_ptr<Select> select_ptr_;

  // 保留所有客户端的连接
  std::map<int, std::shared_ptr<Connection>> connections_ptr_;

 private:
  bool terminate_ = false;

  const std::string addr_;
  const int32_t port_;
  bool start_listen();

  EQueue<std::shared_ptr<TaskDef>> task_queue_;
  std::thread task_thread_;
  void task_run();

  std::unique_ptr<BigData> bigdata_ptr_;
};

} // end namespace scheduler
} // end namespace distributed

#endif // __DISTRIBUTED_SCHEDULER_SCHEDULER_H__