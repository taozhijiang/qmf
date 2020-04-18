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
  using connections_ptr_type =
    std::shared_ptr<std::map<int, std::shared_ptr<Connection>>>;

 public:
  Scheduler(const std::string& addr, int32_t port) : addr_(addr), port_(port) {
  }

  bool init() {

    if (!start_listen())
      return false;

    connections_ptr_ =
      std::make_shared<std::map<int, std::shared_ptr<Connection>>>();
    if (!connections_ptr_) {
      LOG(ERROR) << "create Connections failed.";
      return false;
    }

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

  // 数据推送
  bool push_all_rating(const std::shared_ptr<TaskDef>& taskdef);
  bool push_all_fixed(const std::shared_ptr<TaskDef>& taskdef);
  size_t connections_count();
  size_t connections_count(enum LaborStatus status,
                           const std::shared_ptr<TaskDef>& taskdef);

  std::unique_ptr<Select> select_ptr_;

  // 保留所有客户端的连接
  // 每次任务执行的开始，使用一个快照；更新的时候也是使用智能指针保护

 private:
  std::mutex connections_mutex_;
  connections_ptr_type connections_ptr_;

  bool terminate_ = false;

  const std::string addr_;
  const int32_t port_;
  bool start_listen();

  EQueue<std::shared_ptr<TaskDef>> task_queue_;
  std::thread task_thread_;
  void task_run();
  bool RunOneTask(const std::shared_ptr<TaskDef>& taskdef);

  std::unique_ptr<BigData> bigdata_ptr_;
};

} // end namespace scheduler
} // end namespace distributed

#endif // __DISTRIBUTED_SCHEDULER_SCHEDULER_H__