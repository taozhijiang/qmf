/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#ifndef __DISTRIBUTED_LABOR_LABOR_H__
#define __DISTRIBUTED_LABOR_LABOR_H__

#include <cstdint>
#include <string>

#include <qmf/wals/WALSEngineLite.h>

#include <distributed/common/BigData.h>
#include <distributed/common/Common.h>
#include <distributed/common/Message.h>

namespace distributed {
namespace labor {

class Labor {

 public:
  Labor(const std::string& addr, int32_t port)
    : addr_(addr), port_(port), terminate_(false) {
  }

  bool init();
  void loop();
  void terminate() {
    terminate_ = true;
  }

 private:
  int socketfd_ = -1;

  const std::string addr_;
  const int32_t port_;
  bool start_connect();
  bool start_attach();

  bool handle_head();

  bool terminate_ = false;

  Head head_;
  int head_idx_;

  std::unique_ptr<BigData> bigdata_ptr_;
  std::unique_ptr<qmf::WALSEngineLite> engine_ptr_;
};

} // end namespace labor
} // end namespace distributed

#endif // __DISTRIBUTED_LABOR_LABOR_H__