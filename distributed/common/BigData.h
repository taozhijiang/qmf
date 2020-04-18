/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#ifndef __DISTRIBUTED_COMMON_BIGDATA_H__
#define __DISTRIBUTED_COMMON_BIGDATA_H__

#include <cstdlib>
#include <cstdint>

#include <qmf/DatasetReader.h>
// Matrix内部是std::vector<Double>存储的！
#include <qmf/Matrix.h>
#include <qmf/FactorData.h>

namespace distributed {

// 每一个任务、每一轮迭代的时候都需要更新
struct BigData {

  BigData() {
    ::srandom(::time(NULL));
    task_id_ = ::random() & 0xFFFF;
  }

  uint32_t task_id() const {
    return task_id_;
  }

  uint32_t epcho_id() const {
    return epcho_id_;
  }

  uint32_t incr_epcho() {
    return ++ epcho_id_;
  }

  // 用户评价矩阵
  std::vector<qmf::DatasetElem> rating_vec_;

  // epcho_id_ = 1, 3, 5, ... fix item, cal user
  // epcho_id_ = 2, 4, 6, ... fix user, cal item

  std::shared_ptr<qmf::FactorData> item_factor_ptr_;
  std::shared_ptr<qmf::FactorData> user_factor_ptr_;

  // reset for new task
  void start_term() {
    ++task_id_;
    epcho_id_ = 0;
  }

  uint32_t task_id_;  // 任务ID
  uint32_t epcho_id_; // epcho迭代ID
};

} // end namespace distributed

#endif // __DISTRIBUTED_COMMON_BIGDATA_H__
