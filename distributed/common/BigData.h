/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#ifndef __DISTRIBUTED_COMMON_BIGDATA_H__
#define __DISTRIBUTED_COMMON_BIGDATA_H__

#include <cstdint>

// Matrix内部是std::vector<Double>存储的！
#include <qmf/Matrix.h>

namespace distributed {


// 每一个任务、每一轮迭代的时候都需要更新
struct BigData {

  uint32_t task_id_;  // 任务ID
  uint32_t epcho_id_; // epcho迭代ID

  uint64_t nfactors_;
  double lambda_;     // regulation lambda
  double confidence_; // confidence weight

  // 用户评价矩阵
  std::shared_ptr<int> rating_vec_ptr_;

  // epcho_id_ = 1, 3, 5, ... fix item, cal user
  // epcho_id_ = 2, 4, 6, ... fix user, cal item
  std::shared_ptr<qmf::Matrix> item_vec_ptr_;
  std::shared_ptr<qmf::Matrix> user_vec_ptr_;
};

} // end namespace distributed

#endif // __DISTRIBUTED_COMMON_BIGDATA_H__
