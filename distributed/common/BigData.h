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
#include <bitset> // std::bitset

#include <qmf/DatasetReader.h>

// Matrix use std::vector<Double> for internal storage
#include <qmf/Matrix.h>
#include <qmf/FactorData.h>

#include <distributed/common/Common.h>
#include <distributed/common/Message.h>

namespace distributed {

struct BigData {

  // TODO: dynamic create task_bits for performance
  using bucket_bits_type = std::bitset<kBucketBits>;

  BigData() {

    // ::srandom(::time(NULL));
    // taskid_ = ::random() & 0xFFFF;

    taskid_ = epchoid_ = 0;
  }

  uint32_t taskid() const {
    return taskid_;
  }

  uint32_t epchoid() const {
    return epchoid_;
  }

  uint32_t nfactors() const {
    return nfactors_;
  }

  double lambda() const {
    return lambda_;
  }

  double confidence() const {
    return confidence_;
  }

  // start new epcho
  uint32_t incr_epchoid() {
    bucket_bits_.reset();
    return ++epchoid_;
  }

  // user rating matrix, also unknown as dataset
  std::vector<qmf::DatasetElem> rating_vec_;

  // epcho_id_ = 1, 3, 5, ... fix item, cal user
  // epcho_id_ = 2, 4, 6, ... fix user, cal item

  std::shared_ptr<qmf::FactorData> item_factor_ptr_;
  std::shared_ptr<qmf::FactorData> user_factor_ptr_;
  std::shared_ptr<qmf::Matrix> YtY_ptr_;

  // used in scheduler
  bucket_bits_type bucket_bits_;

  // reset actin for new task
  // called by Scheduler
  void start_term(uint32_t nfactors, double lambda, double confidence) {

    ++taskid_;

    epchoid_ = 0;
    nfactors_ = nfactors;
    lambda_ = lambda;
    confidence_ = confidence;

    bucket_bits_.reset();
  }

  // called by Labor, update local info
  void set_param(const Head& head) {

    taskid_ = head.taskid;
    epchoid_ = head.epchoid;
    nfactors_ = head.nfactors;
    lambda_ = head.lambda;
    confidence_ = head.confidence;
  }

 private:
  uint32_t taskid_;
  uint32_t epchoid_;

  uint32_t nfactors_;
  double lambda_;     // regulation lambda
  double confidence_; // confidence weight
  
};

} // end namespace distributed

#endif // __DISTRIBUTED_COMMON_BIGDATA_H__
