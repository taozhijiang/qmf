/*
 * Copyright 2016 Quora, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * 原始的 WALSEngine 比较的复杂，耦合了较多的操作
 * 这里精简为 WALSEngineLite 对于分布式中的Scheduler和Labor都可以使用
 */

#pragma once

#include <memory>
#include <vector>

#include <qmf/Engine.h>
#include <qmf/FactorData.h>
#include <qmf/metrics/MetricsEngine.h>
#include <qmf/Types.h>
#include <qmf/utils/IdIndex.h>

#include <distributed/common/BigData.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

namespace qmf {

class WALSEngineLite {
 public:
  explicit WALSEngineLite(std::unique_ptr<distributed::BigData>& bigdata,
                          const size_t nthreads = 16)
    : bigdata_ptr_(bigdata), thread_num_(nthreads) {
  }

  void init();

  void optimize();

  void evaluate(const size_t epoch);

  size_t nusers() const;

  size_t nitems() const;

  void saveUserFactors(const std::string& fileName) const;
  void saveItemFactors(const std::string& fileName) const;
  void saveFactors(const FactorData& factorData,
                   const IdIndex& index,
                   const std::string& fileName) const;

 private:
  struct Signal {
    int64_t id;
    Double value;
  };

  struct SignalGroup {
    int64_t sourceId;
    std::vector<Signal> group;
  };

  static void groupSignals(std::vector<SignalGroup>& signals,
                           IdIndex& index,
                           std::vector<DatasetElem>& dataset);

  static void sortDataset(std::vector<DatasetElem>& dataset);

  Double iterate(FactorData& leftData,
                 const IdIndex& leftIndex,
                 const std::vector<SignalGroup>& leftSignals,
                 const FactorData& rightData,
                 const IdIndex& rightIndex);

  void computeXtX(const Matrix& X, Matrix* out);

  static Double
    updateFactorsForOne(Double* result,
                        const size_t n,
                        const Matrix& Y,
                        const IdIndex& rightIndex,
                        const SignalGroup& signalGroup, /* signal_id & value */
                        Matrix A,                       /*YtY copy*/
                        const Double alpha,
                        const Double lambda);

  // indexes
  IdIndex userIndex_;
  IdIndex itemIndex_;

  // signals
  std::vector<SignalGroup> userSignals_;
  std::vector<SignalGroup> itemSignals_;

  std::unique_ptr<distributed::BigData>& bigdata_ptr_;
  const size_t thread_num_;
};
} // namespace qmf
