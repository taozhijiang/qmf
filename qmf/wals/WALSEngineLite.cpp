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

#include <algorithm>

#include <omp.h>

#include <qmf/wals/WALSEngineLite.h>

namespace qmf {

// 这个对象会被多次使用，所以需要保证初始化后是完全干净的
void WALSEngineLite::init() {

  userIndex_.reset();
  itemIndex_.reset();

  userSignals_.clear();
  itemSignals_.clear();

  // 拷贝过来，建立索引
  auto mutableDataset = bigdata_ptr_->rating_vec_;
  groupSignals(userSignals_, userIndex_, mutableDataset);

  // swap userId with itemId
  for (auto& elem : mutableDataset) {
// g++ will complain for "cannot bind packed field to xxx&"
#if !defined(__GNUC__)
    std::swap(elem.userId, elem.itemId);
#else
    auto tmp = elem.userId;
    elem.userId = elem.itemId;
    elem.itemId = tmp;
#endif
  }
  groupSignals(itemSignals_, itemIndex_, mutableDataset);
}

void WALSEngineLite::optimize() {

#if 0
  for (size_t epoch = 1; epoch <= config_.nepochs; ++epoch) {
    // fix item factors, update user factors
    iterate(*userFactors_, userIndex_, userSignals_, *itemFactors_, itemIndex_);
    // fix user factors, update item factors
    const Double loss = iterate(
      *itemFactors_, itemIndex_, itemSignals_, *userFactors_, userIndex_);
    LOG(INFO) << "epoch " << epoch << ": train loss = " << loss;
    // evaluate
    evaluate(epoch);
  }
#endif
}

void WALSEngineLite::evaluate(const size_t epoch) {

#if 0
  // evaluate test average metrics
  if (metricsEngine_ && !metricsEngine_->testAvgMetrics().empty() &&
      !testUsers_.empty() &&
      (metricsEngine_->config().alwaysCompute || epoch == config_.nepochs)) {

    computeTestScores(
      testScores_, testUsers_, *userFactors_, *itemFactors_, parallel_);
    metricsEngine_->computeAndRecordTestAvgMetrics(
      epoch, testLabels_, testScores_, parallel_);
  }
#endif
}

void WALSEngineLite::saveFactors(const FactorData& factorData,
                                 const IdIndex& index,
                                 const std::string& fileName) const {
  std::ofstream fout(fileName);

  CHECK_EQ(factorData.nelems(), index.size());
  fout << std::fixed;
  fout << std::setprecision(9);
  for (size_t idx = 0; idx < factorData.nelems(); ++idx) {
    const int64_t id = index.id(idx);
    fout << id;
    if (factorData.withBiases()) {
      fout << ' ' << factorData.biasAt(idx);
    }
    for (size_t fidx = 0; fidx < factorData.nfactors(); ++fidx) {
      fout << ' ' << factorData.at(idx, fidx);
    }
    fout << '\n';
  }
}

void WALSEngineLite::saveUserFactors(const std::string& fileName) const {
  CHECK(bigdata_ptr_->user_factor_ptr_) << "user factors wasn't initialized";
  saveFactors(*bigdata_ptr_->user_factor_ptr_, userIndex_, fileName);
}

void WALSEngineLite::saveItemFactors(const std::string& fileName) const {
  CHECK(bigdata_ptr_->item_factor_ptr_) << "item factors wasn't initialized";
  saveFactors(*bigdata_ptr_->item_factor_ptr_, itemIndex_, fileName);
}

size_t WALSEngineLite::nusers() const {
  return userIndex_.size();
}

size_t WALSEngineLite::nitems() const {
  return itemIndex_.size();
}

void WALSEngineLite::groupSignals(std::vector<SignalGroup>& signals,
                                  IdIndex& index,
                                  std::vector<DatasetElem>& dataset) {
  sortDataset(dataset);
  const int64_t InvalidId = std::numeric_limits<int64_t>::min();
  int64_t prevId = InvalidId;
  std::vector<Signal> group;
  for (const auto& elem : dataset) {
    if (elem.userId != prevId) {
      if (prevId != InvalidId) {
        signals.emplace_back(SignalGroup{prevId, group});
      }
      prevId = elem.userId;
      group.clear();
    }
    group.emplace_back(Signal{elem.itemId, elem.value});
  }
  if (prevId != InvalidId) {
    signals.emplace_back(SignalGroup{prevId, group});
  }
  for (size_t i = 0; i < signals.size(); ++i) {
    const size_t idx = index.getOrSetIdx(signals[i].sourceId);
    CHECK_EQ(idx, i);
  }
}

void WALSEngineLite::sortDataset(std::vector<DatasetElem>& dataset) {
  std::sort(dataset.begin(), dataset.end(), [](const auto& x, const auto& y) {
    if (x.userId != y.userId) {
      return x.userId < y.userId;
    }
    return x.itemId < y.itemId;
  });
}

Double WALSEngineLite::iterate(uint64_t start_index,
                               uint64_t end_index,
                               FactorData& leftData,
                               const IdIndex& leftIndex,
                               const std::vector<SignalGroup>& leftSignals,
                               const FactorData& rightData,
                               const IdIndex& rightIndex) {

  // auto genZero = [](auto...) { return 0.0; };
  // leftData.setFactors(genZero);

  Matrix& X = leftData.getFactors();
  const Matrix& Y = rightData.getFactors();

#if defined(__GNUC__)
//  omp_set_num_threads(16);
#endif

  Double loss = 0.0;
  const auto alpha = bigdata_ptr_->confidence();
  const auto lambda = bigdata_ptr_->lambda();

#pragma omp parallel reduction(+ : loss)
  {

#pragma omp for
    for (uint64_t i = start_index; i < end_index; ++i) {
      const size_t leftIdx = leftIndex.idx(leftSignals[i].sourceId);
      loss += updateFactorsForOne(X.data(leftIdx), X.ncols(), Y, rightIndex,
                                  leftSignals[i], *(bigdata_ptr_->YtY_ptr_),
                                  alpha, lambda);
    }
  }

  return loss / Y.nrows() / (end_index - start_index);
}

void WALSEngineLite::computeXtX(const Matrix& X, Matrix* out) {

  // assert X and out samesize

#if defined(__GNUC__)
//  omp_set_num_threads(16);
#endif

  const size_t nrows = X.nrows();
  const size_t ncols = X.ncols();
  out->clear();

#pragma omp parallel for
  for (size_t k = 0; k < nrows; ++k) {
    for (size_t i = 0; i < ncols; ++i) {
      for (size_t j = 0; j < ncols; ++j) {
        (*out)(i, j) += X(k, i) * X(k, j);
      }
    }
  }
}

Double WALSEngineLite::updateFactorsForOne(Double* result,
                                           const size_t n,
                                           const Matrix& Y,
                                           const IdIndex& rightIndex,
                                           const SignalGroup& signalGroup,
                                           Matrix A,
                                           const Double alpha,
                                           const Double lambda) {
  Double loss = 0.0;
  Vector b(n);
  for (const auto& signal : signalGroup.group) {
    const size_t rightIdx = rightIndex.idx(signal.id);
    for (size_t i = 0; i < n; ++i) {
      b(i) += Y(rightIdx, i) * (1.0 + alpha * signal.value);
      for (size_t j = 0; j < n; ++j) {
        A(i, j) += Y(rightIdx, i) * alpha * signal.value * Y(rightIdx, j);
      }
    }
    // for term p^t * C * p
    loss += 1.0 + alpha * signal.value;
  }
  // B = Y^t * C * Y
  Matrix B = A;
  for (size_t i = 0; i < n; ++i) {
    A(i, i) += lambda;
  }
  // A * x = b
  Vector x = linearSymmetricSolve(A, b);
  // x^t * Y^t * C * Y * x
  for (size_t i = 0; i < n; ++i) {
    for (size_t j = 0; j < n; ++j) {
      loss += B(i, j) * x(i) * x(j);
    }
  }
  // -2 * x^t * Y^t * C * p
  for (size_t i = 0; i < n; ++i) {
    loss -= 2 * x(i) * b(i);
  }

  for (size_t i = 0; i < n; ++i) {
    *(result + i) = x(i);
  }
  return loss;
}

} // namespace qmf
