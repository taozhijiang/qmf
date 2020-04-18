/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#include <random>

#include <distributed/scheduler/Scheduler.h>

#include <glog/logging.h>

namespace distributed {
namespace scheduler {

std::string task_def_dump(const std::shared_ptr<TaskDef>& taskdef) {

  std::stringstream ss;
  ss << std::endl
     << "------ full task ------" << std::endl
     << "\tnepochs: " << taskdef->nepochs() << std::endl
     << "\tnfactors: " << taskdef->nfactors() << std::endl
     << "\tregularization_lambda: " << taskdef->regularization_lambda()
     << std::endl
     << "\tconfidence_weight: " << taskdef->confidence_weight() << std::endl
     << "\tinit_distribution_bound: " << taskdef->init_distribution_bound()
     << std::endl
     << "\tdistribution_file: " << taskdef->distribution_file() << std::endl
     << "\ttrain_set: " << taskdef->train_set() << std::endl
     << "------    end    ------" << std::endl;

  return ss.str();
}

bool Scheduler::RunOneTask(const std::shared_ptr<TaskDef>& taskdef) {

  LOG(INFO) << task_def_dump(taskdef);

  bigdata_ptr_->start_term();

  // step 1. 加载train_set数据

  LOG(INFO) << "loading training dataset";
  qmf::DatasetReader trainReader(taskdef->train_set());
  trainReader.readAll(bigdata_ptr_->rating_vec_);
  if (bigdata_ptr_->rating_vec_.empty()) {
    LOG(ERROR) << "load training dataset empty: " << taskdef->train_set();
    return false;
  }
  LOG(INFO) << "load training datset size: "
            << bigdata_ptr_->rating_vec_.size();

  std::set<int64_t> user_set;
  std::set<int64_t> item_set;
  for (const auto& elem : bigdata_ptr_->rating_vec_) {
    user_set.insert(elem.userId);
    item_set.insert(elem.itemId);
  }

  LOG(INFO) << "detected item count: " << item_set.size();
  LOG(INFO) << "detected user count: " << user_set.size();

  // step 2. 初始化uniform
  bigdata_ptr_->item_factor_ptr_ =
    std::make_shared<qmf::FactorData>(item_set.size(), taskdef->nfactors());
  bigdata_ptr_->user_factor_ptr_ =
    std::make_shared<qmf::FactorData>(user_set.size(), taskdef->nfactors());

  if (taskdef->distribution_file().empty()) {

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<qmf::Double> distr(
      -taskdef->init_distribution_bound(), taskdef->init_distribution_bound());
    auto genUnif = [&distr, &gen](auto...) { return distr(gen); };
    // don't need to initialize user factors
    bigdata_ptr_->item_factor_ptr_->setFactors(genUnif);
    LOG(INFO) << "initialize item factors with random.";

  } else {

    bigdata_ptr_->item_factor_ptr_->setFactors(taskdef->distribution_file());
    LOG(INFO) << "initialize item factors with file: "
              << taskdef->distribution_file();
  }

  // step 3. push rating 给所有的labor
  if (!push_all_rating(taskdef)) {
    LOG(ERROR) << "scheduler push rating matrix to all labor failed.";
    return false;
  }

  // 检查是否有labor的状态为kRateLoad了
  ::sleep(10);

  uint32_t rateload = 0;
  for (auto iter = connections_ptr_.begin(); iter != connections_ptr_.end();
       ++iter) {
    auto connection = iter->second;
    if (connection->status_ == LaborStatus::kRateLoad)
      ++rateload;
  }

  if (rateload == 0) {
    LOG(ERROR) << "no available kRateLoad labor found!";
    return false;
  }
  LOG(INFO) << "current available labor: " << rateload;

  // step 4. 执行迭代
  for (size_t i = 0; i < taskdef->nepochs(); ++i) {

    bigdata_ptr_->incr_epcho();
    push_all_fixed(taskdef);

    ::sleep(5);

    bigdata_ptr_->incr_epcho();
    push_all_fixed(taskdef);

    ::sleep(5);
  }

  // step 5. 保存

  return true;
}

} // end namespace scheduler
} // end namespace distributed