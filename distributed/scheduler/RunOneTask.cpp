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
     << "\tuser_factors: " << taskdef->user_factors() << std::endl
     << "\titem_factors: " << taskdef->item_factors() << std::endl
     << "------    end    ------" << std::endl;

  return ss.str();
}

bool Scheduler::RunOneTask(const std::shared_ptr<TaskDef>& taskdef) {

  LOG(INFO) << task_def_dump(taskdef);

  bigdata_ptr_->start_term(taskdef->nfactors(),
                           taskdef->regularization_lambda(),
                           taskdef->confidence_weight());

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

  engine_ptr_->init();
  LOG(INFO) << "detected item count: " << engine_ptr_->nitems();
  LOG(INFO) << "detected user count: " << engine_ptr_->nusers();

  // step 2. 初始化uniform
  bigdata_ptr_->item_factor_ptr_ = std::make_shared<qmf::FactorData>(
    engine_ptr_->nitems(), taskdef->nfactors());
  bigdata_ptr_->user_factor_ptr_ = std::make_shared<qmf::FactorData>(
    engine_ptr_->nusers(), taskdef->nfactors());

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

  const size_t kStartConnectionCount = connections_count();
  LOG(INFO) << "current active labor: " << kStartConnectionCount;

  if (!push_all_rating(taskdef)) {
    LOG(ERROR) << "scheduler push rating matrix to all labor failed.";
    return false;
  }

  size_t rate_count = 0;
  while ((rate_count = connections_count(true)) < kStartConnectionCount) {

    LOG(INFO) << "waiting ... current rateload labor count " << rate_count
              << ", expect " << kStartConnectionCount;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  // step 4. 执行迭代
  size_t fixed_count = 0;
  for (size_t i = 0; i < taskdef->nepochs(); ++i) {

    bigdata_ptr_->incr_epchoid();
    push_all_fixed(taskdef);

    // waiting all FixedLoad
    while ((fixed_count = connections_count(true)) < kStartConnectionCount) {

      LOG(INFO) << "waiting ... current fixedload labor count " << fixed_count
                << ", expect " << kStartConnectionCount;
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // calc

    bigdata_ptr_->incr_epchoid();
    push_all_fixed(taskdef);

    // waiting all FixedLoad
    while ((fixed_count = connections_count(true)) < kStartConnectionCount) {

      LOG(INFO) << "waiting ... current fixedload labor count " << fixed_count
                << ", expect " << kStartConnectionCount;
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // calc
  }

  // step 5. 保存
  LOG(INFO) << "saving user_factors and item_factors ";
  engine_ptr_->saveUserFactors(taskdef->user_factors());
  engine_ptr_->saveItemFactors(taskdef->item_factors());

  return true;
}

} // end namespace scheduler
} // end namespace distributed