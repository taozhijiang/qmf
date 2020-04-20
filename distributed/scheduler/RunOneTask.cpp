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

  // step 1. load train set

  LOG(INFO) << "loading training dataset";
  qmf::DatasetReader trainReader(taskdef->train_set());
  trainReader.readAll(bigdata_ptr_->rating_vec_);
  if (bigdata_ptr_->rating_vec_.empty()) {
    LOG(ERROR) << "training dataset empty: " << taskdef->train_set();
    return false;
  }
  LOG(INFO) << "total training dataset size: "
            << bigdata_ptr_->rating_vec_.size();

  // this will build users/items index
  engine_ptr_->init();
  LOG(INFO) << "detected item count: " << engine_ptr_->nitems();
  LOG(INFO) << "detected user count: " << engine_ptr_->nusers();

  // step 2. uniform the fixed factors
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

    // we don't need to initialize user factors
    bigdata_ptr_->item_factor_ptr_->setFactors(genUnif);
    LOG(INFO) << "initialize items factors with random.";

  } else {

    bigdata_ptr_->item_factor_ptr_->setFactors(taskdef->distribution_file());
    LOG(INFO) << "initialize items factors with static file: "
              << taskdef->distribution_file();
  }

  // step 3. push rating matrix to all labors

  // at least more than half Labors should available
  const size_t kStartConnectionCount = connections_count();
  const size_t kQuorumsConnectionCount = kStartConnectionCount / 2 + 1;
  LOG(INFO) << "current total Labor count " << kStartConnectionCount
            << ", at least available Labor: " << kQuorumsConnectionCount;

  if (!push_all_rating_matrix()) {
    LOG(ERROR) << "scheduler push rating matrix to all labor failed.";
    return false;
  }

  size_t rate_count = 0;
  while ((rate_count = connections_count(true)) < kQuorumsConnectionCount) {

    LOG(INFO) << "waiting ... current rateload labor count " << rate_count
              << ", expect at least " << kStartConnectionCount;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  // step 4. iterate to do the m.f.
  size_t fixed_count = 0;
  for (size_t i = 0; i < taskdef->nepochs(); ++i) {

    bigdata_ptr_->incr_epchoid();
    push_all_fixed_factors();

    // waiting all FixedLoad
    while ((fixed_count = connections_count(true)) < kQuorumsConnectionCount) {

      LOG(INFO) << "waiting ... current fixedload labor count " << fixed_count
                << ", expect at least " << kQuorumsConnectionCount;
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    LOG(INFO) << "begin iterate users factors ...";
    if (!iterate_factors()) {
      LOG(ERROR) << "task " << bigdata_ptr_->taskid() << ":"
                 << bigdata_ptr_->epchoid()
                 << " iterate users factors failed!!!";
      return false;
    }

    bigdata_ptr_->incr_epchoid();
    push_all_fixed_factors();

    // waiting all FixedLoad
    while ((fixed_count = connections_count(true)) < kQuorumsConnectionCount) {

      LOG(INFO) << "waiting ... current fixedload labor count " << fixed_count
                << ", expect at least " << kQuorumsConnectionCount;
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    LOG(INFO) << "begin iterate items factors ...";
    if (!iterate_factors()) {
      LOG(ERROR) << "task " << bigdata_ptr_->taskid() << ":"
                 << bigdata_ptr_->epchoid()
                 << " iterate items factors failed!!!";
      return false;
    }
  }

  // step 5. save the result to fs
  LOG(INFO) << "saving user_factors and item_factors ";
  engine_ptr_->saveUserFactors(taskdef->user_factors());
  engine_ptr_->saveItemFactors(taskdef->item_factors());

  return true;
}

bool Scheduler::iterate_factors() {

  bool iterate_user = bigdata_ptr_->epchoid() % 2;

  uint64_t bucket_number = 0;
  if (iterate_user) {
    bucket_number = (engine_ptr_->nusers() + kBucketSize - 1) / kBucketSize;
    LOG(INFO) << "users factors count " << engine_ptr_->nusers()
              << " mapped to " << bucket_number << " buckets.";
  } else {
    bucket_number = (engine_ptr_->nitems() + kBucketSize - 1) / kBucketSize;
    LOG(INFO) << "items factors count " << engine_ptr_->nusers()
              << " mapped to " << bucket_number << " buckets.";
  }

  uint64_t index = 0; // the incr bucket index

  while (true) {

    connections_ptr_type copy_connections = share_connections_ptr();
    for (auto iter = copy_connections->begin(); iter != copy_connections->end();
         ++iter) {

      // find available labor
      auto connection = iter->second;
      if (!connection->is_labor_)
        continue;

      // check whether stale, and need to kHeartBeat
      if (connection->is_busy_) {

        time_t timeout = kHeartBeatInternal;
        if (connection->is_stale(timeout)) {
          connection->touch();
          push_heartbeat(connection);
          LOG(INFO) << "connection " << connection->addr() << " is stale for "
                    << timeout << " seconds, send kHeartBeat message.";
        }

        continue;
      }

      // find the unfinished bucket
      while (bigdata_ptr_->bucket_bits_[index] &&
             bigdata_ptr_->bucket_bits_.count() < bucket_number) {
        index = (index + 1) % bucket_number;
      }

      VLOG(3) << "procent ("
              << ((bigdata_ptr_->bucket_bits_.count() * 100) / bucket_number)
              << "%) finished, current index " << index << ", finished count "
              << bigdata_ptr_->bucket_bits_.count() << ", total "
              << bucket_number;

      if (!bigdata_ptr_->bucket_bits_[index]) {

        connection->touch();
        connection->bucket_start_ = ::time(NULL);
        push_bucket(index, connection->socket_);
        connection->is_busy_ = true;

        index = (index + 1) % bucket_number;
      }

      if (bigdata_ptr_->bucket_bits_.count() == bucket_number) {
        LOG(INFO) << "iterate done!";
        return true;
      }
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  return true;
}

} // end namespace scheduler
} // end namespace distributed