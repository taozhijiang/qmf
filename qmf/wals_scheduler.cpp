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

#include <qmf/wals/WALSEngine.h>
#include <qmf/DatasetReader.h>
#include <qmf/metrics/MetricsEngine.h>
#include <qmf/utils/Util.h>

#include <distributed/scheduler/Scheduler.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

// scheduler network
DEFINE_string(scheduler_ip, "0.0.0.0", "scheduler ip address");
DEFINE_int32(scheduler_port, 8900, "scheduler listen port");

std::unique_ptr<distributed::scheduler::Scheduler> scheduler;

static void signal_handler(int signal) {
  switch (signal) {

  case SIGUSR1:
  case SIGINT:
    scheduler->terminate();
    LOG(INFO) << "termiating system.";
    ::sleep(5);
    break;

  default:
    LOG(ERROR) << "signal not processed: " << signal;
    break;
  }
}

int main(int argc, char** argv) {

  gflags::SetUsageMessage("wals_scheduler");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  // make glog to log to stderr
  FLAGS_logtostderr = 1;

  ::signal(SIGUSR1, ::signal_handler);
  ::signal(SIGINT, ::signal_handler);
  ::signal(SIGCHLD, SIG_IGN);

  scheduler = std::make_unique<distributed::scheduler::Scheduler>(
    FLAGS_scheduler_ip, FLAGS_scheduler_port);
  if (!scheduler || !scheduler->init()) {
    LOG(ERROR) << "create or initialize scheduler failed.";
    return EXIT_FAILURE;
  }

  scheduler->select_loop();

  return 0;
}
