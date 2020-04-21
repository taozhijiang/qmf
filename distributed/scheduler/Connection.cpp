
/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#include <fcntl.h>

#include <distributed/scheduler/Connection.h>
#include <distributed/scheduler/Scheduler.h>

#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include <distributed/proto/task.pb.h>

#include <distributed/common/SendOps.h>
#include <distributed/common/RecvOps.h>

#include <glog/logging.h>

namespace distributed {
namespace scheduler {

bool Connection::event() {

  if (stage_ == Stage::kHead) {

    char* ptr = reinterpret_cast<char*>(&head_);

    // need to read more
    if (head_idx_ < kHeadSize) {
      int len = ::read(socket_, ptr + head_idx_, kHeadSize - head_idx_);
      if (len == -1) {
        LOG(ERROR) << "read head failed for " << addr();
        return false;
      } else if (len == 0) {
        LOG(ERROR) << "peer closed " << addr();
        return false;
      }

      head_idx_ += len;

      // need additional read
      if (head_idx_ < kHeadSize) {
        return true;
      }
    }

    // prase net header
    head_.from_net_endian();
    if (!head_.validate()) {
      LOG(ERROR) << "message header magic, version, length check failed."
                 << head_.dump();
      return false;
    }

    VLOG(3) << "read head successful, transmit to  kBody: " << addr();
    stage_ = Stage::kBody;
    return handle_head();

  } else if (stage_ == Stage::kBody) {

    // need to read more
    if (data_idx_ < head_.length) {

      // reserve more space
      if (data_.size() < head_.length)
        data_.resize(head_.length);

      char* ptr = data_.data();
      if (!ptr) {
        LOG(ERROR) << "Bug me! reserved data_ pointer to nullptr...";
        return false;
      }

      int len = ::read(socket_, ptr + data_idx_, head_.length - data_idx_);
      if (len == -1) {
        LOG(ERROR) << "read data failed for " << addr();
        return false;
      } else if (len == 0) {
        LOG(ERROR) << "peer closed " << addr();
        return false;
      }

      // normal read
      data_idx_ += len;

      // need additional read
      if (data_idx_ < head_.length) {
        return true;
      }

      VLOG(3) << "read head successful, transmit to  kDone: " << addr();
      stage_ = Stage::kDone;
      return handle_body();
    }

    // If new message here, we not process currently;
    return true;
  }

  LOG(ERROR) << "uknown stage_: " << static_cast<int>(stage_);
  return false;
}

bool Connection::handle_head() {

  bool retval = true;
  switch (head_.opcode) {

  case static_cast<int>(OpCode::kSubmitTask):
  case static_cast<int>(OpCode::kAttachLabor):
  case static_cast<int>(OpCode::kPushRateRsp):
  case static_cast<int>(OpCode::kPushFixedRsp):
  case static_cast<int>(OpCode::kCalcRsp):
  case static_cast<int>(OpCode::kInfoRsp):
    break;

  case static_cast<int>(OpCode::kSubmitTaskRsp):
  case static_cast<int>(OpCode::kAttachLaborRsp):
  case static_cast<int>(OpCode::kPushRate):
  case static_cast<int>(OpCode::kPushFixed):
  case static_cast<int>(OpCode::kCalc):
  case static_cast<int>(OpCode::kHeartBeat):
  default:
    LOG(FATAL) << "invalid OpCode received by scheduler:"
               << static_cast<int>(head_.opcode);
    retval = false;
    break;
  }

  return retval;
}

bool Connection::handle_body() {

  bool retval = true;

  this->touch();
  switch (head_.opcode) {

  case static_cast<int>(OpCode::kSubmitTask): {

    std::string message = std::string(data_.data(), data_idx_);
    VLOG(3) << "kSubmitTask recv with " << message;
    is_labor_ = false;

    bool success = false;
    do {
      std::string taskfile = std::string(data_.data(), data_idx_);
      int taskfd = ::open(taskfile.c_str(), O_RDONLY);
      if (taskfd < 0) {
        LOG(ERROR) << "read task file failed " << taskfile;
        break;
      }

      auto task = std::make_shared<TaskDef>();
      if (!task) {
        LOG(ERROR) << "create TaskDef failed.";
        break;
      }

      google::protobuf::io::FileInputStream finput(taskfd);
      finput.SetCloseOnDelete(true);
      if (!google::protobuf::TextFormat::Parse(&finput, task.get())) {
        LOG(ERROR) << "parse task file failed " << taskfile;
        break;
      }

      scheduler_.add_task(task);
      success = true;

      LOG(INFO) << "add new task successfully: " << taskfile;

    } while (0);

    reset();

    message = success ? "OK" : "FA";
    SendOps::send_message(socket_, OpCode::kSubmitTaskRsp, message);
    break;
  }

  case static_cast<int>(OpCode::kAttachLabor): {

    std::string message = std::string(data_.data(), data_idx_);
    VLOG(3) << "kAttachLabor recv with " << message;
    is_labor_ = true;

    reset();
    message = "attach_labor_rsp_ok";
    SendOps::send_message(socket_, OpCode::kAttachLaborRsp, message);
    break;
  }

  case static_cast<int>(OpCode::kPushRateRsp): {

    std::string message = std::string(data_.data(), data_idx_);
    VLOG(3) << "kPushRateRsp recv with " << message;

    if (message == "OK") {
      LOG(INFO) << "kPushRateRsp return OK, update our status";
      taskid_ = head_.taskid;
      epchoid_ = head_.epchoid;
    }
    reset();
    break;
  }

  case static_cast<int>(OpCode::kPushFixedRsp): {

    std::string message = std::string(data_.data(), data_idx_);
    VLOG(3) << "kPushFixedRsp recv with " << message;

    if (message == "OK") {
      LOG(INFO) << "kPushFixedRsp OK from " << addr() << ", update our status";
      taskid_ = head_.taskid;
      epchoid_ = head_.epchoid;
    }
    reset();
    break;
  }

  case static_cast<int>(OpCode::kCalcRsp): {

    auto& bigdata_ptr = scheduler_.bigdata_ptr();
    auto& engine_ptr = scheduler_.engine_ptr();

    // TODO 直接下载到指定内存，避免拷贝
    VLOG(3) << "already recv data size: " << data_idx_;

    do {

      // 非匹配的结果，直接丢弃
      if (head_.taskid != bigdata_ptr->taskid() ||
          head_.epchoid != bigdata_ptr->epchoid()) {
        LOG(ERROR) << "unmatch calc response: " << head_.dump();
        break;
      }

      // 拷贝到区域，更新 bucket_bits_

      char* dest = nullptr;
      uint64_t len = 0;

      bool iterate_user = bigdata_ptr->epchoid() % 2;
      if (iterate_user) {

        const uint64_t start_idx = head_.bucket * kBucketSize;
        const uint64_t end_idx =
          std::min<uint64_t>(start_idx + kBucketSize, engine_ptr->nusers());

        // 回传 user factors 结果
        const qmf::Matrix& matrix = bigdata_ptr->user_factor_ptr_->getFactors();
        dest = reinterpret_cast<char*>(
          const_cast<qmf::Matrix&>(matrix).data(start_idx));
        len = (end_idx - start_idx) * sizeof(qmf::Double) * head_.nfactors;

        if (len != head_.length) {
          LOG(ERROR) << "length check failed, expect " << len << ", but get "
                     << head_.length;
          dest = nullptr;
          break;
        }

      } else {

        const uint64_t start_idx = head_.bucket * kBucketSize;
        const uint64_t end_idx =
          std::min<uint64_t>(start_idx + kBucketSize, engine_ptr->nitems());

        // 回传 user factors 结果
        const qmf::Matrix& matrix = bigdata_ptr->item_factor_ptr_->getFactors();
        dest = reinterpret_cast<char*>(
          const_cast<qmf::Matrix&>(matrix).data(start_idx));
        len = (end_idx - start_idx) * sizeof(qmf::Double) * head_.nfactors;

        if (len != head_.length) {
          LOG(ERROR) << "length check failed, expect " << len << ", but get "
                     << head_.length;
          dest = nullptr;
          break;
        }
      }

      if (dest && len) {

        // this bucket calculate successfully, we update the time cost to
        // the bigdata for stale estimate.

        ::memcpy(dest, data_.data(), len);
        bigdata_ptr->bucket_bits_[head_.bucket] = true;
        time_t cost = ::time(NULL) - bucket_start_;
        LOG(INFO) << "bucket calculate task " << head_.stepinfo()
                  << " successfully, time cost " << cost << " secs. ";
      }

    } while (0);

    reset();
    break;
  }

  case static_cast<int>(OpCode::kInfoRsp): {

    // 正常情况下，Scheduler开始计算的时候，都会将评价矩阵和每一轮
    // 的FixedFactor发送给所有Labor，然后再开始分片计算，但是如果
    // 有中途失败或者重新加入的机器，就会出现状态不一致的情况，这个时
    // 候返回Error，Scheduler根据返回的Head信息选择重新推送Rate或
    // 者是FixedFactor即可

    // 补发需要和主线程push进行互斥保护，否则labor可能收到乱序的不完整报文

    auto& bigdata_ptr = scheduler_.bigdata_ptr();

    do {
      if (head_.taskid != bigdata_ptr->taskid()) {

        LOG(INFO) << "found remote taskid: " << head_.taskid
                  << ", update it with " << bigdata_ptr->taskid();

        if (lock_socket_.test_and_set()) {
          LOG(INFO) << "connection socket used by other ..." << addr();
          break;
        }

        VLOG(3) << "== LUCKY resent task " << bigdata_ptr->taskid()
                << " rating to remote " << addr();

        const auto& dataset = bigdata_ptr->rating_vec_;
        const char* dat = reinterpret_cast<const char*>(dataset.data());
        uint64_t len = sizeof(dataset[0]) * dataset.size();

        if (!SendOps::send_bulk(
              socket_, OpCode::kPushRate, dat, len, bigdata_ptr->taskid(),
              bigdata_ptr->epchoid(), bigdata_ptr->nfactors(), 0,
              bigdata_ptr->lambda(), bigdata_ptr->confidence())) {
          LOG(ERROR) << "fallback sending rating to " << addr() << " failed.";
        }

        lock_socket_.clear();

      } else if (head_.epchoid != bigdata_ptr->epchoid()) {

        LOG(INFO) << "found for taskid " << head_.taskid
                  << ", remote epchoid: " << head_.epchoid
                  << ", update it with " << bigdata_ptr->epchoid();

        if (lock_socket_.test_and_set()) {
          LOG(INFO) << "connection socket used by other ..." << addr();
          break;
        }

        VLOG(3) << "== LUCKY resent fixedfactor " << bigdata_ptr->taskid()
                << ":" << bigdata_ptr->epchoid() << " rating to remote "
                << addr();

        const char* dat = nullptr;
        uint64_t len = 0;
        if (bigdata_ptr->epchoid() % 2) {
          const qmf::Matrix& matrix =
            bigdata_ptr->item_factor_ptr_->getFactors();
          dat = reinterpret_cast<const char*>(
            const_cast<qmf::Matrix&>(matrix).data());
          len =
            sizeof(qmf::Matrix::value_type) * matrix.nrows() * matrix.ncols();
          LOG(INFO) << "epcho_id " << bigdata_ptr->epchoid()
                    << " transform itemFactors with size " << len;
        } else {
          const qmf::Matrix& matrix =
            bigdata_ptr->user_factor_ptr_->getFactors();
          dat = reinterpret_cast<const char*>(
            const_cast<qmf::Matrix&>(matrix).data());
          len =
            sizeof(qmf::Matrix::value_type) * matrix.nrows() * matrix.ncols();
          LOG(INFO) << "epcho_id " << bigdata_ptr->epchoid()
                    << " transform userFactors with size " << len;
        }

        if (!SendOps::send_bulk(
              socket_, OpCode::kPushFixed, dat, len, bigdata_ptr->taskid(),
              bigdata_ptr->epchoid(), bigdata_ptr->nfactors(), 0,
              bigdata_ptr->lambda(), bigdata_ptr->confidence())) {
          LOG(ERROR) << "fallback sending fixed to " << addr() << " failed.";
        }

        lock_socket_.clear();

      } else {

        // GOOD, latest info for this connection.
        // don't forget to update the latest labor information to our Scheduler
        // local record

        std::string message = std::string(data_.data(), data_idx_);
        VLOG(3) << "kPushFixedRsp recv with " << message;

        if (message == "OK") {
          LOG(INFO) << "kPushFixedRsp OK from " << addr()
                    << ", update our status";
          taskid_ = head_.taskid;
          epchoid_ = head_.epchoid;
        }
        reset();
        break;
      }

    } while (0);

    reset();
    break;
  }

  case static_cast<int>(OpCode::kSubmitTaskRsp):
  case static_cast<int>(OpCode::kAttachLaborRsp):
  case static_cast<int>(OpCode::kPushRate):
  case static_cast<int>(OpCode::kPushFixed):
  case static_cast<int>(OpCode::kCalc):
  case static_cast<int>(OpCode::kHeartBeat):
  default:
    LOG(FATAL) << "invalid OpCode received by Scheduler:"
               << static_cast<int>(head_.opcode);
    retval = false;
    break;
  }

  return retval;
}

} // end namespace scheduler
} // end namespace distributed
