
/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#include <distributed/scheduler/Connection.h>
#include <distributed/common/SendOps.h>

#include <glog/logging.h>

namespace distributed {
namespace scheduler {

bool Connection::event() {

  if (stage_ == Stage::kHead) {

    char* ptr = reinterpret_cast<char*>(&head_);

    // need to read more
    if (head_idx_ < sizeof(Head)) {
      int len = ::read(socket_, ptr + head_idx_, sizeof(Head) - head_idx_);
      if (len == -1) {
        LOG(ERROR) << "read head failed for " << self();
        return false;
      } else if (len == 0) {
        LOG(ERROR) << "peer closed " << self();
        return false;
      }

      head_idx_ += len;

      // need additional read
      if (head_idx_ < sizeof(Head)) {
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
        LOG(ERROR) << "read data failed for " << self();
        return false;
      } else if (len == 0) {
        LOG(ERROR) << "peer closed " << self();
        return false;
      }

      // normal read
      data_idx_ += len;

      // need additional read
      if (data_idx_ < head_.length) {
        return true;
      }

      stage_ = Stage::kDone;
      data_idx_ = 0;
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
    break;

  case static_cast<int>(OpCode::kSubmitTaskRsp):
  case static_cast<int>(OpCode::kAttachLaborRsp):
  case static_cast<int>(OpCode::kPushRate):
  case static_cast<int>(OpCode::kPushFixed):
  case static_cast<int>(OpCode::kCalc):
  default:
    LOG(ERROR) << "invalid OpCode received from scheduler:"
               << static_cast<int>(head_.opcode);
    retval = false;
    break;
  }

  return retval;
}

bool Connection::handle_body() {

  bool retval = true;
  switch (head_.opcode) {

  case static_cast<int>(OpCode::kSubmitTask):
    LOG(INFO) << "NOT IMPLEMENTED... " << std::endl;
    reset();
    break;

  case static_cast<int>(OpCode::kAttachLabor): {
    LOG(INFO) << "recv attch with message: "
              << std::string(data_.data(), data_.size());
    reset();
    std::string message = "attach_labor_rsp_ok";
    SendOps::send_message(socket_, OpCode::kAttachLaborRsp, message);
    break;
  }

  case static_cast<int>(OpCode::kPushRateRsp):
  case static_cast<int>(OpCode::kPushFixedRsp):
  case static_cast<int>(OpCode::kCalcRsp):
    LOG(INFO) << "NOT IMPLEMENTED... " << std::endl;
    reset();
    break;

  case static_cast<int>(OpCode::kSubmitTaskRsp):
  case static_cast<int>(OpCode::kAttachLaborRsp):
  case static_cast<int>(OpCode::kPushRate):
  case static_cast<int>(OpCode::kPushFixed):
  case static_cast<int>(OpCode::kCalc):
  default:
    LOG(ERROR) << "invalid OpCode received from scheduler:"
               << static_cast<int>(head_.opcode);
    retval = false;
    break;
  }

  return retval;
}

} // end namespace scheduler
} // end namespace distributed
