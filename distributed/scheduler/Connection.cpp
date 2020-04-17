
/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#include <distributed/scheduler/Connection.h>

#include <glog/logging.h>

namespace distributed {
namespace scheduler {

bool Connection::event() {

  if (stage_ == Stage::kHead) {

    char* ptr = reinterpret_cast<char*>(&head_);
    int len = ::read(socket_, ptr + head_idx_, sizeof(Head) - head_idx_);
    if (len == -1) {
      LOG(ERROR) << "read head failed for " << self();
      return false;
    } else if (len == 0) {
      LOG(ERROR) << "peer closed " << self();
      return false;
    }

    // normal read
    head_idx_ += len;
    if (head_idx_ < sizeof(Head)) {
      return true;
    }

    // prase net header
    head_.from_net_endian();
    if (head_.magic != kHeaderMagic || head_.version != kHeaderVersion ||
        head_.status != 0) {
      LOG(ERROR) << "message header magic, version, status check failed."
                 << head_.dump();
      return false;
    }

    return handle_head();

  } else if (stage_ == Stage::kBody) {

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

} // end namespace scheduler
} // end namespace distributed
