/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <thread>
#include <chrono> // std::chrono::seconds

#include <distributed/labor/Labor.h>
#include <distributed/common/SendOps.h>
#include <distributed/labor/RecvOps.h>

#include <glog/logging.h>

namespace distributed {
namespace labor {

bool Labor::init() {

  if (!start_connect())
    return false;

  if (!start_attach())
    return false;

  bigdata_ptr_ = std::make_unique<BigData>();
  if (!bigdata_ptr_) {
    LOG(ERROR) << "create BigData failed.";
    return false;
  }

  return true;
}

bool Labor::start_connect() {

  if ((socketfd_ = ::socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    LOG(ERROR) << "create socket error: " << ::strerror(errno);
    return false;
  }

  bool success = false;
  do {

    // If a receive operation has blocked for this much time without receiving
    // additional data, it shall return with a partial count or errno set to
    // [EAGAIN] or [EWOULDBLOCK] if no data is received.

    // set the recvtimeout, for I don't want a select in client
    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    if (::setsockopt(socketfd_, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv,
                     sizeof tv) < 0) {
      LOG(ERROR) << "setting socket recvtimeout failed: " << strerror(errno);
      break;
    }

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port_);
    if (::inet_pton(AF_INET, addr_.c_str(), &serv_addr.sin_addr) <= 0) {
      LOG(ERROR) << "Invalid server address: " << addr_;
      break;
    }

    // connect
    if (::connect(socketfd_, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) <
        0) {
      LOG(ERROR) << "connect to " << addr_ << ":" << port_ << " failed.";
      break;
    }

    success = true;
  } while (0);

  if (!success) {
    close(socketfd_);
    return false;
  }

  LOG(INFO) << "connect listen to " << addr_ << ":" << port_
            << " successfully!";
  return true;
}

bool Labor::start_attach() {

  std::string message = "attach_labor";
  if (!SendOps::send_message(socketfd_, OpCode::kAttachLabor, message)) {
    LOG(ERROR) << "labor start_attach send failed.";
    return false;
  }

  Head head{};
  bool critical = false;
  bool ret = false;

  do {

    bool retval = RecvOps::try_recv_head(socketfd_, &head, &critical);
    if (critical) {
      LOG(ERROR) << "recv head failed.";
      break;
    }

    // empty recv, retry again
    if (!retval)
      continue;

    std::vector<char> msg;
    msg.resize(head.length);
    char* buff = msg.data();
    ret = RecvOps::recv_message(socketfd_, head, buff);

    if (ret) {
      LOG(INFO) << "response: " << std::string(msg.data(), msg.size());
    }

    break;

  } while (true);

  return ret;
}

void Labor::loop() {

  LOG(INFO) << "start loop thread ...";

  while (!terminate_) {

    bool critical = false;

    // 自带socket超时机制
    bool retval = RecvOps::try_recv_head(socketfd_, &head_, &critical);
    if (critical) {
      LOG(ERROR) << "recv head failed.";
      break;
    }

    // empty recv, retry again
    if (!retval) {
      LOG(INFO) << "empty recv.";
      continue;
    }

    retval = handle_head();
    if (!retval) {
      LOG(ERROR) << "labor handle head failed: " << head_.dump();
      break;
    }
  }

  LOG(INFO) << "terminate loop thread ...";
}

bool Labor::handle_head() {

  bool retval = true;
  switch (head_.opcode) {

  case static_cast<int>(OpCode::kPushRate): {
    int64_t item_sz = head_.length / sizeof(qmf::DatasetElem);
    bigdata_ptr_->rating_vec_.resize(item_sz);
    char* dat = reinterpret_cast<char*>(bigdata_ptr_->rating_vec_.data());
    retval = RecvOps::recv_message(socketfd_, head_, dat);
    if (!retval) {
      LOG(ERROR) << "recv rating matrix failed.";
      break;
    }

    // other ...
    bigdata_ptr_->task_id_ = head_.task;
    bigdata_ptr_->epcho_id_ = head_.epcho;

    // response
    std::string message = "OK";
    retval = SendOps::send_message(socketfd_, OpCode::kPushRateRsp, message);
    if (!retval) {
      LOG(ERROR) << "send response failed.";
    }

    break;
  }

  case static_cast<int>(OpCode::kPushFixed):

    break;

  case static_cast<int>(OpCode::kCalc):
    break;

  case static_cast<int>(OpCode::kSubmitTaskRsp):
  case static_cast<int>(OpCode::kAttachLaborRsp):
  case static_cast<int>(OpCode::kSubmitTask):
  case static_cast<int>(OpCode::kAttachLabor):
  case static_cast<int>(OpCode::kPushRateRsp):
  case static_cast<int>(OpCode::kPushFixedRsp):
  case static_cast<int>(OpCode::kCalcRsp):
  default:
    LOG(ERROR) << "invalid OpCode received from scheduler:"
               << static_cast<int>(head_.opcode);
    retval = false;
    break;
  }

  return retval;
}

} // end namespace labor
} // end namespace distributed
