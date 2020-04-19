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

  engine_ptr_ = std::make_unique<qmf::WALSEngineLite>(bigdata_ptr_);
  if (!engine_ptr_) {
    LOG(ERROR) << "create WALSEngineLite failed.";
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

    // 无论何时，Labor收到kPushRate都直接更新本地数据

    VLOG(3) << head_.dump();

    int64_t item_sz = head_.length / sizeof(qmf::DatasetElem);
    bigdata_ptr_->rating_vec_.resize(item_sz);
    char* dat = reinterpret_cast<char*>(bigdata_ptr_->rating_vec_.data());

    retval = RecvOps::recv_message(socketfd_, head_, dat);
    if (!retval) {
      LOG(ERROR) << "recv rating matrix failed.";
      break;
    }

    bigdata_ptr_->set_param(head_);

    // build index ...
    engine_ptr_->init();

    // build factors
    bigdata_ptr_->item_factor_ptr_ =
      std::make_shared<qmf::FactorData>(engine_ptr_->nitems(), head_.nfactors);
    bigdata_ptr_->user_factor_ptr_ =
      std::make_shared<qmf::FactorData>(engine_ptr_->nusers(), head_.nfactors);

    // only setFactors can allocate internal space
    bigdata_ptr_->item_factor_ptr_->setFactors();
    bigdata_ptr_->user_factor_ptr_->setFactors();

    // response
    const char* msg = "OK";
    if (!SendOps::send_bulk(socketfd_, OpCode::kPushRateRsp, msg, 2,
                            head_.taskid, head_.epchoid)) {
      LOG(ERROR) << "send response failed.";
    }

    break;
  }

  case static_cast<int>(OpCode::kPushFixed): {

    // 只有之前的taskid一致，才可以接受kPushFixed
    //
    // ! 即使是检查错误，这里也需要把剩余的 length 数据读取完
    // ! 否则下次通信的时候还是会串话，导致头解析失败
    //

    if (head_.taskid != bigdata_ptr_->taskid()) {
      LOG(ERROR) << "taskid mismatch, local " << bigdata_ptr_->taskid()
                 << ", but recv " << head_.taskid;

      RecvOps::recv_and_drop(socketfd_, head_.length);

      const char* msg = "FA";
      if (!SendOps::send_bulk(socketfd_, OpCode::kErrorRsp, msg, 2,
                              bigdata_ptr_->taskid(),
                              bigdata_ptr_->epchoid())) {
        LOG(ERROR) << "send response failed.";
      }
      break;
    }

    const int64_t infer_sz =
      head_.length / (head_.nfactors * sizeof(qmf::Double));

    VLOG(3) << head_.dump();
    VLOG(3) << "detected factors item/user size: " << infer_sz;

    // epcho_id_ = 1, 3, 5, ... fix item, cal user
    // epcho_id_ = 2, 4, 6, ... fix user, cal item

    char* dat = nullptr;
    if (head_.epchoid % 2) {

      if (infer_sz != engine_ptr_->nitems()) {
        LOG(FATAL) << "inference item size " << infer_sz << ", but dataset "
                   << engine_ptr_->nitems();
      }

      const qmf::Matrix& matrix = bigdata_ptr_->item_factor_ptr_->getFactors();
      dat = reinterpret_cast<char*>(const_cast<qmf::Matrix&>(matrix).data());

    } else {

      if (infer_sz != engine_ptr_->nusers()) {
        LOG(FATAL) << "inference user size " << infer_sz << ", but dataset "
                   << engine_ptr_->nusers();
      }

      const qmf::Matrix& matrix = bigdata_ptr_->user_factor_ptr_->getFactors();
      dat = reinterpret_cast<char*>(const_cast<qmf::Matrix&>(matrix).data());
    }

    if (!RecvOps::recv_message(socketfd_, head_, dat)) {
      LOG(ERROR) << "recv fixed factors failed.";
      break;
    }

    bigdata_ptr_->set_param(head_);

    // response
    std::string message = "OK";
    if (!SendOps::send_bulk(socketfd_, OpCode::kPushFixedRsp, message.c_str(),
                            2, head_.taskid, head_.epchoid)) {
      LOG(ERROR) << "send response failed.";
    }

    break;
  }

  case static_cast<int>(OpCode::kCalc): {

    // 只有taskid和epchoid完全一致，才可以进行计算
    if (head_.taskid != bigdata_ptr_->taskid() ||
        head_.epchoid != bigdata_ptr_->epchoid()) {
      LOG(ERROR) << "taskid/epchoid mismatch, local " << bigdata_ptr_->taskid()
                 << ":" << bigdata_ptr_->epchoid() << ", but recv "
                 << head_.taskid << ":" << head_.epchoid;

      RecvOps::recv_and_drop(socketfd_, head_.length);

      const char* msg = "FA";
      if (!SendOps::send_bulk(socketfd_, OpCode::kErrorRsp, msg, 2,
                              bigdata_ptr_->taskid(),
                              bigdata_ptr_->epchoid())) {
        LOG(ERROR) << "send response failed.";
      }
      break;
    }

    break;
  }

  case static_cast<int>(OpCode::kSubmitTaskRsp):
  case static_cast<int>(OpCode::kAttachLaborRsp):
  case static_cast<int>(OpCode::kSubmitTask):
  case static_cast<int>(OpCode::kAttachLabor):
  case static_cast<int>(OpCode::kPushRateRsp):
  case static_cast<int>(OpCode::kPushFixedRsp):
  case static_cast<int>(OpCode::kCalcRsp):
  default:
    LOG(FATAL) << "invalid OpCode received from scheduler:"
               << static_cast<int>(head_.opcode);
    retval = false;
    break;
  }

  return retval;
}

} // end namespace labor
} // end namespace distributed
