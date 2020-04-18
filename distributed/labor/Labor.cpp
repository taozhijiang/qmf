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

  Head head {};
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
    
    if(ret) {
      LOG(INFO) << "response: " << std::string(msg.data(), msg.size());
    }
    
    break;

  } while (true);

  
  return ret;
}

void Labor::loop() {

  LOG(INFO) << "start loop thread ...";

  while (!terminate_) {

    // Terminate ??
    std::this_thread::sleep_for(std::chrono::seconds(1));
    continue;
  }

  LOG(INFO) << "terminate loop thread ...";
}

} // end namespace labor
} // end namespace distributed
