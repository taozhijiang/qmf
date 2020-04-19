/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#ifndef __DISTRIBUTED_COMMON_SEND_OPS_H__
#define __DISTRIBUTED_COMMON_SEND_OPS_H__

#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cstdint>
#include <string>

#include <distributed/common/Message.h>
#include <distributed/common/SendOps.h>
#include <glog/logging.h>

namespace distributed {

class SendOps {

 public:
  static bool send_lite(int socketfd, const char* buff, uint64_t len) {
    if (!buff)
      return false;

    uint64_t sent = 0;
    while (sent < len) {
      int retval = ::write(socketfd, buff + sent, len - sent);
      if (retval < 0) {
        LOG(ERROR) << "SendOps write error: " << strerror(errno);
        return false;
      }

      VLOG(3) << "sent " << sent << ", retval " << retval;
      sent += retval;
    }

    VLOG(3) << "total sent " << sent;
    return true;
  }

  // 简易发送消息的函数，如果出错返回false
  static bool
    send_message(int socketfd, enum OpCode code, const std::string& msg) {

    Head head(code);
    head.length = msg.size();
    head.to_net_endian();

    return send_lite(
             socketfd, reinterpret_cast<const char*>(&head), sizeof(Head)) &&
           send_lite(socketfd, msg.c_str(), msg.size());
  }

  static bool send_bulk(int socketfd,
                        enum OpCode code,
                        const char* buff,
                        uint64_t len,
                        uint32_t taskid = 0,
                        uint32_t epchoid = 0,
                        uint32_t nfactors = 0,
                        uint32_t bucket = 0,
                        double lambda = 0,
                        double confidence = 0) {

    Head head(code);

    head.length = len;
    head.taskid = taskid;
    head.epchoid = epchoid;
    head.nfactors = nfactors;
    head.bucket = bucket;
    head.lambda = lambda;
    head.confidence = confidence;
    head.to_net_endian();

    return send_lite(
             socketfd, reinterpret_cast<const char*>(&head), sizeof(Head)) &&
           send_lite(socketfd, buff, len);
  }
};

} // end namespace distributed

#endif // __DISTRIBUTED_COMMON_SEND_OPS_H__