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
#include <glog/logging.h>

namespace distributed {

class SendOps {

public:

  static bool send_lite(int socketfd, const char* buff, size_t len) {
    if (!buff)
      return false;

    int sent = 0;
    while (sent < len) {
      int retval = ::write(socketfd, buff + sent, len - sent);
      if (retval < 0) {
        LOG(ERROR) << "write/send error: " << strerror(errno);
        return false;
      }

      sent += retval;
    }

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


};

} // end namespace distributed

#endif // __DISTRIBUTED_COMMON_SEND_OPS_H__