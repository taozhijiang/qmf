/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#ifndef __DISTRIBUTED_LABOR_RECV_OPS_H__
#define __DISTRIBUTED_LABOR_RECV_OPS_H__

#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cstdint>
#include <algorithm>
#include <string>

#include <distributed/common/Message.h>
#include <glog/logging.h>

namespace distributed {
namespace labor {

class RecvOps {

 public:
  static bool try_recv_head(int socketfd, Head* head, bool* critical) {

    if (!head || !critical)
      return false;

    char* ptr = reinterpret_cast<char*>(head);
    uint64_t recv = 0;
    while (recv < kHeadSize) {
      int retval = ::read(socketfd, ptr + recv, kHeadSize - recv);
      if (retval < 0) {
        // recv 超时
        if (errno == EAGAIN || errno == EWOULDBLOCK) {

          // 对于已经接收了部分数据的，等待剩余数据
          if (recv > 0) {
            continue;
          }

          // 什么都每收到，not critical
          // VLOG(3) << "recv timeout, and no previous recv: " << recv;
          return false;

        } else {

          LOG(ERROR) << "read error: " << strerror(errno);
          *critical = true;
          return false;
        }

      } else if (retval == 0) {

        LOG(ERROR) << "peer close down: " << socketfd;
        *critical = true;
        return false;
      }

      VLOG(3) << "this term recv: " << recv << ", retval " << retval;
      recv += retval;
    }

    // recv full head buff
    head->from_net_endian();
    if (!head->validate()) {
      LOG(ERROR) << "message header magic, version, length check failed."
                 << head->dump();
      return false;
    }

    return true;
  }

  static bool recv_message(int socketfd, const Head& head, char* buff) {

    if (head.length == 0 || !buff)
      return false;

    uint64_t recv = 0;
    while (recv < head.length) {
      int retval = ::read(socketfd, buff + recv, head.length - recv);
      if (retval < 0) {
        // recv 超时
        if (errno == EAGAIN || errno == EWOULDBLOCK)
          continue;

        LOG(ERROR) << "read error: " << strerror(errno);
        return false;

      } else if (retval == 0) {

        LOG(ERROR) << "peer close down: " << socketfd;
        return false;
      }

      recv += retval;
      VLOG(3) << "this term recv: " << recv << ", retval " << retval;
    }

    VLOG(3) << "successful recved " << recv;
    return true;
  }

  // 读取 len 个数据，废弃掉
  static bool recv_and_drop(int socketfd, uint64_t len) {

    if (len == 0)
      return true;

    char buff[1 * 1024 * 1024]{};
    uint64_t recv = 0;

    while (recv < len) {
    
      int retval = ::read(socketfd, buff, std::min<uint64_t>(sizeof(buff), len - recv));
      if (retval < 0) {

        // recv 超时
        if (errno == EAGAIN || errno == EWOULDBLOCK)
          continue;

        LOG(ERROR) << "read error: " << strerror(errno);
        return false;

      } else if (retval == 0) {

        LOG(ERROR) << "peer close down: " << socketfd;
        return false;
      }

      recv += retval;
    }

    return true;
  }
};

} // end namespace labor
} // end namespace distributed

#endif // __DISTRIBUTED_LABOR_RECV_OPS_H__