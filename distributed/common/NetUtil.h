/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#ifndef __DISTRIBUTED_COMMON_NET_UTIL_H__
#define __DISTRIBUTED_COMMON_NET_UTIL_H__

#include <sys/types.h> /* See NOTES */
#include <sys/socket.h>

#include <glog/logging.h>

namespace distributed {

class NetUtil {

 public:
  // Increase the SND/RCV BUF size, hope this can increase the network
  // performance and throughput
  static void optimize_send_recv_buff(int socketfd,
                                      const int sz = (5 << 20) /* 5M */) {

    int old_snd = 0;
    int old_rcv = 0;
    socklen_t len = sizeof(int);

    if (::getsockopt(socketfd, SOL_SOCKET, SO_SNDBUF, &old_snd, &len) < 0 ||
        ::getsockopt(socketfd, SOL_SOCKET, SO_RCVBUF, &old_rcv, &len) < 0) {
      LOG(INFO) << "get previous SO_SNDBUF/OS_RCVDBUF failed "
                << strerror(errno);
    }

    LOG(INFO) << "previous SO_SNDBUF size: " << old_snd
              << ", OS_RCVBUFF size: " << old_rcv;

    if (::setsockopt(socketfd, SOL_SOCKET, SO_SNDBUF, &sz, len) < 0)
      LOG(ERROR) << "set SO_SNDBUF faild " << strerror(errno);

    if (::setsockopt(socketfd, SOL_SOCKET, SO_RCVBUF, &sz, len) < 0)
      LOG(ERROR) << "set SO_RCVBUF faild " << strerror(errno);

    if (::getsockopt(socketfd, SOL_SOCKET, SO_SNDBUF, &old_snd, &len) < 0 ||
        ::getsockopt(socketfd, SOL_SOCKET, SO_RCVBUF, &old_rcv, &len) < 0) {
      LOG(INFO) << "get previous SO_SNDBUF/OS_RCVDBUF failed "
                << strerror(errno);
    }

    LOG(INFO) << "after setting SO_SNDBUF size: " << old_snd
              << ", OS_RCVBUFF size: " << old_rcv;

    return;
  }
};

} // end namespace distributed

#endif // __DISTRIBUTED_COMMON_NET_UTIL_H__