/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#ifndef __DISTRIBUTED_COMMON_COMMON_H__
#define __DISTRIBUTED_COMMON_COMMON_H__

#include <cstdint>

namespace distributed {


// 将所有计算按照下面的计数进行等分，每次计算请求只需要传递索引号就可以了
const size_t kBucketSize = 10000;
const size_t kBucketBits = 10000; //1亿的user/item维度，可以了吧


const size_t kTrivalMsgSize = 128;

// force to send kHeartBeat
const time_t kHeartBeatInternal = 60;

} // end namespace distributed

#endif // __DISTRIBUTED_COMMON_COMMON_H__
