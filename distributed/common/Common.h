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

// all Labors share the same dataset with Scheduler, so we just need to pass the
// calculate segment index, the Labor inference the actual range
const size_t kBucketSize = 10000;
const size_t kBucketBits = 10000; // support maxium 100m users/items

const size_t kTrivalMsgSize = 128;

// force to send kHeartBeat
const time_t kHeartBeatInternal = 30;

} // end namespace distributed

#endif // __DISTRIBUTED_COMMON_COMMON_H__
