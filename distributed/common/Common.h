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



enum class LaborStatus : uint8_t {

  kAttach = 1,
  kRateLoad = 2,
  kFixedLoad = 3,

};

} // end namespace distributed

#endif // __DISTRIBUTED_COMMON_COMMON_H__
