/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#ifndef __DISTRIBUTED_COMMON_BIGDATA_H__
#define __DISTRIBUTED_COMMON_BIGDATA_H__

#include <cstdint>
#include <endian.h>

namespace distributed {

class BigData {

 public:
  BigData& instance();

 private:
  std::vector<Double>;

 private:
  BigData();
  ~BigData();
};

} // end namespace distributed

#endif // __DISTRIBUTED_COMMON_BIGDATA_H__
