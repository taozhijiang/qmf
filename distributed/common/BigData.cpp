/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */


#include <distributed/common/BigData.h>

namespace distributed {

BigData& BigData::instance() {
  static BigData bi{};
  return &bi;
}

} // end namespace distributed

#endif // __DISTRIBUTED_COMMON_BIGDATA_H__
