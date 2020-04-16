/*-
 * Copyright (c) 2019 TAO Zhijiang<taozhijiang@gmail.com>
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */


#ifndef __DISTRIBUTED_LABOR_LABOR_H__
#define __DISTRIBUTED_LABOR_LABOR_H__


namespace distributed {
namespace labor {


class Labor {

public:
    Labor(const std::string& ip, int32_t port):
        ip_(ip),
        port_(port) {

        }


    bool init() {

        return false;
    }


private:
    const std::string ip_;
    const int32_t port_;
};

} // end namespace labor
} // end namespace distributed


#endif // __DISTRIBUTED_LABOR_LABOR_H__