/*-
 * Copyright (c) 2019 TAO Zhijiang<taozhijiang@gmail.com>
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */


#ifndef __DISTRIBUTED_SCHEDULER_SCHEDULER_H__
#define __DISTRIBUTED_SCHEDULER_SCHEDULER_H__

#include <string>

namespace distributed {
namespace scheduler {

class Scheduler {

public:

    Scheduler(const std::string& ip, int32_t port):
        ip_(ip),
        port_(port) {
        }


    bool init() {

        int socketfd = start_listen();
        if(socketfd < 0)
            return false;

        return true;
    }

private:
    
    int start_listen();
    void select_loop_run();

private:

    bool terminate_ = false;

    const std::string ip_;
    const int32_t port_;
};


} // end namespace scheduler
} // end namespace distributed


#endif // __DISTRIBUTED_SCHEDULER_SCHEDULER_H__