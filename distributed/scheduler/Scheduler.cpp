

#include "Scheduler.h"


#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <glog/logging.h>

namespace distributed {
namespace scheduler {

int Scheduler::start_listen() {

    int socketfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if(socketfd < 0) {
        LOG(ERROR) << "create socket error: " << ::strerror(errno);
        return -1;
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<short>(port_));
    addr.sin_addr.s_addr = inet_addr(ip_.c_str());

    if(::bind(socketfd,(struct sockaddr*)&addr,sizeof(addr)) < 0) {
        LOG(ERROR) << "bind error: " << ::strerror(errno);
        close(socketfd);
        return -1;
    }

    if(::listen(socketfd, 10) < 0){
        LOG(ERROR) << "bind error: " << ::strerror(errno);
        close(socketfd);
        return -1;
    }

    LOG(INFO) << "scheduler listen to " << ip_ << ":" << port_ << " successfully!";
    return socketfd;
}


} // end namespace scheduler
} // end namespace distributed