/*-
 * Copyright (c) 2019 TAO Zhijiang<taozhijiang@gmail.com>
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */


#ifndef __DISTRIBUTED_MESSAGE_H__
#define __DISTRIBUTED_MESSAGE_H__

#include <endian.h>

#include <cstdint>
#include <string>


namespace distributed {

const static uint16_t kHeaderMagic  = 0x4D46; 'M''F'
const static uint8_t  kHeaderVersion = 0x01;


enum class OpCode : uint8_t {
    
    kSubmitTask = 1;
    kSubmitTaskRsp = 2;
    
    kAddWorker = 3;
    kAddWorkerRsp = 4;

    kPushY = 5;
    kPushYRsp = 6;

    kCalc = 7;
    kCalcRsp = 8;
};

struct Header {

    uint16_t magic;         // "MF"
    uint8_t  version;       // 1
    uint8_t  opcode;        // 当前消息类型
    uint32_t task;          // 任务ID
    uint32_t epcho;         // epcho迭代ID
    uint64_t length;        // playload length ( NOT include header)


    std::string dump() const {
        char msg[64]{};
        ::snprintf(msg, sizeof(msg), "mgc:%0x, ver:%0x, opcode:%0x, task:%0x, epcho: %0x, len:%lu",
                 magic, version, opcode, task, epcho, length);
        return msg;
    }

    // 网络大端字节序
    void from_net_endian() {
        magic   = be16toh(magic);
        version = version;
        opcode  = opcode;
        task    = be32toh(task);
        epcho   = be32toh(epcho);
        length  = be64toh(length);
    }

    void to_net_endian() {
        magic   = htobe16(magic);
        version = version;
        opcode  = opcode;
        task    = htobe32(task);
        epcho   = htobe32(epcho);
        length  = htobe64(length);
    }

} __attribute__((__packed__));


} // end namespace distributed

#endif // __DISTRIBUTED_MESSAGE_H__
