/*-
 * Copyright (c) 2020 taozhijiang@gmail.com
 *
 * Licensed under the BSD-3-Clause license, see LICENSE for full information.
 *
 */

#ifndef __DISTRIBUTED_COMMON_MESSAGE_H__
#define __DISTRIBUTED_COMMON_MESSAGE_H__

#ifdef __APPLE__
#include <libkern/OSByteOrder.h>

#define htobe16(x) OSSwapHostToBigInt16(x)
#define htole16(x) OSSwapHostToLittleInt16(x)
#define be16toh(x) OSSwapBigToHostInt16(x)
#define le16toh(x) OSSwapLittleToHostInt16(x)

#define htobe32(x) OSSwapHostToBigInt32(x)
#define htole32(x) OSSwapHostToLittleInt32(x)
#define be32toh(x) OSSwapBigToHostInt32(x)
#define le32toh(x) OSSwapLittleToHostInt32(x)

#define htobe64(x) OSSwapHostToBigInt64(x)
#define htole64(x) OSSwapHostToLittleInt64(x)
#define be64toh(x) OSSwapBigToHostInt64(x)
#define le64toh(x) OSSwapLittleToHostInt64(x)
#else
#include <endian.h>
#endif

#include <cstdint>
#include <string>

namespace distributed {

const static uint16_t kHeaderMagic = 0x4D46; // 'M' 'F'
const static uint8_t kHeaderVersion = 0x01;

enum class OpCode : uint8_t {

  // Scheduler接收客户端请求
  kSubmitTask = 1,
  kSubmitTaskRsp = 2,

  // Labor连接Scheduler
  kAttachLabor = 3,
  kAttachLaborRsp = 4,

  // 推送评分矩阵
  kPushRate = 5,
  kPushRateRsp = 6,

  // 推送固定向量
  kPushFixed = 7,
  kPushFixedRsp = 8,

  // 迭代进行矩阵分解计算
  kCalc = 9,
  kCalcRsp = 10,

  kUnspecified = 100,
};

struct Head {

  Head()
    : magic(kHeaderMagic),
      version(kHeaderVersion),
      opcode(static_cast<uint8_t>(OpCode::kUnspecified)),
      length(0) {
  }

  explicit Head(enum OpCode code)
    : magic(kHeaderMagic),
      version(kHeaderVersion),
      opcode(static_cast<uint8_t>(code)),
      length(0) {
  }

  uint16_t magic;  // "MF"
  uint8_t version; // 1
  uint8_t opcode;  // 当前消息类型

  uint32_t task;  // 任务ID
  uint32_t epcho; // epcho迭代ID

  // 为了高效传输，数据已经不能保证架构无关的了 ...
  uint64_t nfactors;
  double lambda;     // regulation lambda
  double confidence; // confidence weight

  uint64_t length; // playload length ( NOT include header)

  std::string dump() const {
    char msg[64]{};
    ::snprintf(msg, sizeof(msg),
               "mgc:%0x, ver:%0x, opcode:%0x, task:%0x, epcho: %0x, nfactors: "
               "%0x, lambda: %lf, confidence: %lf, len:%lu",
               magic, version, opcode, task, epcho, nfactors, lambda,
               confidence, length);
    return msg;
  }

  // 网络大端字节序
  void from_net_endian() {
    magic = be16toh(magic);
    version = version;
    opcode = opcode;
    task = be32toh(task);
    epcho = be32toh(epcho);
    nfactors = be64toh(nfactors);
    lambda = lambda;
    confidence = confidence;
    length = be64toh(length);
  }

  void to_net_endian() {
    magic = htobe16(magic);
    version = version;
    opcode = opcode;
    task = htobe32(task);
    epcho = htobe32(epcho);
    nfactors = htobe64(nfactors);
    lambda = lambda;
    confidence = confidence;
    length = htobe64(length);
  }

  bool validate() const {
    return magic == kHeaderMagic && version == kHeaderVersion && length > 0 &&
           opcode != static_cast<uint8_t>(OpCode::kUnspecified);
  }

} __attribute__((__packed__));

} // end namespace distributed

#endif // __DISTRIBUTED_COMMON_MESSAGE_H__
