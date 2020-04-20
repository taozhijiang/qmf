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

  // Scheduler recived the submit client's request
  kSubmitTask = 1,
  kSubmitTaskRsp = 2,

  // Labor connect to the Scheduler when startup
  kAttachLabor = 3,
  kAttachLaborRsp = 4,

  // Scheduler push RatingMatrix(Dataset) to Labors
  kPushRate = 5,
  kPushRateRsp = 6,

  // Scheduler push the fixed factors to Labors
  kPushFixed = 7,
  kPushFixedRsp = 8,

  // do the calculate task
  kCalc = 9,
  kCalcRsp = 10,

  // Scheduler send to labor, the labor response with its local info
  kHeartBeat = 11,

  // When labor recived kHeartBeat, or some error detected, then it send back
  // Scheduler with its local info
  kInfoRsp = 12,

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

  uint16_t magic;   // "MF"
  uint8_t version;  // 1
  uint8_t opcode;   // indicate current message type

  uint32_t taskid;  // 
  uint32_t epchoid; // 

  //
  // ! accouting for the performance and efficiency, not all the data
  // ! are platform independent, we recommend to use this cluster on
  // ! the same architecture is possible

  uint32_t nfactors;
  uint32_t bucket;  // the bucket index number splitted

  double lambda;    // regulation lambda
  double confidence; // confidence weight

  uint64_t length;  // playload length ( NOT include header)

  std::string dump() const {
    char msg[256]{};
    ::snprintf(
      msg, sizeof(msg),
      "magic:%0x, version:%0x, opcode:%0x, taskid:%0x, epchoid: %0x, nfactors: "
      "%0x, bucket: %0x, lambda: %.2f confidence: %.2f len: %lu",
      magic, version, opcode, taskid, epchoid, nfactors, bucket, lambda,
      confidence, length);
    return msg;
  }

  // use them for simplify the debug log
  std::string stepinfo() const {
    char msg[256]{};
    ::snprintf(msg, sizeof(msg), "{taskid:%0x, epchoid:%0x, bucket:%0x}",
               taskid, epchoid, bucket);
    return msg;
  }

  // when received from network, transmit to local machine
  void from_net_endian() {
    magic = be16toh(magic);
    version = version;
    opcode = opcode;
    taskid = be32toh(taskid);
    epchoid = be32toh(epchoid);
    nfactors = be32toh(nfactors);
    bucket = be32toh(bucket);
    lambda = lambda;
    confidence = confidence;
    length = be64toh(length);
  }

  // called before send to the remote
  void to_net_endian() {
    magic = htobe16(magic);
    version = version;
    opcode = opcode;
    taskid = htobe32(taskid);
    epchoid = htobe32(epchoid);
    nfactors = htobe32(nfactors);
    bucket = htobe32(bucket);
    lambda = lambda;
    confidence = confidence;
    length = htobe64(length);
  }

  bool validate() const {
    return magic == kHeaderMagic && version == kHeaderVersion && length > 0 &&
           opcode != static_cast<uint8_t>(OpCode::kUnspecified);
  }

} __attribute__((aligned(1), __packed__));

static const size_t kHeadSize = sizeof(Head);

} // end namespace distributed

#endif // __DISTRIBUTED_COMMON_MESSAGE_H__
