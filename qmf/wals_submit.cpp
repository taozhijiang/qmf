#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <iostream>
#include <distributed/common/Message.h>

/**
 * 用来向scheduler提交任务的工具
 */

static void usage() {

  std::cerr << std::endl;
  std::cerr << "[INFO] wals_submit addr port task_file" << std::endl;
  std::cerr << "[INFO]   addr:port    the address of schedular." << std::endl;
  std::cerr << "[INFO]   task_file    plain protobuf type of task desc."
            << std::endl;
  std::cerr << std::endl;
}

static bool send_task(int socketfd, const std::string& task) {

  distributed::Head head(distributed::OpCode::kSubmitTask);
  head.length = task.size();
  head.to_net_endian();

  const size_t sz = distributed::kHeadSize + task.size();
  char* buff = static_cast<char*>(::malloc(sz));
  if (!buff) {
    std::cerr << "[ERROR] malloc sz: " << sz << " failed." << std::endl;
    return false;
  }

  ::memcpy(buff, reinterpret_cast<const char*>(&head), distributed::kHeadSize);
  ::memcpy(buff + distributed::kHeadSize, task.c_str(), task.size());

  // send
  int sent = 0;
  while (sent < sz) {
    int retval = ::write(socketfd, buff + sent, sz - sent);
    if (retval < 0) {
      std::cerr << "[ERROR] send failed: " << strerror(errno) << std::endl;
      ::free(buff);
      return false;
    }

    sent += retval;
  }

  // recv
  const int expected_sz = distributed::kHeadSize + 2; // OK
  ::memset(buff, 0, expected_sz + 1);

  int recved = 0;
  while (recved < expected_sz) {
    int retval = ::read(socketfd, buff + recved, expected_sz - recved);
    if (retval < 0) {
      std::cerr << "[ERROR] recv failed: " << strerror(errno) << std::endl;
      ::free(buff);
      return false;
    }

    recved += retval;
  }

  distributed::Head r_head;
  ::memcpy(reinterpret_cast<char*>(&r_head), buff, distributed::kHeadSize);
  r_head.from_net_endian();
  if (r_head.validate() &&
      r_head.opcode ==
        static_cast<uint8_t>(distributed::OpCode::kSubmitTaskRsp) &&
      r_head.length == 2 && buff[distributed::kHeadSize] == 'O' &&
      buff[distributed::kHeadSize + 1] == 'K') {
    std::cout << "[INFO] submit task OK!" << std::endl;
    ::free(buff);
    return true;
  }

  std::cout << "[ERROR] submit task failed with validate " << r_head.validate()
            << ", head: " << r_head.dump() << std::endl;
  std::cout << "[ERROR] additional char: " << buff[distributed::kHeadSize]
            << ", " << buff[distributed::kHeadSize + 1] << std::endl;
  ::free(buff);
  return false;
}

int main(int argc, char** argv) {

  if (argc < 4) {
    usage();
    return EXIT_FAILURE;
  }

  const std::string addr = argv[1];
  int port = std::atoi(argv[2]);
  const std::string task = argv[3];

  // connect
  int socketfd = -1;
  if ((socketfd = ::socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    std::cerr << "[ERROR] create socket error: " << ::strerror(errno)
              << std::endl;
    return EXIT_FAILURE;
  }

  struct sockaddr_in srv_addr;
  srv_addr.sin_family = AF_INET;
  srv_addr.sin_port = htons(port);
  if (::inet_pton(AF_INET, addr.c_str(), &srv_addr.sin_addr) <= 0) {
    std::cerr << "[ERROR] Invalid server address: " << addr << std::endl;
    return EXIT_FAILURE;
  }

  // connect
  if (::connect(socketfd, (struct sockaddr*)&srv_addr, sizeof(srv_addr)) < 0) {
    std::cerr << "[ERROR] connect to " << addr << ":" << port << " failed."
              << std::endl;
    return EXIT_FAILURE;
  }

  return send_task(socketfd, task) ? EXIT_SUCCESS : EXIT_FAILURE;
}
