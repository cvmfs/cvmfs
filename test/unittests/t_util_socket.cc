#include <gtest/gtest.h>
#include "util/socket.h"

namespace util{
  bool reached_reading_end(const std::string &msg) {
    if (msg.size() >= 3 and msg.substr(msg.size() - 3) == "END") {
      return true;
    }
    return false;
  }

  std::string &clean_result(std::string &res) {
    if (res.size() >= 3) {
      res.erase(res.size() - 3);
    } else {
      exit(EXIT_FAILURE);
    }
    return res;
  }
}

TEST(T_LocalUnixSocket, SingleClientServerCommunication){
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";
  constexpr size_t BufferSize = 12;
  std::string msg = "Hello, world!";

  LocalUnixSocket<BufferSize, ProcessType::Server, util::reached_reading_end,util::clean_result> server{socket_name};
  LocalUnixSocket<BufferSize, ProcessType::Client,util::reached_reading_end,util::clean_result> client(socket_name);

  client.connect().write(msg).write("END");
  EXPECT_EQ(msg,server.accept().read());

  msg="Hello back!";
  server.write(msg).write("END");
  EXPECT_EQ(msg,client.read());
}
