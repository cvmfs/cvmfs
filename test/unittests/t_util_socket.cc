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

TEST(T_LocalUnixSocket, MultipleClientsServerCommunicationSerial){
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";
  constexpr size_t BufferSize = 12;

  LocalUnixSocket<BufferSize, ProcessType::Server, util::reached_reading_end,util::clean_result> server{socket_name};
  LocalUnixSocket<BufferSize, ProcessType::Client,util::reached_reading_end,util::clean_result> c0(socket_name);
  LocalUnixSocket<BufferSize, ProcessType::Client,util::reached_reading_end,util::clean_result> c1(socket_name);
  LocalUnixSocket<BufferSize, ProcessType::Client,util::reached_reading_end,util::clean_result> c2(socket_name);

  c0.connect();
  c1.connect();
  c2.connect();

  server.accept();
  server.accept();
  server.accept();

  auto exchange_messages = [&server](const auto& client, size_t index){
    std::string msg = "Hello, world!";
    client.write(msg).write("END");
    EXPECT_EQ(msg,server.read(index));

    msg="Hello back!";
    server.write(msg,index).write("END",index);
    EXPECT_EQ(msg,client.read());
  };
  
  exchange_messages(c0,0);
  exchange_messages(c1,1);
  exchange_messages(c2,2);
}

TEST(T_LocalUnixSocket, MultipleClientsServerCommunicationMixed){
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";
  constexpr size_t BufferSize = 12;

  LocalUnixSocket<BufferSize, ProcessType::Server, util::reached_reading_end,util::clean_result> server{socket_name};
  LocalUnixSocket<BufferSize, ProcessType::Client,util::reached_reading_end,util::clean_result> c0(socket_name);
  LocalUnixSocket<BufferSize, ProcessType::Client,util::reached_reading_end,util::clean_result> c1(socket_name);
  LocalUnixSocket<BufferSize, ProcessType::Client,util::reached_reading_end,util::clean_result> c2(socket_name);

  c0.connect();
  c1.connect();
  c2.connect();

  server.accept();
  server.accept();
  server.accept();

  c0.write("c0").write("END");
  c2.write("c2").write("END");
  c1.write("c1").write("END");
  EXPECT_EQ(server.read(0),"c0");
  EXPECT_EQ(server.read(1),"c1");
  EXPECT_EQ(server.read(2),"c2");
}
