#include <crypto/hash.h>
#include <gtest/gtest.h>
#include <vector>
#include "util/socket.h"
#include "smallhash.h"

namespace util{
  enum class Command{SendHashes,RecvHashes};
};

TEST(T_IPC_SingleServerSingleClient, ExchangeSingleCommand){
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";

  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> client(socket_name);

  client.connect();
  server.accept();

  util::Command cmd{util::Command::SendHashes};
  server.write(cmd);
  EXPECT_EQ(cmd,client.read<util::Command>()[0]);

  cmd=util::Command::RecvHashes;
  client.write(cmd);
  EXPECT_EQ(cmd, server.read<util::Command>()[0]);
}

TEST(T_IPC_SingleServerSingleClient, ExchangeMultipleCommands){
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";

  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> client(socket_name);

  client.connect();
  server.accept();

  server.write(util::Command::RecvHashes)
    .write(util::Command::RecvHashes)
    .write(util::Command::SendHashes)
    .write(util::Command::RecvHashes);

  auto result = client.read<util::Command>(4);
  EXPECT_EQ(result[0],util::Command::RecvHashes);
  EXPECT_EQ(result[1],util::Command::RecvHashes);
  EXPECT_EQ(result[2],util::Command::SendHashes);
  EXPECT_EQ(result[3],util::Command::RecvHashes);
}

TEST(T_IPC_SingleServerSingleClient, ExchangeSingleNumber){
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";

  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> client(socket_name);

  client.connect();
  server.accept();

  int number=42;

  server.write(number);
  EXPECT_EQ(number,client.read<int>()[0]);

  number = 24;
  client.write(number);
  EXPECT_EQ(number,server.read<int>()[0]);
}

TEST(T_IPC_SingleServerSingleClient, ExchangeMultipleNumbers){
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";

  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> client(socket_name);

  client.connect();
  server.accept();

  server.write(42)
    .write(43)
    .write(44)
    .write(45);

  auto result = client.read<int>(4);
  EXPECT_EQ(result[0],42);
  EXPECT_EQ(result[1],43);
  EXPECT_EQ(result[2],44);
  EXPECT_EQ(result[3],45);
}

TEST(T_IPC_SingleServerSingleClient, ExchangeSingleHash){
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";

  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> client(socket_name);

  client.connect();
  server.accept();

  shash::Any hash;
  hash.Randomize(42);

  client.write(hash);
  EXPECT_EQ(hash, server.read<shash::Any>()[0] );
}

TEST(T_IPC_SingleServerSingleClient, ExchangeMultipleHashes){
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";

  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> client(socket_name);

  constexpr size_t N = 250;
  std::vector<shash::Any> hashes;
  for(size_t i =0 ; i<N ; ++i){
    shash::Any hash;
    hash.Randomize(i);
    hashes.emplace_back(hash);
  }

  client.connect();
  server.accept();

  for(size_t i =0 ; i<N ; ++i){
    client.write(hashes[i]);
  }
  std::vector<shash::Any> result = server.read<shash::Any>(N);
  for(size_t i =0 ; i<N ; ++i){
    EXPECT_EQ(hashes[i], result[i]);
  }
}
/*
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
*/
