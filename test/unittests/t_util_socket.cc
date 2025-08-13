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

TEST(T_IPC_SingleServerMultipleClients, ExchangeSingleCommand){
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";
  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> c0(socket_name);
  LocalUnixSocket<ProcessType::Client> c1(socket_name);

  c0.connect();
  c1.connect();
  server.accept();
  server.accept();

  util::Command send{util::Command::SendHashes};
  util::Command recv{util::Command::RecvHashes};
  server.write(send,0);
  server.write(recv ,1);

  EXPECT_EQ(send,c0.read<util::Command>()[0]);
  EXPECT_EQ(recv,c1.read<util::Command>()[0]);
}

TEST(T_IPC_SingleServerMultipleClients, ExchangeSingleNumber){
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";
  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> c0(socket_name);
  LocalUnixSocket<ProcessType::Client> c1(socket_name);

  c0.connect();
  c1.connect();
  server.accept();
  server.accept();

  c0.write(42);
  c1.write(24);

  EXPECT_EQ(42,server.read<int>(1,0)[0]);
  EXPECT_EQ(24,server.read<int>(1,1)[0]);
}

TEST(T_IPC_SingleServerMultipleClients, ExchangeSingleHash){
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";
  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> c0(socket_name);
  LocalUnixSocket<ProcessType::Client> c1(socket_name);

  c0.connect();
  c1.connect();
  server.accept();
  server.accept();

  shash::Any hash;
  shash::Any hashes[2];
  hash.Randomize(42);
  hashes[0]=hash;
  hash.Randomize(24);
  hashes[1]=hash;

  c0.write(hashes[0]);
  c1.write(hashes[1]);

  EXPECT_EQ(hashes[0],server.read<shash::Any>(1,0)[0]);
  EXPECT_EQ(hashes[1],server.read<shash::Any>(1,1)[0]);
}
