#include <cache.h>
#include <crypto/hash.h>
#include <gtest/gtest.h>
#include <unistd.h>  // fork()

#include <set>
#include <vector>

#include "util/socket.h"

// TODO(christge): The test suit is mature enough to have central setup and
// cleanup


TEST(T_IPC_SingleServerSingleClient, ExchangeSingleCommand) {
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";

  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> client(socket_name);

  client.connect();
  server.accept();

  util::Command cmd{util::Command::SendHashes};
  server.write(cmd);
  EXPECT_EQ(cmd, client.read<util::Command>()[0]);

  cmd = util::Command::RecvHashes;
  client.write(cmd);
  EXPECT_EQ(cmd, server.read<util::Command>()[0]);
}

TEST(T_IPC_SingleServerSingleClient, ExchangeSingleCommandMultiprocess) {
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";
  LocalUnixSocket<ProcessType::Server> server{socket_name};

  pid_t pid = fork();
  switch (pid) {
    case -1:
      ASSERT_TRUE(false);
      break;
    case 0:
      // Client code
      LocalUnixSocket<ProcessType::Client> client(socket_name);
      client.connect();
      EXPECT_EQ(util::Command::SendHashes, client.read<util::Command>()[0]);
      client.write(util::Command::RecvHashes);
      _exit(0);
  }

  server.accept();
  server.write(util::Command::SendHashes);
  EXPECT_EQ(util::Command::RecvHashes, server.read<util::Command>()[0]);
}

TEST(T_IPC_SingleServerSingleClient, ExchangeSingleCommandAsync) {
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";

  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> client(socket_name);

  client.connect();
  server.accept();
  util::Command cmd{util::Command::SendHashes};
  EXPECT_EQ(0, client.try_read<util::Command>().size());

  server.write(cmd);
  auto res = client.try_read<util::Command>();
  EXPECT_EQ(1, res.size());
  EXPECT_EQ(cmd, res[0]);

  cmd = util::Command::RecvHashes;
  EXPECT_EQ(0, server.try_read<util::Command>().size());

  client.write(cmd);
  res = server.try_read<util::Command>();
  EXPECT_EQ(1, res.size());
  EXPECT_EQ(cmd, res[0]);
}

TEST(T_IPC_SingleServerSingleClient, ExchangeMultipleCommands) {
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
  EXPECT_EQ(result[0], util::Command::RecvHashes);
  EXPECT_EQ(result[1], util::Command::RecvHashes);
  EXPECT_EQ(result[2], util::Command::SendHashes);
  EXPECT_EQ(result[3], util::Command::RecvHashes);
}

TEST(T_IPC_SingleServerSingleClient, ExchangeSingleNumber) {
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";

  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> client(socket_name);

  client.connect();
  server.accept();

  int number = 42;

  server.write(number);
  EXPECT_EQ(number, client.read<int>()[0]);

  number = 24;
  client.write(number);
  EXPECT_EQ(number, server.read<int>()[0]);
}

TEST(T_IPC_SingleServerSingleClient, ExchangeSingleNumberAsync) {
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";

  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> client(socket_name);

  client.connect();
  server.accept();

  int number = 42;
  EXPECT_EQ(0, client.try_read<int>().size());

  server.write(number);
  auto res = client.try_read<int>();
  EXPECT_EQ(1, res.size());
  EXPECT_EQ(number, res[0]);

  number = 24;
  EXPECT_EQ(0, server.try_read<int>().size());

  client.write(number);
  res = server.try_read<int>();
  EXPECT_EQ(1, res.size());
  EXPECT_EQ(number, res[0]);
}

TEST(T_IPC_SingleServerSingleClient, ExchangeMultipleNumbers) {
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";

  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> client(socket_name);

  client.connect();
  server.accept();

  server.write(42).write(43).write(44).write(45);

  auto result = client.read<int>(4);
  EXPECT_EQ(result[0], 42);
  EXPECT_EQ(result[1], 43);
  EXPECT_EQ(result[2], 44);
  EXPECT_EQ(result[3], 45);
}

TEST(T_IPC_SingleServerSingleClient, ExchangeSingleHash) {
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";

  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> client(socket_name);

  client.connect();
  server.accept();

  shash::Any hash;
  hash.Randomize(42);

  client.write(hash);
  EXPECT_EQ(hash, server.read<shash::Any>()[0]);
}

TEST(T_IPC_SingleServerSingleClient, ExchangeSingleHashAsync) {
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";

  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> client(socket_name);

  client.connect();
  server.accept();

  shash::Any hash;
  hash.Randomize(42);
  EXPECT_EQ(0, server.try_read<shash::Any>().size());

  client.write(hash);
  auto res = server.try_read<shash::Any>();
  EXPECT_EQ(1, res.size());
  EXPECT_EQ(hash, res[0]);
}

TEST(T_IPC_SingleServerSingleClient, ExchangeMultipleHashes) {
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";

  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> client(socket_name);

  constexpr size_t N = 250;
  std::vector<shash::Any> hashes;
  for (size_t i = 0; i < N; ++i) {
    shash::Any hash;
    hash.Randomize(i);
    hashes.emplace_back(hash);
  }

  client.connect();
  server.accept();

  for (size_t i = 0; i < N; ++i) {
    client.write(hashes[i]);
  }
  std::vector<shash::Any> result = server.read<shash::Any>(N);
  for (size_t i = 0; i < N; ++i) {
    EXPECT_EQ(hashes[i], result[i]);
  }
}

TEST(T_IPC_SingleServerSingleClient, ExchangeMultipleHashesAsync) {
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";

  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> client(socket_name);

  constexpr size_t N = 250;
  std::vector<shash::Any> hashes;
  for (size_t i = 0; i < N; ++i) {
    shash::Any hash;
    hash.Randomize(i);
    hashes.emplace_back(hash);
  }

  client.connect();
  server.accept();

  EXPECT_EQ(0, server.try_read<shash::Any>(N).size());

  for (size_t i = 0; i < N; ++i) {
    client.write(hashes[i]);
  }
  std::vector<shash::Any> result = server.try_read<shash::Any>(N);

  EXPECT_EQ(N, result.size());
  for (size_t i = 0; i < N; ++i) {
    EXPECT_EQ(hashes[i], result[i]);
  }
}

TEST(T_IPC_SingleServerMultipleClients, ExchangeSingleCommand) {
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";
  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> c0(socket_name);
  LocalUnixSocket<ProcessType::Client> c1(socket_name);

  c0.connect();
  c1.connect();
  server.accept();
  server.accept();

  EXPECT_EQ(server.nclients(), 2);

  util::Command send{util::Command::SendHashes};
  util::Command recv{util::Command::RecvHashes};
  server.write(send, 0);
  server.write(recv, 1);

  EXPECT_EQ(send, c0.read<util::Command>()[0]);
  EXPECT_EQ(recv, c1.read<util::Command>()[0]);
}

TEST(T_IPC_SingleServerMultipleClients, ExchangeSingleNumber) {
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";
  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> c0(socket_name);
  LocalUnixSocket<ProcessType::Client> c1(socket_name);

  c0.connect();
  c1.connect();
  server.accept();
  server.accept();

  EXPECT_EQ(server.nclients(), 2);

  c0.write(42);
  c1.write(24);

  EXPECT_EQ(42, server.read<int>(1, 0)[0]);
  EXPECT_EQ(24, server.read<int>(1, 1)[0]);
}

TEST(T_IPC_SingleServerMultipleClients, ExchangeSingleHash) {
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
  hashes[0] = hash;
  hash.Randomize(24);
  hashes[1] = hash;

  c0.write(hashes[0]);
  c1.write(hashes[1]);

  EXPECT_EQ(hashes[0], server.read<shash::Any>(1, 0)[0]);
  EXPECT_EQ(hashes[1], server.read<shash::Any>(1, 1)[0]);
}

TEST(T_IPC_SingleServerMultipleClients, RealisticCase) {
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";
  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> c0(socket_name);
  LocalUnixSocket<ProcessType::Client> c1(socket_name);

  c0.connect();
  c1.connect();
  server.accept();
  server.accept();

  EXPECT_EQ(server.nclients(), 2);

  server.write(util::Command::SendHashes, 0);
  server.write(util::Command::SendHashes, 1);

  EXPECT_EQ(util::Command::SendHashes, c0.read<util::Command>()[0]);
  EXPECT_EQ(util::Command::SendHashes, c1.read<util::Command>()[0]);

  c0.write(util::Command::RecvHashes);
  c1.write(util::Command::RecvHashes);

  EXPECT_EQ(util::Command::RecvHashes, server.read<util::Command>(1, 0)[0]);
  EXPECT_EQ(util::Command::RecvHashes, server.read<util::Command>(1, 1)[0]);
  constexpr size_t c0_hash_n = 42, c1_hash_n = 35;
  shash::Any c0_hashes[c0_hash_n], c1_hashes[c1_hash_n];
  shash::Any hash;

  for (size_t i = 0; i < c0_hash_n; ++i) {
    hash.Randomize(i);
    c0_hashes[i] = hash;
    c0.write(hash);
  }
  std::vector<shash::Any> c0_res = server.read<shash::Any>(c0_hash_n, 0);
  for (size_t i = 0; i < c0_hash_n; ++i) {
    EXPECT_EQ(c0_hashes[i], c0_res[i]);
  }

  for (size_t i = 0; i < c1_hash_n; ++i) {
    hash.Randomize(i);
    c1_hashes[i] = hash;
    c1.write(hash);
  }
  std::vector<shash::Any> c1_res = server.read<shash::Any>(c1_hash_n, 1);
  for (size_t i = 0; i < c1_hash_n; ++i) {
    EXPECT_EQ(c1_hashes[i], c1_res[i]);
  }
}

TEST(T_IPC_SingleServerMultipleClients, RealisticCaseAsync) {
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";
  LocalUnixSocket<ProcessType::Server> server{socket_name};
  LocalUnixSocket<ProcessType::Client> c0(socket_name);
  LocalUnixSocket<ProcessType::Client> c1(socket_name);

  c0.connect();
  c1.connect();
  server.accept();
  server.accept();

  EXPECT_EQ(server.nclients(), 2);

  // CLIENTS are waiting for a command
  EXPECT_EQ(0, c0.try_read<util::Command>().size());
  EXPECT_EQ(0, c0.try_read<util::Command>().size());
  EXPECT_EQ(0, c1.try_read<util::Command>().size());
  EXPECT_EQ(0, c1.try_read<util::Command>().size());
  EXPECT_EQ(0, c0.try_read<util::Command>().size());
  EXPECT_EQ(0, c0.try_read<util::Command>().size());
  EXPECT_EQ(0, c1.try_read<util::Command>().size());
  EXPECT_EQ(0, c1.try_read<util::Command>().size());

  // SERVER eventually sends the command
  server.write(util::Command::SendHashes, 0);
  server.write(util::Command::SendHashes, 1);

  // CLIENTS receive the command
  EXPECT_EQ(util::Command::SendHashes, c0.try_read<util::Command>()[0]);
  EXPECT_EQ(util::Command::SendHashes, c1.try_read<util::Command>()[0]);

  // Meanwhile SERVER is waiting for a response
  EXPECT_EQ(0, server.try_read<util::Command>(1, 0).size());
  EXPECT_EQ(0, server.try_read<util::Command>(1, 0).size());
  EXPECT_EQ(0, server.try_read<util::Command>(1, 1).size());
  EXPECT_EQ(0, server.try_read<util::Command>(1, 1).size());
  EXPECT_EQ(0, server.try_read<util::Command>(1, 0).size());
  EXPECT_EQ(0, server.try_read<util::Command>(1, 0).size());
  EXPECT_EQ(0, server.try_read<util::Command>(1, 1).size());
  EXPECT_EQ(0, server.try_read<util::Command>(1, 1).size());

  // CLIENTS respond
  c0.write(util::Command::RecvHashes);
  c1.write(util::Command::RecvHashes);

  // SERVER receives the response
  EXPECT_EQ(util::Command::RecvHashes, server.try_read<util::Command>(1, 0)[0]);
  EXPECT_EQ(util::Command::RecvHashes, server.try_read<util::Command>(1, 1)[0]);
  constexpr size_t c0_hash_n = 42, c1_hash_n = 35;
  shash::Any c0_hashes[c0_hash_n], c1_hashes[c1_hash_n];
  shash::Any hash;

  // Meanwhile SERVER is waiting for the hashes
  EXPECT_EQ(0, server.try_read<shash::Any>(c0_hash_n, 0).size());
  EXPECT_EQ(0, server.try_read<shash::Any>(c0_hash_n, 0).size());
  EXPECT_EQ(0, server.try_read<shash::Any>(c1_hash_n, 1).size());
  EXPECT_EQ(0, server.try_read<shash::Any>(c1_hash_n, 1).size());
  EXPECT_EQ(0, server.try_read<shash::Any>(c0_hash_n, 0).size());
  EXPECT_EQ(0, server.try_read<shash::Any>(c1_hash_n, 1).size());

  // c0 SENDS its hashes
  for (size_t i = 0; i < c0_hash_n; ++i) {
    hash.Randomize(i);
    c0_hashes[i] = hash;
    c0.write(hash);
  }

  // SERVER gets the hashes of c0, while waiting for the hashes of c1
  std::vector<shash::Any> c0_res = server.try_read<shash::Any>(c0_hash_n, 0);
  EXPECT_EQ(c0_hash_n, c0_res.size());
  for (size_t i = 0; i < c0_hash_n; ++i) {
    EXPECT_EQ(c0_hashes[i], c0_res[i]);
  }

  EXPECT_EQ(0, server.try_read<shash::Any>(c1_hash_n, 1).size());
  EXPECT_EQ(0, server.try_read<shash::Any>(c1_hash_n, 1).size());

  // SERVER gets the hashes of c1
  for (size_t i = 0; i < c1_hash_n; ++i) {
    hash.Randomize(i);
    c1_hashes[i] = hash;
    c1.write(hash);
  }
  std::vector<shash::Any> c1_res = server.try_read<shash::Any>(c1_hash_n, 1);
  EXPECT_EQ(c1_hash_n, c1_res.size());
  for (size_t i = 0; i < c1_hash_n; ++i) {
    EXPECT_EQ(c1_hashes[i], c1_res[i]);
  }
}

TEST(T_IPC_QuotaCacheMgrsCommunication, CollectHashes) {
  constexpr char socket_name[] = "/tmp/socket-exp/test.sock";
  QuotaManagerSocket qm{socket_name};

  constexpr size_t cm0_n = 42, cm1_n = 35;
  shash::Any hash;
  shash::Any cm0_hashes[cm0_n], cm1_hashes[cm1_n];
  for (size_t i = 0; i < cm0_n; ++i) {
    hash.Randomize(i);
    cm0_hashes[i] = hash;
  }
  for (size_t i = 0; i < cm1_n; ++i) {
    hash.Randomize(i);
    cm1_hashes[i] = hash;
  }

  pid_t pid = fork();
  // handshake to establish communication with each CacheManager
  switch (pid) {
    case -1:
      ASSERT_TRUE(false);
      break;
    case 0:
      // CacheManager-s code
      CacheManagerSocket cm0{socket_name};
      CacheManagerSocket cm1{socket_name};
      cm0.connect();
      cm1.connect();

      auto cmd = cm0.read<util::Command>();
      EXPECT_TRUE(cmd.size() > 0);
      EXPECT_EQ(cmd[0], util::Command::SendHashes);
      cm0.write(util::Command::RecvHashes);
      cm0.write(cm0_n);
      for (size_t i = 0; i < cm0_n; ++i) {
        cm0.write(cm0_hashes[i]);
      }

      cmd = cm1.read<util::Command>();
      EXPECT_TRUE(cmd.size() > 0);
      EXPECT_EQ(cmd[0], util::Command::SendHashes);
      cm1.write(util::Command::RecvHashes);
      cm1.write(cm1_n);
      for (size_t i = 0; i < cm1_n; ++i) {
        cm0.write(cm1_hashes[i]);
      }

      _exit(0);
  }
  // QuotaManager code
  qm.accept();
  // asking for hashes
  std::set<shash::Any> collected = qm.collect<shash::Any>();
  for (size_t i = 0; i < cm0_n; ++i) {
    EXPECT_TRUE(collected.find(cm0_hashes[i]) != collected.end());
  }
  for (size_t i = 0; i < cm1_n; ++i) {
    EXPECT_TRUE(collected.find(cm1_hashes[i]) != collected.end());
  }
}

