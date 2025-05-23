/**
 * This file is part of the CernVM File System.
 */

#include <fcntl.h>
#include <gtest/gtest.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cassert>
#include <cstring>

#include "publish/cmd_util.h"
#include "publish/except.h"
#include "publish/repository_util.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace publish {

class T_Util : public ::testing::Test {
 protected:
  virtual void SetUp() { }

 protected:
};

TEST_F(T_Util, CheckoutMarker3) {
  EXPECT_EQ(NULL, CheckoutMarker::CreateFrom("/no/such/path"));

  shash::Any hash = shash::MkFromHexPtr(
      shash::HexPtr("0123456789abcdef0123456789abcdef01234567"),
      shash::kSuffixCatalog);
  CheckoutMarker m("tag", "branch", hash, "");
  m.SaveAs("cvmfs_test_checkout_marker");

  CheckoutMarker *l = CheckoutMarker::CreateFrom("cvmfs_test_checkout_marker");
  ASSERT_TRUE(l != NULL);
  EXPECT_EQ(m.tag(), l->tag());
  EXPECT_EQ(m.branch(), l->branch());
  EXPECT_EQ(m.hash(), l->hash());
  EXPECT_EQ(m.previous_branch(), l->previous_branch());
  delete l;
}


TEST_F(T_Util, CheckoutMarker4) {
  EXPECT_EQ(NULL, CheckoutMarker::CreateFrom("/no/such/path"));

  shash::Any hash = shash::MkFromHexPtr(
      shash::HexPtr("0123456789abcdef0123456789abcdef01234567"),
      shash::kSuffixCatalog);
  CheckoutMarker m("tag", "branch", hash, "prev");
  m.SaveAs("cvmfs_test_checkout_marker");

  CheckoutMarker *l = CheckoutMarker::CreateFrom("cvmfs_test_checkout_marker");
  ASSERT_TRUE(l != NULL);
  EXPECT_EQ(m.tag(), l->tag());
  EXPECT_EQ(m.branch(), l->branch());
  EXPECT_EQ(m.hash(), l->hash());
  EXPECT_EQ(m.previous_branch(), l->previous_branch());
  delete l;
}


TEST_F(T_Util, ServerLockFile) {
  ServerLockFile lock("foo.lock");

  EXPECT_TRUE(lock.TryLock());
  EXPECT_FALSE(lock.TryLock());
  lock.Unlock();
  EXPECT_TRUE(lock.TryLock());
  lock.Unlock();

  {
    ServerLockFileCheck check1(lock);
    EXPECT_TRUE(check1.owns_lock());
    ServerLockFileCheck check2(lock);
    EXPECT_FALSE(check2.owns_lock());
  }

  {
    ServerLockFileCheck guard1(lock);
    EXPECT_THROW(ServerLockFileGuard guard2(lock), EPublish);
  }

  pid_t pid_child = fork();
  ASSERT_GE(pid_child, 0);
  if (pid_child == 0) {
    ServerLockFileCheck check3(lock);
    EXPECT_TRUE(check3.owns_lock());
    exit(0);
  }
  EXPECT_EQ(0, WaitForChild(pid_child));

  {
    ServerLockFileCheck check4(lock);
    EXPECT_TRUE(check4.owns_lock());
  }
}

TEST_F(T_Util, ServerFlagFile) {
  ServerFlagFile flag("foo.flag");
  EXPECT_FALSE(flag.IsSet());
  flag.Set();
  EXPECT_TRUE(flag.IsSet());
  flag.Clear();
  EXPECT_FALSE(flag.IsSet());

  pid_t pid_child = fork();
  ASSERT_GE(pid_child, 0);
  if (pid_child == 0) {
    flag.Set();
    EXPECT_TRUE(flag.IsSet());
    exit(0);
  }
  EXPECT_EQ(0, WaitForChild(pid_child));

  EXPECT_TRUE(flag.IsSet());
  flag.Clear();
}

TEST_F(T_Util, SetInConfig) {
  EXPECT_THROW(SetInConfig("/no/such/file", "x", "y"), EPublish);
  EXPECT_FALSE(FileExists("test_publish_config"));
  SetInConfig("test_publish_config", "X", "y");
  EXPECT_TRUE(FileExists("test_publish_config"));

  int fd = open("test_publish_config", O_RDONLY);
  EXPECT_GE(fd, 0);
  std::string content;
  EXPECT_TRUE(SafeReadToString(fd, &content));
  close(fd);
  EXPECT_EQ("X=y\n", content);

  SetInConfig("test_publish_config", "X", "z");
  fd = open("test_publish_config", O_RDONLY);
  EXPECT_GE(fd, 0);
  EXPECT_TRUE(SafeReadToString(fd, &content));
  close(fd);
  EXPECT_EQ("X=z\n", content);

  SetInConfig("test_publish_config", "A", "b");
  fd = open("test_publish_config", O_RDONLY);
  EXPECT_GE(fd, 0);
  EXPECT_TRUE(SafeReadToString(fd, &content));
  close(fd);
  EXPECT_EQ("X=z\nA=b\n", content);

  SetInConfig("test_publish_config", "X", "");
  fd = open("test_publish_config", O_RDONLY);
  EXPECT_GE(fd, 0);
  EXPECT_TRUE(SafeReadToString(fd, &content));
  close(fd);
  EXPECT_EQ("A=b\n", content);
}

TEST_F(T_Util, CallServerHook) {
  EXPECT_EQ(0, CallServerHook("hookX", "t.cvmfs.io", "no/such/file"));

  EXPECT_TRUE(SafeWriteToFile(
      "hookY() { if [ $1 = \"t.cvmfs.io\" ]; then return 0; fi; return 42; }\n",
      "cvmfs_test_hooks.sh", 0644));

  EXPECT_EQ(0, CallServerHook("hookX", "t.cvmfs.io", "./cvmfs_test_hooks.sh"));
  EXPECT_EQ(0, CallServerHook("hookY", "t.cvmfs.io", "./cvmfs_test_hooks.sh"));
  EXPECT_EQ(42, CallServerHook("hookY", "x.cvmfs.io", "./cvmfs_test_hooks.sh"));
}

namespace {

void *MainTalkCommandAnswer(void *data) {
  int socket_fd = *reinterpret_cast<int *>(data);
  struct sockaddr_un remote;
  socklen_t socket_size = sizeof(remote);
  int con_fd = accept(socket_fd, (struct sockaddr *)&remote, &socket_size);
  assert(con_fd > 0);
  char buf[32];
  int nbytes = recv(con_fd, buf, sizeof(buf) - 1, 0);
  buf[nbytes] = '\0';
  assert(strcmp("command", buf) == 0);
  std::string answer = "answer";
  (void)send(con_fd, &answer[0], answer.length(), MSG_NOSIGNAL);
  shutdown(con_fd, SHUT_RDWR);
  close(con_fd);
  return NULL;
}

}  // anonymous namespace

TEST_F(T_Util, SendTalkCommand) {
  EXPECT_THROW(SendTalkCommand("/no/such/file", "command"), EPublish);
  int socket_fd = MakeSocket("cvmfs_test_socket", 0600);
  ASSERT_GE(socket_fd, 0);
  ASSERT_EQ(0, listen(socket_fd, 1));
  pthread_t thread;
  int retval = pthread_create(&thread, NULL, MainTalkCommandAnswer, &socket_fd);
  ASSERT_EQ(0, retval);

  std::string answer = SendTalkCommand("cvmfs_test_socket", "command");
  pthread_join(thread, NULL);
  EXPECT_STREQ("answer", answer.c_str());

  close(socket_fd);
  unlink("cvmfs_test_socket");
}

}  // namespace publish
