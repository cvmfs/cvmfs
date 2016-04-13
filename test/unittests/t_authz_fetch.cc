/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <string>

#include "util/posix.h"
#include "voms_authz/authz.h"
#include "voms_authz/authz_fetch.h"

using namespace std;  // NOLINT


class T_AuthzFetch : public ::testing::Test {
 protected:
  virtual void SetUp() {
    MakePipe(pipe_send_);
    MakePipe(pipe_recv_);
    fetcher_ = new AuthzExternalFetcher("X", pipe_send_[1], pipe_recv_[0]);
  }

  virtual void TearDown() {
    delete fetcher_;
    close(pipe_send_[0]);
    close(pipe_recv_[1]);
  }

  static void *MainSendHandshake(void *data) {
    int fd = *reinterpret_cast<int *>(data);
    string ready_msg = "{\"cvmfs_authz_v1\": {\"msgid\": 1, \"revision\": 0}}";
    uint32_t version = AuthzExternalFetcher::kProtocolVersion;
    uint32_t length = ready_msg.length();
    SafeWrite(fd, &version, sizeof(version));
    SafeWrite(fd, &length, sizeof(length));
    SafeWrite(fd, ready_msg.data(), length);
    return NULL;
  }

  AuthzExternalFetcher *fetcher_;
  int pipe_send_[2];
  int pipe_recv_[2];
};


TEST_F(T_AuthzFetch, ExecHelper) {
  AuthzExternalFetcher *authz_fetcher =
    new AuthzExternalFetcher("X", "/bin/sh");
  authz_fetcher->ExecHelper();
  EXPECT_TRUE(authz_fetcher->Send("\n/bin/echo hello\n"));
  string dummy;
  EXPECT_FALSE(authz_fetcher->Recv(&dummy));
  EXPECT_TRUE(authz_fetcher->fail_state_);
  delete authz_fetcher;

  authz_fetcher = new AuthzExternalFetcher("X", "/bin/sh");
  authz_fetcher->ExecHelper();
  kill(authz_fetcher->pid_, SIGKILL);
  int statloc;
  waitpid(authz_fetcher->pid_, &statloc, 0);
  authz_fetcher->pid_ = -1;
  EXPECT_FALSE(authz_fetcher->Send("\n/bin/echo hello\n"));
  EXPECT_TRUE(authz_fetcher->fail_state_);
  delete authz_fetcher;

  authz_fetcher = new AuthzExternalFetcher("X", "/no/such/file");
  // Execve will fail but that's noted on first communication
  authz_fetcher->ExecHelper();
  // Might fail or not, depending on how fast fork is
  authz_fetcher->Send("\n/bin/echo hello\n");
  EXPECT_FALSE(authz_fetcher->Recv(&dummy));
  EXPECT_TRUE(authz_fetcher->fail_state_);
  delete authz_fetcher;
}


TEST_F(T_AuthzFetch, ExecHelperSlow) {
  AuthzExternalFetcher *authz_fetcher =
    new AuthzExternalFetcher("X", "/bin/sh");
  authz_fetcher->ExecHelper();
  // Make /bin/sh hang on open stdin/stdout
  int fd_send = authz_fetcher->fd_send_;
  int fd_recv = authz_fetcher->fd_recv_;
  authz_fetcher->fd_send_ = authz_fetcher->fd_recv_ = -1;
  // Should take a little but not hang
  delete authz_fetcher;
  close(fd_send);
  close(fd_recv);
}


TEST_F(T_AuthzFetch, ParseMsg) {
  AuthzExternalMsg binary_msg;
  EXPECT_FALSE(fetcher_->ParseMsg("", &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg("{{{", &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg("{\"cvmfs_authz_v2\": {}}", &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg("{\"cvmfs_authz_v1\": {}}", &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg(
    "{\"cvmfs_authz_v1\": {\"msgid\": 0}}", &binary_msg));
  EXPECT_TRUE(fetcher_->ParseMsg(
    "{\"cvmfs_authz_v1\": {\"msgid\": 0, \"revision\": 0}}", &binary_msg));
  EXPECT_TRUE(fetcher_->ParseMsg(
    "{\"cvmfs_authz_v1\": {\"msgid\": 0, \"revision\": 0, null}}",
    &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg(
    "{\"cvmfs_authz_v1\": {\"msgid\": 1000, \"revision\": 0}}", &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg(
    "{\"cvmfs_authz_v1\": {\"msgid\": -1, \"revision\": 0}}", &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg(
    "{\"cvmfs_authz_v1\": {\"msgid\": 0, \"revision\": -1}}", &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg(
    "{\"cvmfs_authz_v1\": {\"msgid\": \"0\", \"revision\": 0}}", &binary_msg));
}


TEST_F(T_AuthzFetch, Handshake) {
  pthread_t thread_handshake;
  ASSERT_EQ(0,
    pthread_create(&thread_handshake, NULL, MainSendHandshake, &pipe_recv_[1]));
  EXPECT_TRUE(fetcher_->Handshake());
  pthread_join(thread_handshake, NULL);

  AuthzExternalFetcher *authz_fetcher =
    new AuthzExternalFetcher("X", "/no/such/file");
  EXPECT_FALSE(authz_fetcher->Handshake());
  delete authz_fetcher;
}
