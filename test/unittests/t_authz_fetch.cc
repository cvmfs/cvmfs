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

#include "authz/authz.h"
#include "authz/authz_fetch.h"
#include "options.h"
#include "util/posix.h"
#include "util/string.h"

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

  struct HelperFds {
    int fd_stdin;
    int fd_stdout;
  };


  static void RecvAndFind(int fd_stdin, const string &json_snippet) {
    uint32_t version;
    uint32_t length;
    SafeRead(fd_stdin, &version, sizeof(version));
    EXPECT_EQ(AuthzExternalFetcher::kProtocolVersion, version);
    SafeRead(fd_stdin, &length, sizeof(length));
    EXPECT_LE(length, kPageSize);
    char buf[kPageSize];
    SafeRead(fd_stdin, buf, length);
    string msg_syn(buf, length);
    EXPECT_NE(string::npos, msg_syn.find(json_snippet));
  }

  static void MakeHandshake(int fd_stdin, int fd_stdout) {
    RecvAndFind(fd_stdin, "\"msgid\":0");
    string ready_msg = "{\"cvmfs_authz_v1\": {\"msgid\": 1, \"revision\": 0}}";
    uint32_t version = AuthzExternalFetcher::kProtocolVersion;
    uint32_t length = ready_msg.length();
    SafeWrite(fd_stdout, &version, sizeof(version));
    SafeWrite(fd_stdout, &length, sizeof(length));
    SafeWrite(fd_stdout, ready_msg.data(), length);
  }

  static void *MainSendHandshake(void *data) {
    HelperFds helper_fds = *reinterpret_cast<HelperFds *>(data);
    MakeHandshake(helper_fds.fd_stdin, helper_fds.fd_stdout);
    return NULL;
  }

  static void *MainOneAuth(void *data) {
    HelperFds helper_fds = *reinterpret_cast<HelperFds *>(data);
    RecvAndFind(helper_fds.fd_stdin, "\"uid\":2");
    string auth_msg = string("{\"cvmfs_authz_v1\":")
                      + "{\"msgid\": 3, \"revision\": 0, \"status\": 0, "
                        "\"ttl\": 60,"
                      + "\"x509_proxy\": \"" + Base64(string(8192, 'X'))
                      + "\"}}";
    uint32_t version = AuthzExternalFetcher::kProtocolVersion;
    uint32_t length = auth_msg.length();
    SafeWrite(helper_fds.fd_stdout, &version, sizeof(version));
    SafeWrite(helper_fds.fd_stdout, &length, sizeof(length));
    SafeWrite(helper_fds.fd_stdout, auth_msg.data(), length);
    return NULL;
  }

  SimpleOptionsParser options_mgr_;
  AuthzExternalFetcher *fetcher_;
  int pipe_send_[2];
  int pipe_recv_[2];
};


TEST_F(T_AuthzFetch, ExecHelper) {
  AuthzExternalFetcher *authz_fetcher = new AuthzExternalFetcher(
      "X", "/bin/sh", "", &options_mgr_);
  authz_fetcher->ExecHelper();
  EXPECT_TRUE(authz_fetcher->Send("\n/bin/echo hello\n"));
  string dummy;
  EXPECT_FALSE(authz_fetcher->Recv(&dummy));
  EXPECT_TRUE(authz_fetcher->fail_state_);
  delete authz_fetcher;

  authz_fetcher = new AuthzExternalFetcher("X", "/bin/sh", "", &options_mgr_);
  authz_fetcher->ExecHelper();
  kill(authz_fetcher->pid_, SIGKILL);
  int statloc;
  waitpid(authz_fetcher->pid_, &statloc, 0);
  authz_fetcher->pid_ = -1;
  EXPECT_FALSE(authz_fetcher->Send("\n/bin/echo hello\n"));
  EXPECT_TRUE(authz_fetcher->fail_state_);
  delete authz_fetcher;

  authz_fetcher = new AuthzExternalFetcher("X", "/no/such/file", "",
                                           &options_mgr_);
  // Execve will fail but that's noted on first communication
  authz_fetcher->ExecHelper();
  // Might fail or not, depending on how fast fork is
  authz_fetcher->Send("\n/bin/echo hello\n");
  EXPECT_FALSE(authz_fetcher->Recv(&dummy));
  EXPECT_TRUE(authz_fetcher->fail_state_);
  delete authz_fetcher;
}


TEST_F(T_AuthzFetch, ExecHelperSlow) {
  AuthzExternalFetcher *authz_fetcher = new AuthzExternalFetcher(
      "X", "/bin/sh", "", &options_mgr_);
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
  EXPECT_FALSE(fetcher_->ParseMsg("", kAuthzMsgReady, &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg("{{{", kAuthzMsgReady, &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg("{\"cvmfs_authz_v2\": {}}", kAuthzMsgReady,
                                  &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg("{\"cvmfs_authz_v1\": {}}", kAuthzMsgReady,
                                  &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg("{\"cvmfs_authz_v1\": {\"msgid\": 0}}",
                                  kAuthzMsgReady, &binary_msg));
  EXPECT_TRUE(fetcher_->ParseMsg(
      "{\"cvmfs_authz_v1\": {\"msgid\": 1, \"revision\": 0}}", kAuthzMsgReady,
      &binary_msg));
  EXPECT_TRUE(fetcher_->ParseMsg(
      "{\"cvmfs_authz_v1\": {\"msgid\": 1, \"revision\": 0, null}}",
      kAuthzMsgReady, &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg(
      "{\"cvmfs_authz_v1\": {\"msgid\": 1, \"revision\": 0, null}}",
      kAuthzMsgHandshake, &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg(
      "{\"cvmfs_authz_v1\": {\"msgid\": 1000, \"revision\": 0}}",
      kAuthzMsgReady, &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg(
      "{\"cvmfs_authz_v1\": {\"msgid\": -1, \"revision\": 0}}", kAuthzMsgReady,
      &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg(
      "{\"cvmfs_authz_v1\": {\"msgid\": 1, \"revision\": -1}}", kAuthzMsgReady,
      &binary_msg));
  EXPECT_FALSE(fetcher_->ParseMsg(
      "{\"cvmfs_authz_v1\": {\"msgid\": \"1\", \"revision\": 0}}",
      kAuthzMsgReady, &binary_msg));
  EXPECT_TRUE(fetcher_->ParseMsg(
      string("{\"cvmfs_authz_v1\":")
          + "{\"msgid\": 3, \"revision\": 0, \"status\": 0}}",
      kAuthzMsgPermit, &binary_msg));
  EXPECT_TRUE(fetcher_->ParseMsg(
      string("{\"cvmfs_authz_v1\":")
          + "{\"msgid\": 3, \"revision\": 0, \"status\": 0, \"ttl\": 120}}",
      kAuthzMsgPermit, &binary_msg));
  EXPECT_EQ(kAuthzOk, binary_msg.permit.status);
  EXPECT_EQ(120U, binary_msg.permit.ttl);
  EXPECT_EQ(kTokenUnknown, binary_msg.permit.token.type);
  EXPECT_TRUE(fetcher_->ParseMsg(
      string("{\"cvmfs_authz_v1\":")
          + "{\"msgid\": 3, \"revision\": 0, \"status\": 0, \"ttl\": 240, "
          + "\"x509_proxy\": \"" + Base64("XYZ") + "\"}}",
      kAuthzMsgPermit, &binary_msg));
  EXPECT_EQ(kAuthzOk, binary_msg.permit.status);
  EXPECT_EQ(240U, binary_msg.permit.ttl);
  EXPECT_EQ(kTokenX509, binary_msg.permit.token.type);
  ASSERT_EQ(3U, binary_msg.permit.token.size);
  EXPECT_EQ("XYZ",
            string(reinterpret_cast<char *>(binary_msg.permit.token.data), 3));
  free(binary_msg.permit.token.data);
  EXPECT_TRUE(fetcher_->ParseMsg(
      string("{\"cvmfs_authz_v1\":")
          + "{\"msgid\": 3, \"revision\": 0, \"status\": 3, \"ttl\": 120}}",
      kAuthzMsgPermit, &binary_msg));
  EXPECT_EQ(kAuthzNotMember, binary_msg.permit.status);
  EXPECT_FALSE(
      fetcher_->ParseMsg(string("{\"cvmfs_authz_v1\":")
                             + "{\"msgid\": 3, \"revision\": 0, \"ttl\": 120}}",
                         kAuthzMsgPermit, &binary_msg));
}


TEST_F(T_AuthzFetch, Handshake) {
  HelperFds helper_fds;
  helper_fds.fd_stdin = pipe_send_[0];
  helper_fds.fd_stdout = pipe_recv_[1];
  pthread_t thread_handshake;
  ASSERT_EQ(
      0,
      pthread_create(&thread_handshake, NULL, MainSendHandshake, &helper_fds));
  EXPECT_TRUE(fetcher_->Handshake());
  pthread_join(thread_handshake, NULL);

  AuthzExternalFetcher *authz_fetcher = new AuthzExternalFetcher(
      "X", "/no/such/file", "", &options_mgr_);
  EXPECT_FALSE(authz_fetcher->Handshake());
  delete authz_fetcher;
}


TEST_F(T_AuthzFetch, Fetch) {
  HelperFds helper_fds;
  helper_fds.fd_stdin = pipe_send_[0];
  helper_fds.fd_stdout = pipe_recv_[1];
  pthread_t thread_auth;
  ASSERT_EQ(0, pthread_create(&thread_auth, NULL, MainOneAuth, &helper_fds));
  AuthzToken token;
  unsigned ttl;
  EXPECT_EQ(kAuthzOk, fetcher_->Fetch(AuthzFetcher::QueryInfo(1, 2, 3, "X"),
                                      &token, &ttl));
  pthread_join(thread_auth, NULL);
  EXPECT_EQ(60U, ttl);
  EXPECT_EQ(kTokenX509, token.type);
  EXPECT_EQ(8192U, token.size);
  EXPECT_TRUE(token.data != NULL);
  free(token.data);
}
