/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <string>

#include "voms_authz/authz.h"
#include "voms_authz/authz_fetch.h"

using namespace std;  // NOLINT


class T_AuthzFetch : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
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
