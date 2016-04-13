/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <unistd.h>

#include "voms_authz/authz.h"
#include "voms_authz/authz_fetch.h"


class T_AuthzFetch : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

};


TEST_F(T_AuthzFetch, ExecHelper) {
  // Run the code and don't segfault

  AuthzExternalFetcher *authz_fetcher =
    new AuthzExternalFetcher("X", "/bin/sh");
  authz_fetcher->ExecHelper();
  delete authz_fetcher;

  authz_fetcher = new AuthzExternalFetcher("X", "/no/such/file");
  // Execve will fail but that's noted on first communication
  authz_fetcher->ExecHelper();
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
