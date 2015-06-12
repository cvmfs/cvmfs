/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <fcntl.h>

#include "../../cvmfs/fetch.h"
#include "../../cvmfs/hash.h"
#include "../../cvmfs/util.h"
#include "testutil.h"

using namespace std;  // NOLINT

namespace cvmfs {

class T_Fetcher : public ::testing::Test {
 protected:
  virtual void SetUp() {
    used_fds_ = GetNoUsedFds();
    for (unsigned i = 0; i < 8; ++i) {
      hashes_.push_back(shash::Any(shash::kSha1));
      hashes_[i].digest[0] = i;
    }
  }

  virtual void TearDown() {
    EXPECT_EQ(used_fds_, GetNoUsedFds());
  }

  Fetcher *fetcher_;
  unsigned used_fds_;
  vector<shash::Any> hashes_;
};


TEST_F(T_Fetcher, SignalWaitingThreads) {
  Fetcher tmp("", NULL, NULL);
  int fd = open("/dev/null", O_RDONLY);
  EXPECT_GE(fd, 0);
  int tls_pipe[2];
  MakePipe(tls_pipe);

  tmp.queues_download_[hashes_[0]] = NULL;
  tmp.queues_download_[hashes_[1]] = NULL;
  tmp.queues_download_[hashes_[2]] = NULL;

  tmp.GetTls()->other_pipes_waiting.push_back(tls_pipe[1]);
  tmp.SignalWaitingThreads(-1, hashes_[0], tmp.GetTls());
  EXPECT_EQ(0U, tmp.queues_download_.count(hashes_[0]));

  tmp.GetTls()->other_pipes_waiting.push_back(tls_pipe[1]);
  tmp.SignalWaitingThreads(fd, hashes_[1], tmp.GetTls());
  EXPECT_EQ(0U, tmp.queues_download_.count(hashes_[1]));

  tmp.GetTls()->other_pipes_waiting.push_back(tls_pipe[1]);
  tmp.SignalWaitingThreads(1000000, hashes_[2], tmp.GetTls());
  EXPECT_EQ(0U, tmp.queues_download_.count(hashes_[2]));

  int fd_return0;
  int fd_return1;
  int fd_return2;
  ReadPipe(tls_pipe[0], &fd_return0, sizeof(fd_return0));
  ReadPipe(tls_pipe[0], &fd_return1, sizeof(fd_return1));
  ReadPipe(tls_pipe[0], &fd_return2, sizeof(fd_return2));
  EXPECT_EQ(-1, fd_return0);
  EXPECT_NE(fd, fd_return1);
  EXPECT_EQ(0, close(fd_return1));
  EXPECT_EQ(-EBADF, fd_return2);

  ClosePipe(tls_pipe);
  close(fd);
}

}  // namespace cvmfs
