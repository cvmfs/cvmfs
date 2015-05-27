/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <pthread.h>
#include <signal.h>

#include "../../cvmfs/cache.h"
#include "../../cvmfs/quota.h"
#include "testutil.h"

using namespace std;  // NOLINT

class T_QuotaManager : public ::testing::Test {
 protected:
  virtual void SetUp() {
    used_fds_ = GetNoUsedFds();
    sigpipe_save_ = signal(SIGPIPE, SIG_IGN);

    // Prepare cache directories
    tmp_path_ = CreateTempDir("/tmp/cvmfs_test", 0700);
    delete cache::PosixCacheManager::Create(tmp_path_, false);

    quota_mgr_ =
      PosixQuotaManager::Create(tmp_path_, 10*1024*1024, 5*1024*1024, false);
    ASSERT_TRUE(quota_mgr_ != NULL);

    quota_mgr_->Spawn();
  }

  virtual void TearDown() {
    delete quota_mgr_;
    signal(SIGPIPE, sigpipe_save_);
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
    EXPECT_EQ(used_fds_, GetNoUsedFds());
  }

 protected:
  PosixQuotaManager *quota_mgr_;
  string tmp_path_;
  unsigned used_fds_;
  sig_t sigpipe_save_;
};


TEST_F(T_QuotaManager, BroadcastBackchannels) {
  // Don't die without channels
  quota_mgr_->BroadcastBackchannels("X");

  int channel1[2];
  int channel2[2];
  int channel3[2];
  quota_mgr_->RegisterBackChannel(channel1, "A");
  quota_mgr_->RegisterBackChannel(channel2, "B");
  quota_mgr_->RegisterBackChannel(channel3, "C");
  quota_mgr_->BroadcastBackchannels("X");
  char buf[3];
  ReadPipe(channel1[0], &buf[0], 1);
  EXPECT_EQ('X', buf[0]);
  ReadPipe(channel2[0], &buf[1], 1);
  EXPECT_EQ('X', buf[1]);
  ReadPipe(channel3[0], &buf[2], 1);
  EXPECT_EQ('X', buf[2]);

  // One dies and should get unregistered
  EXPECT_EQ(0, close(channel2[0]));
  quota_mgr_->BroadcastBackchannels("X");
  char buf2[2];
  ReadPipe(channel1[0], &buf2[0], 1);
  EXPECT_EQ('X', buf2[0]);
  ReadPipe(channel3[0], &buf2[1], 1);
  EXPECT_EQ('X', buf2[1]);

  // One gets unregistered
  quota_mgr_->UnregisterBackChannel(channel1, "A");
  // Useless command to make sure the first one is done
  quota_mgr_->List();
  quota_mgr_->BroadcastBackchannels("X");
  char buf3;
  ReadPipe(channel3[0], &buf3, 1);
  EXPECT_EQ('X', buf3);

  // channel3[1] should be closed by the destructor of the quota manager
  close(channel3[0]);
}


TEST_F(T_QuotaManager, BindReturnPipe) {
  EXPECT_EQ(42, quota_mgr_->BindReturnPipe(42));

  quota_mgr_->shared_ = true;
  int pipe_test[2];
  quota_mgr_->MakeReturnPipe(pipe_test);
  int fd = quota_mgr_->BindReturnPipe(pipe_test[1]);
  EXPECT_GE(fd, 0);
  quota_mgr_->UnbindReturnPipe(pipe_test[1]);
  quota_mgr_->UnlinkReturnPipe(pipe_test[1]);
  close(pipe_test[0]);
  EXPECT_EQ(-1, quota_mgr_->BindReturnPipe(pipe_test[1]));
  quota_mgr_->shared_ = false;
}
