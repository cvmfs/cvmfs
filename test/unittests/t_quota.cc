/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <pthread.h>
#include <signal.h>

#include <algorithm>
#include <string>
#include <vector>

#include "../../cvmfs/cache.h"
#include "../../cvmfs/quota.h"
#include "../../cvmfs/util.h"
#include "testutil.h"

using namespace std;  // NOLINT

class T_QuotaManager : public ::testing::Test {
 protected:
  virtual void SetUp() {
    used_fds_ = GetNoUsedFds();
    sigpipe_save_ = signal(SIGPIPE, SIG_IGN);

    // Prepare cache directories
    tmp_path_ = CreateTempDir("/tmp/cvmfs_test", 0700);
    MkdirDeep(tmp_path_ + "/not_spawned", 0700);
    delete cache::PosixCacheManager::Create(tmp_path_, false);
    delete cache::PosixCacheManager::Create(tmp_path_ + "/not_spawned", false);

    limit_ = 10*1024*1024;  // 10M
    threshold_ = 5*1024*1024;  // 5M

    quota_mgr_ =
      PosixQuotaManager::Create(tmp_path_, limit_, threshold_, false);
    ASSERT_TRUE(quota_mgr_ != NULL);
    quota_mgr_->Spawn();

    quota_mgr_not_spawned_ =
      PosixQuotaManager::Create(tmp_path_ + "/not_spawned", limit_, threshold_,
                                false);
    ASSERT_TRUE(quota_mgr_not_spawned_ != NULL);

    for (unsigned i = 0; i < 8; ++i) {
      hashes_.push_back(shash::Any(shash::kSha1));
      hashes_[i].digest[0] = i;
    }
    prng_.InitLocaltime();
  }

  virtual void TearDown() {
    delete quota_mgr_not_spawned_;
    delete quota_mgr_;
    signal(SIGPIPE, sigpipe_save_);
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
    EXPECT_EQ(used_fds_, GetNoUsedFds());
  }

  string PrintStringVector(const vector<string> &lines) {
    string result;
    for (unsigned i = 0; i < lines.size(); ++i)
      result += lines[i] + "\n";
    return result;
  }

 protected:
  uint64_t limit_;
  uint64_t threshold_;
  PosixQuotaManager *quota_mgr_;
  PosixQuotaManager *quota_mgr_not_spawned_;
  string tmp_path_;
  unsigned used_fds_;
  sig_t sigpipe_save_;
  vector<shash::Any> hashes_;
  Prng prng_;
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
  // Trigger flush of the command buffer
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


TEST_F(T_QuotaManager, CheckHighPinWatermark) {
  int channel[2];
  quota_mgr_->RegisterBackChannel(channel, "A");
  EXPECT_TRUE(quota_mgr_->Pin(shash::Any(shash::kSha1), 4000000, "", false));
  char buf;
  ReadPipe(channel[0], &buf, 1);
  EXPECT_EQ('R', buf);
  quota_mgr_->UnregisterBackChannel(channel, "A");
}


TEST_F(T_QuotaManager, Cleanup) {
  shash::Any hash_null(shash::kSha1);
  shash::Any hash_rnd(shash::kSha1);
  hash_rnd.Randomize();
  CreateFile(tmp_path_ + hash_null.MakePathExplicit(1, 2), 0600);
  CreateFile(tmp_path_ + hash_rnd.MakePathExplicit(1, 2), 0600);
  quota_mgr_->async_delete_ = false;

  quota_mgr_->Insert(hash_null, 1, "");
  quota_mgr_->Insert(hash_rnd, 1, "");
  EXPECT_TRUE(quota_mgr_->Cleanup(3));
  EXPECT_EQ(2U, quota_mgr_->GetSize());
  EXPECT_TRUE(quota_mgr_->Cleanup(2));
  EXPECT_EQ(2U, quota_mgr_->GetSize());
  EXPECT_TRUE(quota_mgr_->Cleanup(0));
  EXPECT_EQ(0U, quota_mgr_->GetSize());
  EXPECT_FALSE(FileExists(tmp_path_ + hash_null.MakePathExplicit(1, 2)));
  EXPECT_FALSE(FileExists(tmp_path_ + hash_rnd.MakePathExplicit(1, 2)));

  quota_mgr_->Insert(hash_null, 1, "");
  EXPECT_TRUE(quota_mgr_->Pin(hash_rnd, 1, "", false));
  EXPECT_FALSE(quota_mgr_->Cleanup(0));
  EXPECT_EQ(1U, quota_mgr_->GetSize());
}


TEST_F(T_QuotaManager, CleanupLru) {
  unsigned N = hashes_.size();
  vector<shash::Any> shuffled_hashes = Shuffle(hashes_, &prng_);
  for (unsigned i = 0; i < N; ++i)
    quota_mgr_->Insert(shuffled_hashes[i], 1,
                       StringifyInt(shuffled_hashes[i].digest[0]));
  for (unsigned i = 0; i < N; ++i)
    quota_mgr_->Touch(hashes_[i]);

  EXPECT_TRUE(quota_mgr_->Cleanup(N/2));
  vector<string> remaining = quota_mgr_->List();
  EXPECT_EQ(N/2, remaining.size());
  sort(remaining.begin(), remaining.end());
  for (unsigned i = 0; i < remaining.size(); ++i) {
    EXPECT_EQ(StringifyInt(N/2 + i), remaining[i]);
  }
}


TEST_F(T_QuotaManager, CleanupVolatile) {
  unsigned N = hashes_.size();
  for (unsigned i = 0; i < N-2; ++i)
    quota_mgr_->Insert(hashes_[i], 1, StringifyInt(i));
  // Two last ones are volatile
  for (unsigned i = N-2; i < N; ++i)
    quota_mgr_->InsertVolatile(hashes_[i], 1, StringifyInt(i));

  // Remove one out of two volatile entries
  EXPECT_TRUE(quota_mgr_->Cleanup(N-1));
  vector<string> remaining = quota_mgr_->List();
  EXPECT_EQ(N-1, remaining.size());
  sort(remaining.begin(), remaining.end());
  for (unsigned i = 0; i < N-2; ++i) {
    EXPECT_EQ(StringifyInt(i), remaining[i]);
  }
  EXPECT_EQ(StringifyInt(N-1), remaining[N-2]);

  // Remove half of the entries, volatile entries first
  EXPECT_TRUE(quota_mgr_->Cleanup(N/2));
  remaining = quota_mgr_->List();
  EXPECT_EQ(N/2, remaining.size());
  sort(remaining.begin(), remaining.end());
  for (unsigned i = 0; i < remaining.size(); ++i) {
    EXPECT_EQ(StringifyInt(N/2 + i - 2), remaining[i]);
  }
}


TEST_F(T_QuotaManager, CloseDatabase) {
  // Test if all the locks on an open database are released
  shash::Any hash_null(shash::kSha1);
  shash::Any hash_rnd(shash::kSha1);
  hash_rnd.Randomize();
  quota_mgr_->Insert(hash_null, 1, "/a");
  EXPECT_TRUE(quota_mgr_->Pin(hash_rnd, 1, "/b", false));

  delete quota_mgr_;
  quota_mgr_ =
    PosixQuotaManager::Create(tmp_path_, limit_, threshold_, false);
  ASSERT_TRUE(quota_mgr_ != NULL);
  quota_mgr_->Spawn();
  vector<string> content = quota_mgr_->List();
  sort(content.begin(), content.end());
  EXPECT_EQ("/a\n/b\n", PrintStringVector(content));
}


TEST_F(T_QuotaManager, CloseReturnPipe) {
  // TODO count files in cache dir on fixture shutdown
}
