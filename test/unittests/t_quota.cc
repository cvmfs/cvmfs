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
#include "../../cvmfs/compression.h"
#include "../../cvmfs/fs_traversal.h"
#include "../../cvmfs/hash.h"
#include "../../cvmfs/quota.h"
#include "../../cvmfs/util.h"
#include "testutil.h"

using namespace std;  // NOLINT

class CountPipeHelper {
 public:
  CountPipeHelper() : num_pipes_(0) { }
  void EncounterPipe(const string &parent_path, const string &name) {
    num_pipes_++;
  }
  unsigned num_pipes() { return num_pipes_; }
 private:
  unsigned num_pipes_;
};


class T_QuotaManager : public ::testing::Test {
 protected:
  virtual void SetUp() {
    used_fds_ = GetNoUsedFds();
    sigpipe_save_ = signal(SIGPIPE, SIG_IGN);

    // Prepare cache directories
    tmp_path_ = CreateTempDir("./cvmfs_ut_quota_manager");
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

    CountPipeHelper count_pipe_helper;
    FileSystemTraversal<CountPipeHelper> travsl(&count_pipe_helper, "", false);
    travsl.fn_new_fifo = &CountPipeHelper::EncounterPipe;
    travsl.Recurse(tmp_path_);
    EXPECT_EQ(0U, count_pipe_helper.num_pipes());

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
  quota_mgr_->UnbindReturnPipe(fd);
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
  CreateFile(tmp_path_ + "/" + hash_null.MakePath(), 0600);
  CreateFile(tmp_path_ + "/" + hash_rnd.MakePath(), 0600);
  quota_mgr_->async_delete_ = false;

  quota_mgr_->Insert(hash_null, 1, "");
  quota_mgr_->Insert(hash_rnd, 1, "");
  EXPECT_EQ(0U, quota_mgr_->GetCleanupRate(60));
  EXPECT_TRUE(quota_mgr_->Cleanup(3));
  EXPECT_EQ(0U, quota_mgr_->GetCleanupRate(60));
  EXPECT_EQ(2U, quota_mgr_->GetSize());
  EXPECT_TRUE(quota_mgr_->Cleanup(2));
  EXPECT_EQ(0U, quota_mgr_->GetCleanupRate(60));
  EXPECT_EQ(2U, quota_mgr_->GetSize());
  EXPECT_TRUE(quota_mgr_->Cleanup(0));
  EXPECT_EQ(1U, quota_mgr_->GetCleanupRate(60));
  EXPECT_EQ(0U, quota_mgr_->GetSize());
  EXPECT_FALSE(FileExists(tmp_path_ + "/" + hash_null.MakePath()));
  EXPECT_FALSE(FileExists(tmp_path_ + "/" + hash_rnd.MakePath()));

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


TEST_F(T_QuotaManager, CleanupTouchPinnedOnExit) {
  EXPECT_TRUE(quota_mgr_->Pin(hashes_[0], 1, "pinned", false));
  quota_mgr_->Insert(hashes_[1], 1, "regular");
  delete quota_mgr_;
  quota_mgr_ = PosixQuotaManager::Create(tmp_path_, limit_, threshold_, false);
  quota_mgr_->Spawn();
  ASSERT_TRUE(quota_mgr_ != NULL);
  EXPECT_TRUE(quota_mgr_->Cleanup(1));
  EXPECT_EQ(1U, quota_mgr_->GetSize());
  EXPECT_EQ("pinned\n", PrintStringVector(quota_mgr_->List()));
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


TEST_F(T_QuotaManager, Contains) {
  shash::Any hash_null(shash::kSha1);
  shash::Any hash_rnd(shash::kSha1);
  hash_rnd.Randomize();
  shash::Any hash_rnd2(hash_rnd);
  hash_rnd2.algorithm = shash::kRmd160;
  quota_mgr_->Insert(hash_null, 1, "/a");
  EXPECT_TRUE(quota_mgr_->Pin(hash_rnd, 1, "/b", false));
  quota_mgr_->List();  // trigger database commit

  EXPECT_TRUE(quota_mgr_->Contains(hash_null.ToString()));
  EXPECT_TRUE(quota_mgr_->Contains(hash_rnd.ToString()));
  EXPECT_FALSE(quota_mgr_->Contains(hash_rnd2.ToString()));
}


TEST_F(T_QuotaManager, Create) {
  delete quota_mgr_;
  EXPECT_EQ(NULL, PosixQuotaManager::Create(tmp_path_, 5, 5, false));
  EXPECT_EQ(NULL, PosixQuotaManager::Create(tmp_path_ + "/noent", 5, 5, false));
  quota_mgr_ = PosixQuotaManager::Create(tmp_path_, 10, 5, false);
  EXPECT_TRUE(quota_mgr_ != NULL);
}


TEST_F(T_QuotaManager, CreateShared) {
  delete quota_mgr_;
  EXPECT_EQ(NULL,
    PosixQuotaManager::CreateShared("", tmp_path_ + "/noent", 5, 5));

  // Forking fails
  EXPECT_EQ(NULL, PosixQuotaManager::CreateShared("", tmp_path_, 5, 5));
  EXPECT_EQ(0, unlink((tmp_path_ + "/cachemgr").c_str()));

  // TODO(jblomer): test fork logic (requires changes to __cachemgr__ execve)

  quota_mgr_ = PosixQuotaManager::Create(tmp_path_, 10, 5, false);
  EXPECT_TRUE(quota_mgr_ != NULL);
}


TEST_F(T_QuotaManager, InsertList) {
  EXPECT_EQ("", PrintStringVector(quota_mgr_->List()));
  EXPECT_EQ("", PrintStringVector(quota_mgr_->ListCatalogs()));
  EXPECT_EQ("", PrintStringVector(quota_mgr_->ListPinned()));
  EXPECT_EQ("", PrintStringVector(quota_mgr_->ListVolatile()));

  quota_mgr_->Insert(hashes_[0], 0, "regular");
  quota_mgr_->InsertVolatile(hashes_[1], 0, "volatile");
  EXPECT_TRUE(quota_mgr_->Pin(hashes_[2], 0, "pinned", false));
  EXPECT_TRUE(quota_mgr_->Pin(hashes_[3], 1, "catalog", true));
  EXPECT_EQ(1U, quota_mgr_->GetSize());
  EXPECT_EQ("regular\nvolatile\npinned\n",
            PrintStringVector(quota_mgr_->List()));
  EXPECT_EQ("catalog\n", PrintStringVector(quota_mgr_->ListCatalogs()));
  EXPECT_EQ("pinned\ncatalog\n", PrintStringVector(quota_mgr_->ListPinned()));
  EXPECT_EQ("volatile\n", PrintStringVector(quota_mgr_->ListVolatile()));

  // Insert should be idempotent wrt. listing
  quota_mgr_->Insert(hashes_[0], 0, "regular");
  quota_mgr_->InsertVolatile(hashes_[1], 0, "volatile");
  EXPECT_TRUE(quota_mgr_->Pin(hashes_[2], 0, "pinned", false));
  EXPECT_TRUE(quota_mgr_->Pin(hashes_[3], 1, "catalog", true));
  EXPECT_EQ(1U, quota_mgr_->GetSize());
  EXPECT_EQ("regular\nvolatile\npinned\n",
            PrintStringVector(quota_mgr_->List()));
  EXPECT_EQ("catalog\n", PrintStringVector(quota_mgr_->ListCatalogs()));
  EXPECT_EQ("pinned\ncatalog\n", PrintStringVector(quota_mgr_->ListPinned()));
  EXPECT_EQ("volatile\n", PrintStringVector(quota_mgr_->ListVolatile()));
}


TEST_F(T_QuotaManager, Getters) {
  EXPECT_EQ(QuotaManager::kProtocolRevision, quota_mgr_->GetProtocolRevision());
  EXPECT_EQ(getpid(), quota_mgr_->GetPid());

  EXPECT_EQ(0U, quota_mgr_->GetSize());
  EXPECT_EQ(0U, quota_mgr_->GetSizePinned());

  quota_mgr_->Insert(hashes_[0], 0, "");
  quota_mgr_->Insert(hashes_[1], 1, "");
  EXPECT_TRUE(quota_mgr_->Pin(hashes_[2], 1, "", false));
  EXPECT_EQ(2U, quota_mgr_->GetSize());
  EXPECT_EQ(1U, quota_mgr_->GetSizePinned());
}


TEST_F(T_QuotaManager, InitDatabase) {
  PosixQuotaManager *mgr = new PosixQuotaManager(2, 1, tmp_path_ + "/noent");
  EXPECT_FALSE(mgr->InitDatabase(false));

  EXPECT_TRUE(MkdirDeep(tmp_path_ + "/noent", 0700));
  EXPECT_FALSE(mgr->InitDatabase(false));

  int fd;
  fd = open((tmp_path_ + "/noent/cachedb").c_str(), O_WRONLY | O_CREAT, 0600);
  EXPECT_GE(fd, 0);
  close(fd);
  EXPECT_FALSE(mgr->InitDatabase(false));

  delete quota_mgr_;
  quota_mgr_ = NULL;
  mgr->cache_dir_ = tmp_path_;
  EXPECT_TRUE(mgr->InitDatabase(false));
  mgr->CloseDatabase();
  EXPECT_TRUE(mgr->InitDatabase(true));
  mgr->CloseDatabase();

  delete mgr;
}


TEST_F(T_QuotaManager, MakeReturnPipe) {
  quota_mgr_->shared_ = true;
  int mypipe[2];
  int myotherpipe[2];
  quota_mgr_->MakeReturnPipe(mypipe);
  EXPECT_EQ(0, mypipe[1]);
  EXPECT_GE(mypipe[0], 0);
  quota_mgr_->MakeReturnPipe(myotherpipe);
  EXPECT_EQ(1, myotherpipe[1]);
  EXPECT_GE(myotherpipe[0], 0);
  quota_mgr_->CloseReturnPipe(mypipe);
  quota_mgr_->CloseReturnPipe(myotherpipe);
  quota_mgr_->shared_ = false;
}


TEST_F(T_QuotaManager, PinUnpin) {
  // Too big to pin
  EXPECT_FALSE(quota_mgr_not_spawned_->Pin(hashes_[0], 1000000000, "", false));
  EXPECT_TRUE(quota_mgr_not_spawned_->Pin(hashes_[0], threshold_, "x", false));
  // Pinning idempotent
  EXPECT_TRUE(quota_mgr_not_spawned_->Pin(hashes_[0], threshold_, "x", false));
  // Pinned until threshold
  EXPECT_FALSE(quota_mgr_not_spawned_->Pin(hashes_[1], 1, "", false));
  quota_mgr_not_spawned_->Spawn();
  // Normal files can still be added
  quota_mgr_not_spawned_->Insert(hashes_[1], threshold_, "y");
  EXPECT_EQ("x\ny\n", PrintStringVector(quota_mgr_not_spawned_->List()));
  EXPECT_EQ("x\n", PrintStringVector(quota_mgr_not_spawned_->ListPinned()));

  // Unpin on destruction
  delete quota_mgr_not_spawned_;
  quota_mgr_not_spawned_ =
    PosixQuotaManager::Create(tmp_path_ + "/not_spawned", limit_, threshold_,
                              false);
  ASSERT_TRUE(quota_mgr_not_spawned_ != NULL);
  EXPECT_TRUE(quota_mgr_not_spawned_->Pin(hashes_[2], 1, "z", false));
  quota_mgr_not_spawned_->Spawn();
  EXPECT_EQ("x\nz\n", PrintStringVector(quota_mgr_not_spawned_->List()));
  EXPECT_EQ("z\n", PrintStringVector(quota_mgr_not_spawned_->ListPinned()));

  // Unpin decreases pinned size (and doesn't remove files that still exists)
  CreateFile(tmp_path_ + "/not_spawned/" + hashes_[2].MakePathWithoutSuffix(),
             0600);
  EXPECT_FALSE(quota_mgr_not_spawned_->Pin(hashes_[0], threshold_, "x", false));
  quota_mgr_not_spawned_->Unpin(hashes_[2]);
  EXPECT_EQ("x\nz\n", PrintStringVector(quota_mgr_not_spawned_->List()));
  EXPECT_TRUE(quota_mgr_not_spawned_->Pin(hashes_[0], threshold_, "x", false));

  // Unpin removes a file that does not exist anymore
  uint64_t size_before_unpin = quota_mgr_not_spawned_->GetSize();
  string remove_me =
    tmp_path_ + "/not_spawned/" + hashes_[0].MakePathWithoutSuffix();
  quota_mgr_not_spawned_->Unpin(hashes_[0]);
  uint64_t size_after_unpin = quota_mgr_not_spawned_->GetSize();
  EXPECT_LT(size_after_unpin, size_before_unpin);
  EXPECT_EQ("z\n", PrintStringVector(quota_mgr_not_spawned_->List()));
}


TEST_F(T_QuotaManager, RebuildDatabase) {
  delete quota_mgr_;
  quota_mgr_ = NULL;
  EXPECT_TRUE(MkdirDeep(tmp_path_ + "/new", 0700));
  quota_mgr_ =
    PosixQuotaManager::Create(tmp_path_ + "/new", limit_, threshold_, true);
  EXPECT_EQ(NULL, quota_mgr_);

  quota_mgr_ =
    PosixQuotaManager::Create(tmp_path_, limit_, threshold_, true);
  ASSERT_TRUE(quota_mgr_ != NULL);
  quota_mgr_->Spawn();
  EXPECT_EQ(0U, quota_mgr_->GetSize());
  EXPECT_EQ("", PrintStringVector(quota_mgr_->List()));

  delete quota_mgr_;
  quota_mgr_ = NULL;
  CreateFile(tmp_path_ + "/" + hashes_[0].MakePath(), 0600);
  CreateFile(tmp_path_ + "/" + hashes_[1].MakePath(), 0600);
  unsigned char buf = 'x';
  EXPECT_TRUE(CopyMem2Path(&buf, 1, tmp_path_ + "/" + hashes_[1].MakePath()));
  quota_mgr_ =
    PosixQuotaManager::Create(tmp_path_, limit_, threshold_, true);
  ASSERT_TRUE(quota_mgr_ != NULL);
  quota_mgr_->Spawn();
  EXPECT_EQ(1U, quota_mgr_->GetSize());
  EXPECT_EQ("unknown (automatic rebuild)\nunknown (automatic rebuild)\n",
            PrintStringVector(quota_mgr_->List()));
}


TEST_F(T_QuotaManager, Remove) {
  quota_mgr_->Insert(hashes_[0], 1, "a");
  EXPECT_TRUE(quota_mgr_->Pin(hashes_[1], 1, "b", false));

  quota_mgr_->Remove(hashes_[2]);
  EXPECT_EQ(2U, quota_mgr_->GetSize());
  EXPECT_EQ("a\nb\n", PrintStringVector(quota_mgr_->List()));
  EXPECT_EQ("b\n", PrintStringVector(quota_mgr_->ListPinned()));

  quota_mgr_->Remove(hashes_[1]);
  EXPECT_EQ(1U, quota_mgr_->GetSize());
  EXPECT_EQ("a\n", PrintStringVector(quota_mgr_->List()));
  EXPECT_EQ("", PrintStringVector(quota_mgr_->ListPinned()));

  quota_mgr_->Remove(hashes_[0]);
  EXPECT_EQ(0U, quota_mgr_->GetSize());
  EXPECT_EQ("", PrintStringVector(quota_mgr_->List()));
  EXPECT_EQ("", PrintStringVector(quota_mgr_->ListPinned()));
}


TEST_F(T_QuotaManager, Spawn) {
  // Multiple attempts should be harmless
  quota_mgr_->Spawn();
  quota_mgr_not_spawned_->Spawn();
  quota_mgr_not_spawned_->Spawn();
}


TEST_F(T_QuotaManager, Touch) {
  quota_mgr_->Insert(hashes_[0], 1, "a");
  quota_mgr_->Insert(hashes_[1], 1, "b");
  quota_mgr_->Touch(hashes_[0]);
  quota_mgr_->Cleanup(1);
  EXPECT_EQ("a\n", PrintStringVector(quota_mgr_->List()));
}
