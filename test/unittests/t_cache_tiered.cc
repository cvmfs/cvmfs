/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "cache.h"
#include "cache_ram.h"
#include "cache_tiered.h"
#include "crypto/hash.h"
#include "statistics.h"

using namespace std;  // NOLINT

class T_TieredCacheManager : public ::testing::Test {
 protected:
  virtual void SetUp() {
    upper_cache_ = new RamCacheManager(
        1024, 128, MemoryKvStore::kMallocLibc,
        perf::StatisticsTemplate("test", &stats_upper_));
    lower_cache_ = new RamCacheManager(
        1024, 128, MemoryKvStore::kMallocLibc,
        perf::StatisticsTemplate("test", &stats_lower_));
    tiered_cache_ = TieredCacheManager::Create(upper_cache_, lower_cache_);
    EXPECT_FALSE(tiered_cache_->LoadBreadcrumb("test").IsValid());
    buf_ = 'x';
    hash_one_.digest[1] = 1;
  }

  virtual void TearDown() { delete tiered_cache_; }

  perf::Statistics stats_upper_;
  perf::Statistics stats_lower_;
  CacheManager *tiered_cache_;
  RamCacheManager *upper_cache_;
  RamCacheManager *lower_cache_;
  unsigned char buf_;
  shash::Any hash_one_;
};


TEST_F(T_TieredCacheManager, OpenUpper) {
  EXPECT_EQ(-ENOENT,
            tiered_cache_->Open(CacheManager::LabeledObject(hash_one_)));

  EXPECT_TRUE(upper_cache_->CommitFromMem(
      CacheManager::LabeledObject(hash_one_), &buf_, 1));
  int fd = tiered_cache_->Open(CacheManager::LabeledObject(hash_one_));
  EXPECT_GE(fd, 0);

  EXPECT_EQ(1, tiered_cache_->GetSize(fd));
  unsigned char buf;
  EXPECT_EQ(1, tiered_cache_->Pread(fd, &buf, 1, 0));
  EXPECT_EQ(buf_, buf);

  EXPECT_EQ(0, tiered_cache_->Close(fd));
}


TEST_F(T_TieredCacheManager, CopyUp) {
  EXPECT_EQ(-ENOENT,
            tiered_cache_->Open(CacheManager::LabeledObject(hash_one_)));

  EXPECT_TRUE(lower_cache_->CommitFromMem(
      CacheManager::LabeledObject(hash_one_), &buf_, 1));
  CacheManager::Label label;
  label.flags = CacheManager::kLabelVolatile;
  int fd = tiered_cache_->Open(CacheManager::LabeledObject(hash_one_, label));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(1, stats_upper_.Lookup("test.n_openvolatile")->Get());

  int fd_upper = upper_cache_->Open(CacheManager::LabeledObject(hash_one_));
  EXPECT_GE(fd_upper, 0);
  EXPECT_EQ(0, upper_cache_->Close(fd_upper));

  EXPECT_EQ(1, tiered_cache_->GetSize(fd));
  unsigned char buf;
  EXPECT_EQ(1, tiered_cache_->Pread(fd, &buf, 1, 0));
  EXPECT_EQ(buf_, buf);

  EXPECT_EQ(0, tiered_cache_->Close(fd));
}


TEST_F(T_TieredCacheManager, Transaction) {
  EXPECT_EQ(-ENOENT,
            tiered_cache_->Open(CacheManager::LabeledObject(hash_one_)));
  EXPECT_TRUE(tiered_cache_->CommitFromMem(
      CacheManager::LabeledObject(hash_one_), &buf_, 1));

  int fd_upper = upper_cache_->Open(CacheManager::LabeledObject(hash_one_));
  EXPECT_GE(fd_upper, 0);
  EXPECT_EQ(0, upper_cache_->Close(fd_upper));
  int fd_lower = lower_cache_->Open(CacheManager::LabeledObject(hash_one_));
  EXPECT_GE(fd_lower, 0);
  EXPECT_EQ(0, lower_cache_->Close(fd_lower));
}


TEST_F(T_TieredCacheManager, ReadOnly) {
  reinterpret_cast<TieredCacheManager *>(tiered_cache_)->SetLowerReadOnly();
  EXPECT_EQ(-ENOENT,
            tiered_cache_->Open(CacheManager::LabeledObject(hash_one_)));
  EXPECT_TRUE(tiered_cache_->CommitFromMem(
      CacheManager::LabeledObject(hash_one_), &buf_, 1));

  int fd_upper = upper_cache_->Open(CacheManager::LabeledObject(hash_one_));
  EXPECT_GE(fd_upper, 0);
  EXPECT_EQ(0, upper_cache_->Close(fd_upper));
  EXPECT_EQ(-ENOENT,
            lower_cache_->Open(CacheManager::LabeledObject(hash_one_)));

  void *txn = alloca(tiered_cache_->SizeOfTxn());
  EXPECT_EQ(0, tiered_cache_->StartTxn(hash_one_, 1, txn));
  EXPECT_EQ(0, tiered_cache_->Reset(txn));
  EXPECT_EQ(0, tiered_cache_->AbortTxn(txn));
}
