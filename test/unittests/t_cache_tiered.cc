/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "cache.h"
#include "cache_ram.h"
#include "cache_tiered.h"
#include "hash.h"
#include "statistics.h"

using namespace std;  // NOLINT

class T_TieredCacheManager : public ::testing::Test {
 protected:
  virtual void SetUp() {
    upper_cache_ =
      new RamCacheManager(1024, 128, MemoryKvStore::kMallocLibc,
                          perf::StatisticsTemplate("test", &stats_upper_));
    lower_cache_ =
      new RamCacheManager(1024, 128, MemoryKvStore::kMallocLibc,
                          perf::StatisticsTemplate("test", &stats_lower_));
    tiered_cache_ = TieredCacheManager::Create(upper_cache_, lower_cache_);
    buf_ = 'x';
    hash_one_.digest[1] = 1;
  }

  virtual void TearDown() {
    delete tiered_cache_;
  }

  perf::Statistics stats_upper_;
  perf::Statistics stats_lower_;
  CacheManager *tiered_cache_;
  RamCacheManager *upper_cache_;
  RamCacheManager *lower_cache_;
  unsigned char buf_;
  shash::Any hash_one_;
};


TEST_F(T_TieredCacheManager, OpenUpper) {
  EXPECT_EQ(-ENOENT, tiered_cache_->Open(CacheManager::Bless(hash_one_)));

  EXPECT_TRUE(upper_cache_->CommitFromMem(hash_one_, &buf_, 1, "one"));
  int fd = tiered_cache_->Open(CacheManager::Bless(hash_one_));
  EXPECT_GE(fd, 0);

  EXPECT_EQ(1, tiered_cache_->GetSize(fd));
  unsigned char buf;
  EXPECT_EQ(1, tiered_cache_->Pread(fd, &buf, 1, 0));
  EXPECT_EQ(buf_, buf);

  EXPECT_EQ(0, tiered_cache_->Close(fd));
}


TEST_F(T_TieredCacheManager, CopyUp) {
  EXPECT_EQ(-ENOENT, tiered_cache_->Open(CacheManager::Bless(hash_one_)));

  EXPECT_TRUE(lower_cache_->CommitFromMem(hash_one_, &buf_, 1, "one"));
  int fd = tiered_cache_->Open(CacheManager::Bless(
    hash_one_, CacheManager::kTypeVolatile));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(1, stats_upper_.Lookup("test.n_openvolatile")->Get());

  int fd_upper = upper_cache_->Open(CacheManager::Bless(hash_one_));
  EXPECT_GE(fd_upper, 0);
  EXPECT_EQ(0, upper_cache_->Close(fd_upper));

  EXPECT_EQ(1, tiered_cache_->GetSize(fd));
  unsigned char buf;
  EXPECT_EQ(1, tiered_cache_->Pread(fd, &buf, 1, 0));
  EXPECT_EQ(buf_, buf);

  EXPECT_EQ(0, tiered_cache_->Close(fd));
}


TEST_F(T_TieredCacheManager, Transaction) {
  EXPECT_EQ(-ENOENT, tiered_cache_->Open(CacheManager::Bless(hash_one_)));
  EXPECT_TRUE(tiered_cache_->CommitFromMem(hash_one_, &buf_, 1, "one"));

  int fd_upper = upper_cache_->Open(CacheManager::Bless(hash_one_));
  EXPECT_GE(fd_upper, 0);
  EXPECT_EQ(0, upper_cache_->Close(fd_upper));
  int fd_lower = lower_cache_->Open(CacheManager::Bless(hash_one_));
  EXPECT_GE(fd_lower, 0);
  EXPECT_EQ(0, lower_cache_->Close(fd_lower));
}
