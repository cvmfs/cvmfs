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
      new RamCacheManager(1024, 128, MemoryKvStore::kMallocLibc, &stats_upper_);
    lower_cache_ =
      new RamCacheManager(1024, 128, MemoryKvStore::kMallocLibc, &stats_lower_);
    tiered_cache_ = TieredCacheManager::Create(upper_cache_, lower_cache_);
  }

  virtual void TearDown() {
    delete tiered_cache_;
  }

  perf::Statistics stats_upper_;
  perf::Statistics stats_lower_;
  CacheManager *tiered_cache_;
  RamCacheManager *upper_cache_;
  RamCacheManager *lower_cache_;
};


TEST_F(T_TieredCacheManager, Open) {
  EXPECT_EQ(-ENOENT, tiered_cache_->Open(CacheManager::Bless(shash::Any())));
}
