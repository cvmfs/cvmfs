/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "cache.h"
#include "cache_ram.h"
#include "cache_tiered.h"

using namespace std;  // NOLINT

class T_TieredCacheManager : public ::testing::Test {
 protected:
  virtual void SetUp() {
    tiered_cache_ = TieredCacheManager::Create(NULL, NULL);
  }

  virtual void TearDown() {
    delete tiered_cache_;
  }

  CacheManager *tiered_cache_;
};


TEST_F(T_TieredCacheManager, Basics) {
}
