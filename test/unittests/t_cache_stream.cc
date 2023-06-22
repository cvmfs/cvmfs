/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cache_stream.h>
#include <util/pointer.h>

class T_StreamingCacheManager : public ::testing::Test {
 protected:
  virtual void SetUp() {}
  virtual void TearDown() {}

  UniquePtr<StreamingCacheManager> cache_mgr_;
};

TEST_F(T_StreamingCacheManager, Basics) {

}
