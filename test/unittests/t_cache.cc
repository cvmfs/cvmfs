/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <string>

#include "../../cvmfs/cache.h"
#include "../../cvmfs/hash.h"

using namespace std;  // NOLINT

class T_Cache : public ::testing::Test {
 protected:
  virtual void SetUp() {
    used_fds = GetNoUsedFds();

    tmp_path_ = CreateTempDir("/tmp/cvmfs_test", 0700);
    cache_mgr_ = cache::PosixCacheManager::Create(tmp_path_, false);
    ASSERT_TRUE(cache_mgr_ != NULL);

    ASSERT_TRUE(cache_mgr_->CommitFromMem(hash_null, NULL, 0));
    unsigned char buf = 'A';
    hash_one.digest[0] = 1;
    ASSERT_TRUE(cache_mgr_->CommitFromMem(hash_one, &buf, 1));
  }

  virtual void TearDown() {
    delete cache_mgr_;
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
    EXPECT_EQ(used_fds, GetNoUsedFds());
  }

  unsigned GetNoUsedFds() {
    unsigned result = 0;
    int max_fd = getdtablesize();
    assert(max_fd >= 0);
    for (unsigned fd = 0; fd < unsigned(max_fd); ++fd) {
      int retval = fcntl(fd, F_GETFD, 0);
      if (retval != -1)
        result++;
    }
    return result;
  }

 protected:
  cache::PosixCacheManager *cache_mgr_;
  string tmp_path_;
  shash::Any hash_null;
  shash::Any hash_one;
  unsigned used_fds;
};


class FaultyCacheManager : public cache::CacheManager {
  virtual int Open(const shash::Any &id) { return 0; }
  virtual int64_t GetSize(int fd) { return 1; }
  virtual int Close(int fd) { return 0; }
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset) {
    return -EIO;
  }
  virtual uint16_t SizeOfTxn() { return 1; }
  virtual int StartTxn(const shash::Any &id, void *txn) { return 0; }
  virtual int64_t Write(const void *buf, uint64_t sz, void *txn) {
    return -EIO;
  }
  virtual int Reset(void *txn) { return 0; }
  virtual int AbortTxn(void *txn, const std::string &dump_path = "") {
    return 0;
  }
  virtual int OpenFromTxn(void *txn) { return 0; }
  virtual int CommitTxn(void *txn) { return 0; }
};


TEST_F(T_Cache, Open2Mem) {
  unsigned char *retrieve_buf;
  uint64_t retrieve_size;

  EXPECT_FALSE(cache_mgr_->Open2Mem(shash::Any(shash::kMd5),
    &retrieve_buf, &retrieve_size));

  EXPECT_TRUE(cache_mgr_->Open2Mem(hash_null, &retrieve_buf, &retrieve_size));
  EXPECT_EQ(0U, retrieve_size);
  EXPECT_EQ(NULL, retrieve_buf);

  EXPECT_TRUE(cache_mgr_->Open2Mem(hash_one, &retrieve_buf, &retrieve_size));
  EXPECT_EQ(1U, retrieve_size);
  EXPECT_EQ('A', retrieve_buf[0]);

  FaultyCacheManager faulty_cache;
  EXPECT_FALSE(faulty_cache.Open2Mem(hash_one, &retrieve_buf, &retrieve_size));
  EXPECT_EQ(0U, retrieve_size);
  EXPECT_EQ(NULL, retrieve_buf);
}
