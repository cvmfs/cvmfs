/**
 * This file is part of the CernVM File System.
 */

#include <alloca.h>
#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <gtest/gtest.h>

#include "cache.h"
#include "cache_ram.h"
#include "hash.h"
#include "statistics.h"

using namespace std;  // NOLINT

static const unsigned cache_size = 1024;
static const unsigned alloc_size = 16;

namespace cache {

class T_RamCacheManager : public ::testing::Test {
 public:
  T_RamCacheManager()
    : ramcache_(4*alloc_size, 3*cache_size, &statistics_) {
    a_.digest[1] = 1;
  }

 protected:
  virtual void SetUp() {}
  virtual void TearDown() {}

  perf::Statistics statistics_;
  shash::Any a_;
  RamCacheManager ramcache_;
};

TEST_F(T_RamCacheManager, TransactionCommit) {
  char buf[alloc_size];
  memset(buf, 42, alloc_size);
  void *txn = alloca(ramcache_.SizeOfTxn());
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn));
}


TEST_F(T_RamCacheManager, TransactionReset) {
  char buf[alloc_size];
  memset(buf, 42, alloc_size);
  void *txn = alloca(ramcache_.SizeOfTxn());
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn));

  EXPECT_EQ(4, ramcache_.Write(buf, 4, txn));
  EXPECT_EQ(0, ramcache_.Reset(txn));

  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn));
}

TEST_F(T_RamCacheManager, TransactionWrite) {
  char buf[alloc_size];
  memset(buf, 42, alloc_size/2);
  memset(buf + alloc_size/2, 24, alloc_size/2);
  void *txn = alloca(ramcache_.SizeOfTxn());
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn));
  EXPECT_EQ(alloc_size/2, ramcache_.Write(buf, alloc_size/2, txn));
  EXPECT_EQ(-ENOSPC, ramcache_.Write(buf + alloc_size/2, alloc_size, txn));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn));
}


TEST_F(T_RamCacheManager, Read) {
  int fd;
  char buf[alloc_size];
  memset(buf, 42, alloc_size);
  void *txn = alloca(ramcache_.SizeOfTxn());
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn));

  char out[alloc_size];
  memset(out, 0, alloc_size);
  EXPECT_GE((fd = ramcache_.Open(a_)), 0);
  EXPECT_EQ(alloc_size, ramcache_.Pread(fd, out, alloc_size, 0));
  EXPECT_EQ(0, memcmp(buf, out, alloc_size));

  EXPECT_EQ(0, ramcache_.Close(fd));
}

TEST_F(T_RamCacheManager, OpenFromTxn) {
  int fd;
  char buf[alloc_size];
  memset(buf, 42, alloc_size);
  void *txn = alloca(ramcache_.SizeOfTxn());
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn));

  EXPECT_GE((fd = ramcache_.OpenFromTxn(txn)), 0);
  EXPECT_EQ(alloc_size, ramcache_.GetSize(fd));

  EXPECT_EQ(0, ramcache_.Close(fd));
}

TEST_F(T_RamCacheManager, Dup) {
  int fd = 0;
  int dupfd;
  char buf[alloc_size];
  memset(buf, 42, alloc_size);
  void *txn = alloca(ramcache_.SizeOfTxn());
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn));

  EXPECT_EQ(-EBADFD, ramcache_.Dup(fd));

  EXPECT_GE((fd = ramcache_.OpenFromTxn(txn)), 0);
  EXPECT_EQ(alloc_size, ramcache_.GetSize(fd));
  EXPECT_GE((dupfd = ramcache_.Dup(fd)), 0);
  EXPECT_EQ(0, ramcache_.Close(fd));
  EXPECT_EQ(alloc_size, ramcache_.GetSize(dupfd));
  EXPECT_EQ(0, ramcache_.Close(dupfd));
}

TEST_F(T_RamCacheManager, Eviction) {
  char buf[alloc_size];
  memset(buf, 42, alloc_size);

  void *txn1 = alloca(ramcache_.SizeOfTxn());
  void *txn2 = alloca(ramcache_.SizeOfTxn());
  void *txn3 = alloca(ramcache_.SizeOfTxn());
  void *txn4 = alloca(ramcache_.SizeOfTxn());
  void *txn5 = alloca(ramcache_.SizeOfTxn());

  a_.digest[1] = 1;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn1));
  a_.digest[1] = 2;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn2));
  a_.digest[1] = 3;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn3));
  a_.digest[1] = 4;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn4));
  a_.digest[1] = 5;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn5));

  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn1));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn2));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn3));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn4));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn5));

  EXPECT_EQ(0, ramcache_.CommitTxn(txn1));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn2));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn3));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn4));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn5));

  a_.digest[1] = 1;
  EXPECT_EQ(-ENOENT, ramcache_.Open(a_));

  EXPECT_EQ(0, ramcache_.AbortTxn(txn5));
}

TEST_F(T_RamCacheManager, OpenEntries) {
  char buf[alloc_size];
  memset(buf, 42, alloc_size);

  void *txn1 = alloca(ramcache_.SizeOfTxn());
  void *txn2 = alloca(ramcache_.SizeOfTxn());
  void *txn3 = alloca(ramcache_.SizeOfTxn());
  void *txn4 = alloca(ramcache_.SizeOfTxn());
  void *txn5 = alloca(ramcache_.SizeOfTxn());

  a_.digest[1] = 1;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn1));
  a_.digest[1] = 2;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn2));
  a_.digest[1] = 3;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn3));
  a_.digest[1] = 4;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn4));
  a_.digest[1] = 5;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn5));

  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn1));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn2));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn3));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn4));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn5));

  EXPECT_GE(ramcache_.OpenFromTxn(txn1), 0);
  EXPECT_GE(ramcache_.OpenFromTxn(txn2), 0);
  EXPECT_GE(ramcache_.OpenFromTxn(txn3), 0);
  EXPECT_GE(ramcache_.OpenFromTxn(txn4), 0);
  EXPECT_LT(ramcache_.OpenFromTxn(txn5), 0);

  EXPECT_EQ(0, ramcache_.AbortTxn(txn5));
}

TEST_F(T_RamCacheManager, PinnedEntry) {
  char buf[alloc_size];
  memset(buf, 42, alloc_size);

  void *txn1 = alloca(ramcache_.SizeOfTxn());
  void *txn2 = alloca(ramcache_.SizeOfTxn());
  void *txn3 = alloca(ramcache_.SizeOfTxn());
  void *txn4 = alloca(ramcache_.SizeOfTxn());
  void *txn5 = alloca(ramcache_.SizeOfTxn());

  a_.digest[1] = 1;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn1));
  a_.digest[1] = 2;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn2));
  a_.digest[1] = 3;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn3));
  a_.digest[1] = 4;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn4));
  a_.digest[1] = 5;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn5));

  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn1));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn2));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn3));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn4));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn5));

  ramcache_.CtrlTxn("", cache::CacheManager::kTypePinned, 0, txn1);

  EXPECT_EQ(0, ramcache_.CommitTxn(txn1));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn2));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn3));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn4));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn5));

  a_.digest[1] = 1;
  EXPECT_GE(ramcache_.Open(a_), 0);
  a_.digest[1] = 2;
  EXPECT_EQ(-ENOENT, ramcache_.Open(a_));
}

TEST_F(T_RamCacheManager, VolatileEntry) {
  char buf[alloc_size];
  memset(buf, 42, alloc_size);

  void *txn1 = alloca(ramcache_.SizeOfTxn());
  void *txn2 = alloca(ramcache_.SizeOfTxn());
  void *txn3 = alloca(ramcache_.SizeOfTxn());
  void *txn4 = alloca(ramcache_.SizeOfTxn());
  void *txn5 = alloca(ramcache_.SizeOfTxn());

  a_.digest[1] = 1;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn1));
  a_.digest[1] = 2;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn2));
  a_.digest[1] = 3;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn3));
  a_.digest[1] = 4;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn4));
  a_.digest[1] = 5;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn5));

  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn1));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn2));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn3));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn4));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn5));

  ramcache_.CtrlTxn("", cache::CacheManager::kTypeVolatile, 0, txn4);

  EXPECT_EQ(0, ramcache_.CommitTxn(txn1));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn2));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn3));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn4));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn5));

  a_.digest[1] = 1;
  EXPECT_GE(ramcache_.Open(a_), 0);
  a_.digest[1] = 4;
  EXPECT_EQ(-ENOENT, ramcache_.Open(a_));
}

TEST_F(T_RamCacheManager, LargeCommit) {
  char buf[alloc_size];
  memset(buf, 42, alloc_size);

  void *txn1 = alloca(ramcache_.SizeOfTxn());
  void *txn2 = alloca(ramcache_.SizeOfTxn());
  void *txn3 = alloca(ramcache_.SizeOfTxn());

  a_.digest[1] = 1;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn1));
  a_.digest[1] = 2;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn2));
  a_.digest[1] = 3;
  EXPECT_EQ(0, ramcache_.StartTxn(a_, 4*alloc_size, txn3));

  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn1));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn2));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn3));

  EXPECT_EQ(0, ramcache_.CommitTxn(txn1));
  int fd = ramcache_.OpenFromTxn(txn2);
  EXPECT_GE(fd, 0);
  EXPECT_EQ(-ENOSPC, ramcache_.CommitTxn(txn3));
  a_.digest[1] = 1;
  EXPECT_EQ(-ENOENT, ramcache_.Open(a_));
  a_.digest[1] = 2;
  EXPECT_EQ(0, ramcache_.Close(fd));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn3));
}

}  // namespace cache
