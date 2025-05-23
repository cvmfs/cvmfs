/**
 * This file is part of the CernVM File System.
 */

#include <alloca.h>
#include <errno.h>
#include <gtest/gtest.h>
#include <stdint.h>
#include <string.h>

#include "cache.h"
#include "cache_ram.h"
#include "crypto/hash.h"
#include "statistics.h"
#include "util/prng.h"

using namespace std;  // NOLINT

static const unsigned cache_size = 1024;
static const unsigned alloc_size = 16;

class T_RamCacheManager : public ::testing::Test {
 public:
  T_RamCacheManager()
      : ramcache_(4 * alloc_size,
                  cache_size,
                  MemoryKvStore::kMallocLibc,
                  perf::StatisticsTemplate("test", &statistics_)) {
    a_.digest[1] = 1;
  }

 protected:
  virtual void SetUp() { }
  virtual void TearDown() { }

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
  memset(buf, 42, alloc_size / 2);
  memset(buf + alloc_size / 2, 24, alloc_size / 2);
  void *txn = alloca(ramcache_.SizeOfTxn());
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn));
  EXPECT_EQ(alloc_size / 2, ramcache_.Write(buf, alloc_size / 2, txn));
  EXPECT_EQ(-EFBIG, ramcache_.Write(buf + alloc_size / 2, alloc_size, txn));
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
  EXPECT_GE((fd = ramcache_.Open(CacheManager::LabeledObject(a_))), 0);
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

  EXPECT_EQ(-EBADF, ramcache_.Dup(fd));

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
  EXPECT_EQ(-ENOENT, ramcache_.Open(CacheManager::LabeledObject(a_)));
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

  CacheManager::Label label;
  label.flags = CacheManager::kLabelPinned;
  ramcache_.CtrlTxn(label, 0, txn1);

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
  EXPECT_GE(ramcache_.Open(CacheManager::LabeledObject(a_)), 0);
  a_.digest[1] = 2;
  EXPECT_EQ(-ENOENT, ramcache_.Open(CacheManager::LabeledObject(a_)));
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

  CacheManager::Label label;
  label.flags = CacheManager::kLabelVolatile;
  ramcache_.CtrlTxn(label, 0, txn4);

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
  EXPECT_GE(ramcache_.Open(CacheManager::LabeledObject(a_)), 0);
  a_.digest[1] = 4;
  EXPECT_EQ(-ENOENT, ramcache_.Open(CacheManager::LabeledObject(a_)));
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
  EXPECT_EQ(0, ramcache_.StartTxn(a_, 4 * alloc_size, txn3));

  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn1));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn2));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn3));

  EXPECT_EQ(0, ramcache_.CommitTxn(txn1));
  int fd = ramcache_.OpenFromTxn(txn2);
  EXPECT_GE(fd, 0);
  EXPECT_EQ(-ENOSPC, ramcache_.CommitTxn(txn3));
  a_.digest[1] = 1;
  EXPECT_EQ(-ENOENT, ramcache_.Open(CacheManager::LabeledObject(a_)));
  a_.digest[1] = 2;
  EXPECT_EQ(0, ramcache_.Close(fd));
  EXPECT_EQ(0, ramcache_.CommitTxn(txn3));
}


TEST_F(T_RamCacheManager, OpenClose) {
  Prng prng;
  prng.InitLocaltime();
  int fds[cache_size];
  char buf[alloc_size];
  memset(buf, 42, alloc_size);
  void *txn = alloca(ramcache_.SizeOfTxn());
  EXPECT_EQ(0, ramcache_.StartTxn(a_, alloc_size, txn));
  EXPECT_EQ(alloc_size, ramcache_.Write(buf, alloc_size, txn));
  EXPECT_GE((fds[0] = ramcache_.OpenFromTxn(txn)), 0);
  EXPECT_EQ(0, ramcache_.CommitTxn(txn));

  for (size_t i = 1; i < cache_size; ++i) {
    EXPECT_GE((fds[i] = ramcache_.Dup(fds[0])), 0);
  }
  EXPECT_EQ(-ENFILE, ramcache_.Dup(fds[0]));

  for (size_t i = 0; i < 10000; ++i) {
    size_t j = prng.Next(cache_size);
    EXPECT_EQ(0, ramcache_.Close(fds[j]));
    size_t adj = j > 0 ? j - 1 : cache_size - 1;
    EXPECT_GE((fds[j] = ramcache_.Dup(adj)), 0);
  }

  for (size_t i = 0; i < cache_size; ++i) {
    EXPECT_EQ(0, ramcache_.Close(fds[i]));
  }
}
