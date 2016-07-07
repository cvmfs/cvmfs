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
using cache::RamCacheManager;

static const unsigned cache_size = 1024;
static const unsigned alloc_size = 16;


TEST(T_RamCacheManager, TransactionCommit) {
  perf::Statistics statistics;
  shash::Any a;
  a.digest[1] = 1;
  RamCacheManager ramcache(4*alloc_size, 3*cache_size, &statistics);

  char buf[alloc_size];
  memset(buf, 42, alloc_size);
  void *txn = alloca(ramcache.SizeOfTxn());
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn));
  EXPECT_EQ(0, ramcache.CommitTxn(txn));
}


TEST(T_RamCacheManager, TransactionReset) {
  perf::Statistics statistics;
  shash::Any a;
  a.digest[1] = 1;
  RamCacheManager ramcache(4*alloc_size, 3*cache_size, &statistics);

  char buf[alloc_size];
  memset(buf, 42, alloc_size);
  void *txn = alloca(ramcache.SizeOfTxn());
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn));

  EXPECT_EQ(4, ramcache.Write(buf, 4, txn));
  EXPECT_EQ(0, ramcache.Reset(txn));

  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn));
  EXPECT_EQ(0, ramcache.CommitTxn(txn));
}

TEST(T_RamCacheManager, TransactionWrite) {
  perf::Statistics statistics;
  shash::Any a;
  a.digest[1] = 1;
  RamCacheManager ramcache(4*alloc_size, 3*cache_size, &statistics);

  char buf[alloc_size];
  memset(buf, 42, alloc_size/2);
  memset(buf + alloc_size/2, 24, alloc_size/2);
  void *txn = alloca(ramcache.SizeOfTxn());
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn));
  EXPECT_EQ(alloc_size/2, ramcache.Write(buf, alloc_size/2, txn));
  EXPECT_EQ(-ENOSPC, ramcache.Write(buf + alloc_size/2, alloc_size, txn));
  EXPECT_EQ(0, ramcache.CommitTxn(txn));
}


TEST(T_RamCacheManager, Read) {
  int fd;
  perf::Statistics statistics;
  shash::Any a;
  a.digest[1] = 1;
  RamCacheManager ramcache(4*alloc_size, 3*cache_size, &statistics);

  char buf[alloc_size];
  memset(buf, 42, alloc_size);
  void *txn = alloca(ramcache.SizeOfTxn());
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn));
  EXPECT_EQ(0, ramcache.CommitTxn(txn));

  char out[alloc_size];
  memset(out, 0, alloc_size);
  EXPECT_GE((fd = ramcache.Open(a)), 0);
  EXPECT_EQ(alloc_size, ramcache.Pread(fd, out, alloc_size, 0));
  EXPECT_EQ(0, memcmp(buf, out, alloc_size));

  EXPECT_EQ(0, ramcache.Close(fd));
}


TEST(T_RamCacheManager, OpenFromTxn) {
  int fd;
  perf::Statistics statistics;
  shash::Any a;
  a.digest[1] = 1;
  RamCacheManager ramcache(4*alloc_size, 3*cache_size, &statistics);

  char buf[alloc_size];
  memset(buf, 42, alloc_size);
  void *txn = alloca(ramcache.SizeOfTxn());
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn));

  EXPECT_GE((fd = ramcache.OpenFromTxn(txn)), 0);
  EXPECT_EQ(alloc_size, ramcache.GetSize(fd));

  EXPECT_EQ(0, ramcache.Close(fd));
}


TEST(T_RamCacheManager, Dup) {
  int fd = 0;
  int dupfd;
  perf::Statistics statistics;
  shash::Any a;
  a.digest[1] = 1;
  RamCacheManager ramcache(4*alloc_size, 3*cache_size, &statistics);

  char buf[alloc_size];
  memset(buf, 42, alloc_size);
  void *txn = alloca(ramcache.SizeOfTxn());
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn));

  EXPECT_EQ(-EBADFD, ramcache.Dup(fd));

  EXPECT_GE((fd = ramcache.OpenFromTxn(txn)), 0);
  EXPECT_EQ(alloc_size, ramcache.GetSize(fd));
  EXPECT_GE((dupfd = ramcache.Dup(fd)), 0);
  EXPECT_EQ(0, ramcache.Close(fd));
  EXPECT_EQ(alloc_size, ramcache.GetSize(dupfd));
  EXPECT_EQ(0, ramcache.Close(dupfd));
}


TEST(T_RamCacheManager, Eviction) {
  perf::Statistics statistics;
  shash::Any a;
  RamCacheManager ramcache(4*alloc_size, 3*cache_size, &statistics);

  char buf[alloc_size];
  memset(buf, 42, alloc_size);

  void *txn1 = alloca(ramcache.SizeOfTxn());
  void *txn2 = alloca(ramcache.SizeOfTxn());
  void *txn3 = alloca(ramcache.SizeOfTxn());
  void *txn4 = alloca(ramcache.SizeOfTxn());
  void *txn5 = alloca(ramcache.SizeOfTxn());

  a.digest[1] = 1;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn1));
  a.digest[1] = 2;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn2));
  a.digest[1] = 3;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn3));
  a.digest[1] = 4;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn4));
  a.digest[1] = 5;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn5));

  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn1));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn2));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn3));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn4));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn5));

  EXPECT_EQ(0, ramcache.CommitTxn(txn1));
  EXPECT_EQ(0, ramcache.CommitTxn(txn2));
  EXPECT_EQ(0, ramcache.CommitTxn(txn3));
  EXPECT_EQ(0, ramcache.CommitTxn(txn4));
  EXPECT_EQ(0, ramcache.CommitTxn(txn5));

  a.digest[1] = 1;
  EXPECT_EQ(-ENOENT, ramcache.Open(a));

  EXPECT_EQ(0, ramcache.AbortTxn(txn5));
}


TEST(T_RamCacheManager, OpenEntries) {
  perf::Statistics statistics;
  shash::Any a;
  RamCacheManager ramcache(4*alloc_size, 3*cache_size, &statistics);

  char buf[alloc_size];
  memset(buf, 42, alloc_size);

  void *txn1 = alloca(ramcache.SizeOfTxn());
  void *txn2 = alloca(ramcache.SizeOfTxn());
  void *txn3 = alloca(ramcache.SizeOfTxn());
  void *txn4 = alloca(ramcache.SizeOfTxn());
  void *txn5 = alloca(ramcache.SizeOfTxn());

  a.digest[1] = 1;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn1));
  a.digest[1] = 2;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn2));
  a.digest[1] = 3;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn3));
  a.digest[1] = 4;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn4));
  a.digest[1] = 5;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn5));

  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn1));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn2));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn3));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn4));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn5));

  EXPECT_GE(ramcache.OpenFromTxn(txn1), 0);
  EXPECT_GE(ramcache.OpenFromTxn(txn2), 0);
  EXPECT_GE(ramcache.OpenFromTxn(txn3), 0);
  EXPECT_GE(ramcache.OpenFromTxn(txn4), 0);
  EXPECT_LT(ramcache.OpenFromTxn(txn5), 0);

  EXPECT_EQ(0, ramcache.AbortTxn(txn5));
}


TEST(T_RamCacheManager, PinnedEntry) {
  perf::Statistics statistics;
  shash::Any a;
  RamCacheManager ramcache(4*alloc_size, 3*cache_size, &statistics);

  char buf[alloc_size];
  memset(buf, 42, alloc_size);

  void *txn1 = alloca(ramcache.SizeOfTxn());
  void *txn2 = alloca(ramcache.SizeOfTxn());
  void *txn3 = alloca(ramcache.SizeOfTxn());
  void *txn4 = alloca(ramcache.SizeOfTxn());
  void *txn5 = alloca(ramcache.SizeOfTxn());

  a.digest[1] = 1;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn1));
  a.digest[1] = 2;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn2));
  a.digest[1] = 3;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn3));
  a.digest[1] = 4;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn4));
  a.digest[1] = 5;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn5));

  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn1));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn2));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn3));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn4));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn5));

  ramcache.CtrlTxn("", cache::CacheManager::kTypePinned, 0, txn1);

  EXPECT_EQ(0, ramcache.CommitTxn(txn1));
  EXPECT_EQ(0, ramcache.CommitTxn(txn2));
  EXPECT_EQ(0, ramcache.CommitTxn(txn3));
  EXPECT_EQ(0, ramcache.CommitTxn(txn4));
  EXPECT_EQ(0, ramcache.CommitTxn(txn5));

  a.digest[1] = 1;
  EXPECT_GE(ramcache.Open(a), 0);
  a.digest[1] = 2;
  EXPECT_EQ(-ENOENT, ramcache.Open(a));
}


TEST(T_RamCacheManager, VolatileEntry) {
  perf::Statistics statistics;
  shash::Any a;
  RamCacheManager ramcache(4*alloc_size, 3*cache_size, &statistics);

  char buf[alloc_size];
  memset(buf, 42, alloc_size);

  void *txn1 = alloca(ramcache.SizeOfTxn());
  void *txn2 = alloca(ramcache.SizeOfTxn());
  void *txn3 = alloca(ramcache.SizeOfTxn());
  void *txn4 = alloca(ramcache.SizeOfTxn());
  void *txn5 = alloca(ramcache.SizeOfTxn());

  a.digest[1] = 1;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn1));
  a.digest[1] = 2;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn2));
  a.digest[1] = 3;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn3));
  a.digest[1] = 4;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn4));
  a.digest[1] = 5;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn5));

  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn1));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn2));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn3));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn4));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn5));

  ramcache.CtrlTxn("", cache::CacheManager::kTypeVolatile, 0, txn4);

  EXPECT_EQ(0, ramcache.CommitTxn(txn1));
  EXPECT_EQ(0, ramcache.CommitTxn(txn2));
  EXPECT_EQ(0, ramcache.CommitTxn(txn3));
  EXPECT_EQ(0, ramcache.CommitTxn(txn4));
  EXPECT_EQ(0, ramcache.CommitTxn(txn5));

  a.digest[1] = 1;
  EXPECT_GE(ramcache.Open(a), 0);
  a.digest[1] = 4;
  EXPECT_EQ(-ENOENT, ramcache.Open(a));
}


TEST(T_RamCacheManager, LargeCommit) {
  perf::Statistics statistics;
  shash::Any a;
  RamCacheManager ramcache(4*alloc_size, 3*cache_size, &statistics);

  char buf[alloc_size];
  memset(buf, 42, alloc_size);

  void *txn1 = alloca(ramcache.SizeOfTxn());
  void *txn2 = alloca(ramcache.SizeOfTxn());
  void *txn3 = alloca(ramcache.SizeOfTxn());

  a.digest[1] = 1;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn1));
  a.digest[1] = 2;
  EXPECT_EQ(0, ramcache.StartTxn(a, alloc_size, txn2));
  a.digest[1] = 3;
  EXPECT_EQ(0, ramcache.StartTxn(a, 4*alloc_size, txn3));

  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn1));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn2));
  EXPECT_EQ(alloc_size, ramcache.Write(buf, alloc_size, txn3));

  EXPECT_EQ(0, ramcache.CommitTxn(txn1));
  int fd = ramcache.OpenFromTxn(txn2);
  EXPECT_GE(fd, 0);
  EXPECT_EQ(-ENOSPC, ramcache.CommitTxn(txn3));
  a.digest[1] = 1;
  EXPECT_EQ(-ENOENT, ramcache.Open(a));
  a.digest[1] = 2;
  EXPECT_EQ(0, ramcache.Close(fd));
  EXPECT_EQ(0, ramcache.CommitTxn(txn3));
}
