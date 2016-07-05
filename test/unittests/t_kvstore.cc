/**
 * This file is part of the CernVM File System.
 */

#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <gtest/gtest.h>

#include "cache.h"
#include "hash.h"
#include "kvstore.h"
#include "statistics.h"

using namespace std;  // NOLINT

static const unsigned cache_size = 1024;
static const size_t malloc_size = 16;

TEST(T_MemoryKvStore, Commit) {
  perf::Statistics statistics;
  kvstore::MemoryKvStore store(cache_size, "T_MemoryKvStore", &statistics);

  kvstore::MemoryBuffer buf;
  shash::Md5 m(shash::AsciiPtr("!"));
  shash::Any a(m.algorithm, m.digest, m.suffix);

  buf.address = malloc(malloc_size);
  buf.size = malloc_size;
  buf.refcount = 0;
  buf.object_type = cache::CacheManager::kTypeRegular;

  EXPECT_EQ(-ENOENT, store.GetSize(a));
  EXPECT_EQ(0, (int64_t) store.GetUsed());
  EXPECT_FALSE(store.Commit(a, buf));
  EXPECT_EQ((int64_t) malloc_size, store.GetSize(a));
  EXPECT_TRUE(store.Commit(a, buf));
  EXPECT_EQ(malloc_size, store.GetUsed());
}

TEST(T_MemoryKvStore, PopBuffer) {
  perf::Statistics statistics;
  kvstore::MemoryKvStore store(cache_size, "T_MemoryKvStore", &statistics);

  kvstore::MemoryBuffer buf;
  shash::Md5 m(shash::AsciiPtr("!"));
  shash::Any a(m.algorithm, m.digest, m.suffix);

  buf.address = malloc(malloc_size);
  buf.size = malloc_size;
  buf.refcount = 0;
  buf.object_type = cache::CacheManager::kTypeRegular;

  EXPECT_EQ(0, (int64_t) store.GetUsed());
  EXPECT_FALSE(store.Commit(a, buf));
  EXPECT_EQ(malloc_size, store.GetUsed());
  memset(buf.address, 42, malloc_size);
  kvstore::MemoryBuffer out;
  EXPECT_FALSE(store.PopBuffer(shash::Any(), &out));
  EXPECT_TRUE(store.PopBuffer(a, &out));
  EXPECT_EQ(out.address, buf.address);
  EXPECT_EQ(0, (int64_t) store.GetUsed());
  memset(out.address, 24, malloc_size);
  free(out.address);
}

TEST(T_MemoryKvStore, Delete) {
  perf::Statistics statistics;
  kvstore::MemoryKvStore store(cache_size, "T_MemoryKvStore", &statistics);

  kvstore::MemoryBuffer buf;
  shash::Md5 m1(shash::AsciiPtr("!"));
  shash::Any a1(m1.algorithm, m1.digest, m1.suffix);
  shash::Md5 m2(shash::AsciiPtr("i"));
  shash::Any a2(m2.algorithm, m2.digest, m2.suffix);

  buf.address = malloc(malloc_size);
  buf.size = malloc_size;
  buf.refcount = 0;
  buf.object_type = cache::CacheManager::kTypeRegular;

  EXPECT_EQ(0, (int64_t) store.GetUsed());
  EXPECT_FALSE(store.Commit(a1, buf));
  EXPECT_EQ(malloc_size, store.GetUsed());
  EXPECT_FALSE(store.Delete(a2));
  buf.address = malloc(malloc_size);
  EXPECT_FALSE(store.Commit(a2, buf));
  EXPECT_EQ(2*malloc_size, store.GetUsed());
  memset(buf.address, 42, malloc_size);
  EXPECT_TRUE(store.Delete(a1));
  EXPECT_FALSE(store.Delete(a1));
  EXPECT_EQ(malloc_size, store.GetUsed());
  EXPECT_TRUE(store.Delete(a2));
  EXPECT_EQ(0, (int64_t) store.GetUsed());
}

TEST(T_MemoryKvStore, Read) {
  perf::Statistics statistics;
  kvstore::MemoryKvStore store(cache_size, "T_MemoryKvStore", &statistics);

  kvstore::MemoryBuffer buf;
  shash::Md5 m(shash::AsciiPtr("!"));
  shash::Any a(m.algorithm, m.digest, m.suffix);

  char correct[malloc_size];
  char out[malloc_size];
  buf.address = malloc(malloc_size);
  buf.size = malloc_size;
  buf.refcount = 0;
  buf.object_type = cache::CacheManager::kTypeRegular;
  memset(buf.address, 42, malloc_size);
  memset(correct, 24, malloc_size);
  memset(out, 0, malloc_size);

  EXPECT_EQ(0, (int64_t) store.GetUsed());
  EXPECT_FALSE(store.Commit(a, buf));
  EXPECT_EQ(malloc_size, store.GetUsed());

  EXPECT_EQ((int64_t) malloc_size, store.Read(a, out, malloc_size, 0));
  EXPECT_GT(strncmp(out, correct, malloc_size), 0);
  memset(correct, 42, malloc_size);
  memset(out, 0, malloc_size);

  EXPECT_EQ(4, store.Read(a, &out[2], 4, 3));
  EXPECT_EQ(0, strncmp(&out[2], &correct[2], 4));
  memset(out, 0, malloc_size);

  EXPECT_EQ((int64_t) malloc_size - 3, store.Read(a, out, 1111, 3));
  EXPECT_EQ(0, strncmp(out, correct, malloc_size - 3));
  memset(out, 0, malloc_size);

  EXPECT_TRUE(store.Delete(a));
  EXPECT_EQ(0, (int64_t) store.GetUsed());
}

TEST(T_MemoryKvStore, Refcount) {
  perf::Statistics statistics;
  kvstore::MemoryKvStore store(cache_size, "T_MemoryKvStore", &statistics);

  kvstore::MemoryBuffer buf;
  shash::Md5 m(shash::AsciiPtr("!"));
  shash::Any a(m.algorithm, m.digest, m.suffix);

  buf.address = malloc(malloc_size);
  buf.size = malloc_size;
  buf.refcount = 0;
  buf.object_type = cache::CacheManager::kTypeRegular;

  EXPECT_FALSE(store.Ref(a));
  EXPECT_FALSE(store.Unref(a));
  EXPECT_EQ(0, (int64_t) store.GetUsed());
  EXPECT_FALSE(store.Commit(a, buf));
  EXPECT_EQ(malloc_size, store.GetUsed());

  EXPECT_EQ(0, store.GetRefcount(a));
  EXPECT_FALSE(store.Unref(a));
  EXPECT_EQ(0, store.GetRefcount(a));
  EXPECT_TRUE(store.Ref(a));
  EXPECT_EQ(1, store.GetRefcount(a));

  EXPECT_TRUE(store.Ref(a));
  EXPECT_TRUE(store.Ref(a));
  EXPECT_EQ(3, store.GetRefcount(a));
  EXPECT_TRUE(store.Unref(a));
  EXPECT_TRUE(store.Unref(a));
  EXPECT_EQ(1, store.GetRefcount(a));

  EXPECT_TRUE(store.Unref(a));
  EXPECT_EQ(0, store.GetRefcount(a));
  EXPECT_FALSE(store.Unref(a));
  EXPECT_EQ(0, store.GetRefcount(a));

  EXPECT_TRUE(store.Delete(a));
  EXPECT_EQ(0, (int64_t) store.GetUsed());
}

TEST(T_MemoryKvStore, Shrink) {
  perf::Statistics statistics;
  kvstore::MemoryKvStore store(cache_size, "T_MemoryKvStore", &statistics);

  kvstore::MemoryBuffer buf;
  shash::Md5 m(shash::AsciiPtr("!"));
  shash::Any a(m.algorithm, m.digest, m.suffix);

  buf.address = malloc(malloc_size);
  buf.size = malloc_size;
  buf.refcount = 0;
  buf.object_type = cache::CacheManager::kTypeRegular;

  EXPECT_EQ(0, (int64_t) store.GetUsed());
  EXPECT_FALSE(store.Commit(a, buf));
  EXPECT_EQ(malloc_size, store.GetUsed());
  for (int i = 0; i < 99; i++) {
    (*(reinterpret_cast<uint32_t *>(a.digest + 1)))++;
    buf.address = malloc(malloc_size);
    store.Commit(a, buf);
  }
  EXPECT_EQ(100*malloc_size, store.GetUsed());

  EXPECT_TRUE(store.Shrink(1000*malloc_size));
  EXPECT_TRUE(store.Shrink(100*malloc_size));
  EXPECT_EQ(100*malloc_size, store.GetUsed());
  EXPECT_TRUE(store.Shrink(94*malloc_size));
  EXPECT_EQ(94*malloc_size, store.GetUsed());
  EXPECT_TRUE(store.Shrink(60*malloc_size - 1));
  EXPECT_EQ(59*malloc_size, store.GetUsed());
  EXPECT_TRUE(store.Shrink(90));
  EXPECT_EQ((90/malloc_size)*malloc_size, store.GetUsed());
  EXPECT_TRUE(store.Shrink(0));
  EXPECT_EQ(0, (int64_t) store.GetUsed());
}
