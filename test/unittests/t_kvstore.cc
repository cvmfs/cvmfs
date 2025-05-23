/**
 * This file is part of the CernVM File System.
 */

#include <errno.h>
#include <gtest/gtest.h>
#include <stdint.h>
#include <string.h>

#include "cache.h"
#include "crypto/hash.h"
#include "kvstore.h"
#include "statistics.h"

using namespace std;  // NOLINT

static const unsigned cache_size = 1024;
static const size_t malloc_size = 16;

namespace kvstore {

class T_MemoryKvStore : public ::testing::Test {
 public:
  T_MemoryKvStore()
      : store_(cache_size,
               MemoryKvStore::kMallocLibc,
               128 * malloc_size,
               perf::StatisticsTemplate("test", &statistics_))
      , m1_(shash::AsciiPtr("!"))
      , a1_(m1_.algorithm, m1_.digest, m1_.suffix)
      , m2_(shash::AsciiPtr("i"))
      , a2_(m2_.algorithm, m2_.digest, m2_.suffix) { }

 protected:
  virtual void SetUp() {
    buf_.address = malloc(malloc_size);
    buf_.size = malloc_size;
    buf_.refcount = 0;
    buf_.object_flags = 0;
  }

  virtual void TearDown() { }

  perf::Statistics statistics_;
  MemoryKvStore store_;
  MemoryBuffer buf_;
  shash::Md5 m1_;
  shash::Any a1_;
  shash::Md5 m2_;
  shash::Any a2_;
};

TEST_F(T_MemoryKvStore, Commit) {
  EXPECT_EQ(-ENOENT, store_.GetSize(a1_));
  EXPECT_EQ(0, (int64_t)store_.GetUsed());
  buf_.id = a1_;
  EXPECT_EQ(0, store_.Commit(buf_));
  EXPECT_EQ((int64_t)malloc_size, store_.GetSize(a1_));
  EXPECT_EQ(0, store_.Commit(buf_));
  EXPECT_EQ(malloc_size, store_.GetUsed());
  free(buf_.address);
}

TEST_F(T_MemoryKvStore, Delete) {
  EXPECT_EQ(0, (int64_t)store_.GetUsed());
  buf_.id = a1_;
  EXPECT_EQ(0, store_.Commit(buf_));
  EXPECT_EQ(malloc_size, store_.GetUsed());
  EXPECT_FALSE(store_.Delete(a2_));
  buf_.address = malloc(malloc_size);
  buf_.id = a2_;
  EXPECT_EQ(0, store_.Commit(buf_));
  EXPECT_EQ(2 * malloc_size, store_.GetUsed());
  memset(buf_.address, 42, malloc_size);
  EXPECT_TRUE(store_.Delete(a1_));
  EXPECT_FALSE(store_.Delete(a1_));
  EXPECT_EQ(malloc_size, store_.GetUsed());
  EXPECT_TRUE(store_.Delete(a2_));
  EXPECT_EQ(0, (int64_t)store_.GetUsed());
}

TEST_F(T_MemoryKvStore, Read) {
  char correct[malloc_size];
  char out[malloc_size];
  memset(buf_.address, 42, malloc_size);
  memset(correct, 24, malloc_size);
  memset(out, 0, malloc_size);

  EXPECT_EQ(0, (int64_t)store_.GetUsed());
  buf_.id = a1_;
  EXPECT_EQ(0, store_.Commit(buf_));
  EXPECT_EQ(malloc_size, store_.GetUsed());

  EXPECT_EQ((int64_t)malloc_size, store_.Read(a1_, out, malloc_size, 0));
  EXPECT_GT(strncmp(out, correct, malloc_size), 0);
  memset(correct, 42, malloc_size);
  memset(out, 0, malloc_size);

  EXPECT_EQ(4, store_.Read(a1_, &out[2], 4, 3));
  EXPECT_EQ(0, strncmp(&out[2], &correct[2], 4));
  memset(out, 0, malloc_size);

  EXPECT_EQ((int64_t)malloc_size - 3, store_.Read(a1_, out, 1111, 3));
  EXPECT_EQ(0, strncmp(out, correct, malloc_size - 3));
  memset(out, 0, malloc_size);

  EXPECT_TRUE(store_.Delete(a1_));
  EXPECT_EQ(0, (int64_t)store_.GetUsed());
}

TEST_F(T_MemoryKvStore, Refcount) {
  EXPECT_FALSE(store_.IncRef(a1_));
  EXPECT_FALSE(store_.Unref(a1_));
  EXPECT_EQ(0, (int64_t)store_.GetUsed());
  buf_.id = a1_;
  EXPECT_EQ(0, store_.Commit(buf_));
  EXPECT_EQ(malloc_size, store_.GetUsed());

  EXPECT_EQ(0, store_.GetRefcount(a1_));
  EXPECT_TRUE(store_.IncRef(a1_));
  EXPECT_EQ(1, store_.GetRefcount(a1_));

  EXPECT_TRUE(store_.IncRef(a1_));
  EXPECT_TRUE(store_.IncRef(a1_));
  EXPECT_EQ(3, store_.GetRefcount(a1_));
  EXPECT_TRUE(store_.Unref(a1_));
  EXPECT_TRUE(store_.Unref(a1_));
  EXPECT_EQ(1, store_.GetRefcount(a1_));

  EXPECT_TRUE(store_.Unref(a1_));
  EXPECT_EQ(0, store_.GetRefcount(a1_));

  EXPECT_TRUE(store_.Delete(a1_));
  EXPECT_EQ(0, (int64_t)store_.GetUsed());
}

TEST_F(T_MemoryKvStore, ShrinkTo) {
  buf_.refcount = 1;
  EXPECT_EQ(0, (int64_t)store_.GetUsed());
  buf_.id = a1_;
  EXPECT_EQ(0, store_.Commit(buf_));
  EXPECT_EQ(malloc_size, store_.GetUsed());
  buf_.refcount = 0;
  for (int i = 0; i < 99; i++) {
    (*(reinterpret_cast<uint32_t *>(buf_.id.digest + 1)))++;
    buf_.address = malloc(malloc_size);
    store_.Commit(buf_);
  }
  EXPECT_EQ(100 * malloc_size, store_.GetUsed());

  EXPECT_TRUE(store_.ShrinkTo(1000 * malloc_size));
  EXPECT_TRUE(store_.ShrinkTo(100 * malloc_size));
  EXPECT_EQ(100 * malloc_size, store_.GetUsed());
  EXPECT_TRUE(store_.ShrinkTo(94 * malloc_size));
  EXPECT_EQ(94 * malloc_size, store_.GetUsed());
  EXPECT_TRUE(store_.ShrinkTo(60 * malloc_size - 1));
  EXPECT_EQ(59 * malloc_size, store_.GetUsed());
  EXPECT_TRUE(store_.ShrinkTo(90));
  EXPECT_EQ((90 / malloc_size) * malloc_size, store_.GetUsed());
  EXPECT_FALSE(store_.ShrinkTo(0));
  EXPECT_EQ(malloc_size, store_.GetUsed());
}

}  // namespace kvstore
