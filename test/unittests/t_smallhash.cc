/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <inttypes.h>
#include <pthread.h>

#include <limits>

#include "crypto/hash.h"
#include "smallhash.h"
#include "util/murmur.hxx"

static uint32_t hasher_int(const int &key) {
  return MurmurHash2(&key, sizeof(key), 0x07387a4f);
}

static uint32_t hasher_md5(const shash::Md5 &key) {
  // Don't start with the first bytes, because == is using them as well
  return static_cast<uint32_t>(*(reinterpret_cast<const uint32_t *>(key.digest) + 1));
}

class T_Smallhash : public ::testing::Test {
 protected:
  virtual void SetUp() {
    smallhash_.Init(16, -1, hasher_int);
    smallhash_md5_.Init(16, shash::Md5(shash::AsciiPtr("!")), hasher_md5);
    multihash_.Init(kNumHashmaps, -1, hasher_int);
    active_multihash = &multihash_;

    unsigned num_hashmaps = kNumHashmaps;
    EXPECT_EQ(num_hashmaps, multihash_.num_hashmaps());

    srand(time(NULL));
  }

  uint32_t GetMultiSize() {
    uint32_t individual_sizes[kNumHashmaps];
    multihash_.GetSizes(individual_sizes);
    uint32_t overall_size = 0;
    for (uint8_t i = 0; i < kNumHashmaps; ++i)
      overall_size += individual_sizes[i];
    return overall_size;
  }

  static void *tf_insert(void *data) {
    int ID = reinterpret_cast<uint64_t>(data);

    unsigned chunk = kNumElements / kNumThreads;
    for (unsigned i = chunk * ID; i < chunk * ID + chunk; ++i) {
      active_multihash->Insert(i, i);
    }
    return NULL;
  }

  static void *tf_erase(void *data) {
    int ID = reinterpret_cast<uint64_t>(data);

    unsigned chunk = kNumElements / kNumThreads;
    for (unsigned i = chunk * ID; i < chunk * ID + chunk; ++i) {
      active_multihash->Erase(i);
    }
    return NULL;
  }

  static const unsigned kNumElements = 1000000;
  static const unsigned kNumHashmaps = 42;
  static const unsigned kNumThreads = 8;
  SmallHashDynamic<int, int> smallhash_;
  SmallHashDynamic<shash::Md5, int> smallhash_md5_;
  MultiHash<int, int> multihash_;
  static MultiHash<int, int> *active_multihash;
};
MultiHash<int, int> *T_Smallhash::active_multihash = NULL;


TEST_F(T_Smallhash, Insert) {
  unsigned N = kNumElements;
  for (unsigned i = 0; i < N; ++i) {
    smallhash_.Insert(i, i);
  }

  EXPECT_EQ(N, smallhash_.size());
}


TEST_F(T_Smallhash, InsertMd5) {
  unsigned N = kNumElements;
  for (unsigned i = 0; i < N; ++i) {
    shash::Md5 random_hash;
    random_hash.Randomize(i);
    smallhash_md5_.Insert(random_hash, i);
  }

  EXPECT_EQ(N, smallhash_md5_.size());
}


TEST_F(T_Smallhash, InsertAndCopyMd5Slow) {
  unsigned N = kNumElements;
  for (unsigned i = 0; i < N; ++i) {
    shash::Md5 random_hash;
    random_hash.Randomize(i);
    smallhash_md5_.Insert(random_hash, i);
  }

  const uint32_t max_collisions = std::numeric_limits<uint32_t>::max() / N;
  EXPECT_GT(max_collisions, smallhash_md5_.max_collisions_);

  SmallHashDynamic<shash::Md5, int> new_smallhash_md5;
  new_smallhash_md5.Init(16, shash::Md5(shash::AsciiPtr("!")), hasher_md5);
  new_smallhash_md5 = smallhash_md5_;

  EXPECT_EQ(N, smallhash_md5_.size());
  EXPECT_EQ(N, new_smallhash_md5.size());
  EXPECT_GT(max_collisions, new_smallhash_md5.max_collisions_);
}


TEST_F(T_Smallhash, InsertAndErase) {
  unsigned N = kNumElements;
  unsigned initial_capacity = smallhash_.capacity();
  for (unsigned i = 0; i < N; ++i) {
    smallhash_.Insert(i, i);
  }
  EXPECT_EQ(N, smallhash_.size());
  for (unsigned i = 0; i < N; ++i) {
    smallhash_.Erase(i);
  }
  EXPECT_EQ(unsigned(0), smallhash_.size());
  EXPECT_EQ(initial_capacity, smallhash_.capacity());

  for (unsigned i = 0; i < N; ++i) {
    smallhash_.Insert(i, i);
  }
  EXPECT_EQ(N, smallhash_.size());
  smallhash_.Clear();
  EXPECT_EQ(unsigned(0), smallhash_.size());
  EXPECT_EQ(initial_capacity, smallhash_.capacity());
}


TEST_F(T_Smallhash, EraseUnknown) {
  unsigned N = kNumElements;
  for (unsigned i = 0; i < N; ++i) {
    smallhash_.Insert(i, i);
  }
  EXPECT_FALSE(smallhash_.Erase(N + 1));

  EXPECT_EQ(N, smallhash_.size());

  EXPECT_TRUE(smallhash_.Erase(0));
  EXPECT_EQ(N - 1, smallhash_.size());
}


TEST_F(T_Smallhash, Lookup) {
  unsigned N = kNumElements;
  for (unsigned i = 0; i < N; ++i) {
    smallhash_.Insert(i, i);
  }
  for (unsigned i = 0; i < N; ++i) {
    int value = -1;
    bool found = smallhash_.Lookup(i, &value);
    EXPECT_TRUE(found);
    EXPECT_EQ(unsigned(value), i);
  }
}


TEST_F(T_Smallhash, MultihashCycleSlow) {
  unsigned N = kNumElements;
  for (unsigned i = 0; i < N; ++i) {
    multihash_.Insert(i, i);
  }
  EXPECT_EQ(N, GetMultiSize());
  for (unsigned i = 0; i < N; ++i) {
    int value = -1;
    bool found = multihash_.Lookup(i, &value);
    EXPECT_TRUE(found);
    EXPECT_EQ(unsigned(value), i);
  }
  for (unsigned i = 0; i < N; ++i) {
    multihash_.Erase(i);
  }
  EXPECT_EQ(unsigned(0), GetMultiSize());

  for (unsigned i = 0; i < N; ++i) {
    multihash_.Insert(i, i);
  }
  EXPECT_EQ(N, GetMultiSize());
  multihash_.Clear();
  EXPECT_EQ(unsigned(0), GetMultiSize());
}


TEST_F(T_Smallhash, MultihashBalance) {
  unsigned N = kNumElements;
  for (unsigned i = 0; i < N; ++i) {
    multihash_.Insert(i, i);
  }
  EXPECT_EQ(N, GetMultiSize());

  uint32_t individual_sizes[kNumHashmaps];
  multihash_.GetSizes(individual_sizes);
  for (uint8_t i = 0; i < kNumHashmaps; ++i)
    ASSERT_GT(individual_sizes[i], unsigned(0));
}


TEST_F(T_Smallhash, MultihashMultithread) {
  unsigned N = kNumElements;

  pthread_t threads[kNumThreads];
  for (unsigned i = 0; i < kNumThreads; ++i) {
    pthread_create(&threads[i], NULL, tf_insert, reinterpret_cast<void *>(i));
  }
  for (unsigned i = 0; i < kNumThreads; ++i) {
    pthread_join(threads[i], NULL);
  }
  EXPECT_EQ(N, GetMultiSize());

  for (unsigned i = 0; i < kNumThreads; ++i) {
    pthread_create(&threads[i], NULL, tf_erase, reinterpret_cast<void *>(i));
  }
  for (unsigned i = 0; i < kNumThreads; ++i) {
    pthread_join(threads[i], NULL);
  }
  EXPECT_EQ(unsigned(0), GetMultiSize());
}

TEST_F(T_Smallhash, CanDestructUninitialized) {
  SmallHashDynamic<int, int>
      *smallhash_uninit = new SmallHashDynamic<int, int>();
  // No Init()
  delete smallhash_uninit;
}
