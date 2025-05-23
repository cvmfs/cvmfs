/**
 * This file is part of the CernVM File System.
 */
#define __STDC_FORMAT_MACROS
#include <benchmark/benchmark.h>
#include <inttypes.h>
#include <stdint.h>

#include <cstdio>

#include "bm_util.h"
#include "crypto/hash.h"
#include "directory_entry.h"
#include "shortstring.h"
#include "smallhash.h"
#include "util/murmur.hxx"
#include "util/prng.h"

class BM_SmallHash : public benchmark::Fixture {
 protected:
  static const unsigned kNumRandomNumbers = 1000000;

  virtual void SetUp(const benchmark::State &st) {
    prng_.InitLocaltime();
    for (unsigned i = 0; i < kNumRandomNumbers; ++i) {
      values_int_[i] = prng_.Next(kNumRandomNumbers) + 1;
      shash::Md5 md5hash(reinterpret_cast<const char *>(&values_int_[i]),
                         sizeof(values_int_[i]));
      values_md5_[i] = md5hash;
    }
  }

  virtual void TearDown(const benchmark::State &st) { }

  static inline uint32_t hasher_uint64t(const uint64_t &value) {
    return MurmurHash2(&value, sizeof(value), 0x07387a4f);
  }

  static inline uint32_t hasher_md5(const shash::Md5 &key) {
    // Don't start with the first bytes, because == is using them as well
    return (uint32_t) * (reinterpret_cast<const uint32_t *>(key.digest) + 1);
  }

  void SetCollisionLabel(uint64_t num_collisions,
                         uint32_t max_collisions,
                         size_t iterations,
                         benchmark::State *st) {
    char label[64];
    snprintf(
        label, sizeof(label), "collisions (avg/max) %f / %" PRIu32,
        static_cast<float>(num_collisions) / static_cast<float>(iterations),
        max_collisions);
    st->SetLabel(label);
  }

  Prng prng_;
  uint64_t values_int_[kNumRandomNumbers];
  shash::Md5 values_md5_[kNumRandomNumbers];
};


BENCHMARK_DEFINE_F(BM_SmallHash, Baseline)(benchmark::State &st) {
  unsigned i = 0;
  while (st.KeepRunning()) {
    uint64_t key = values_int_[i % kNumRandomNumbers];
    Escape(&key);
    ++i;
  }
  Escape(&i);
}
BENCHMARK_REGISTER_F(BM_SmallHash, Baseline)->Repetitions(3);


// Typical use in meta-data caches

BENCHMARK_DEFINE_F(BM_SmallHash, InsertIntDirent)(benchmark::State &st) {
  SmallHashFixed<uint64_t, catalog::DirectoryEntry> htable;
  htable.Init(st.range(0), 0, hasher_uint64t);
  catalog::DirectoryEntry value;

  unsigned i = 0;
  while (st.KeepRunning()) {
    uint64_t key = values_int_[i % st.range(0)];
    htable.Insert(key, value);
    ++i;
  }

  st.SetItemsProcessed(i);
  uint64_t num_collisions;
  uint32_t max_collisions;
  htable.GetCollisionStats(&num_collisions, &max_collisions);
  SetCollisionLabel(num_collisions, max_collisions, i, &st);
}
BENCHMARK_REGISTER_F(BM_SmallHash, InsertIntDirent)->Repetitions(3)->Arg(5000);


BENCHMARK_DEFINE_F(BM_SmallHash, InsertIntPath)(benchmark::State &st) {
  SmallHashFixed<uint64_t, PathString> htable;
  htable.Init(st.range(0), 0, hasher_uint64t);
  PathString value;

  unsigned i = 0;
  while (st.KeepRunning()) {
    uint64_t key = values_int_[i % st.range(0)];
    htable.Insert(key, value);
    ++i;
  }

  st.SetItemsProcessed(i);
  uint64_t num_collisions;
  uint32_t max_collisions;
  htable.GetCollisionStats(&num_collisions, &max_collisions);
  SetCollisionLabel(num_collisions, max_collisions, i, &st);
}
BENCHMARK_REGISTER_F(BM_SmallHash, InsertIntPath)->Repetitions(3)->Arg(5000);


BENCHMARK_DEFINE_F(BM_SmallHash, InsertMd5Dirent)(benchmark::State &st) {
  SmallHashFixed<shash::Md5, catalog::DirectoryEntry> htable;
  htable.Init(st.range(0), shash::Md5(shash::AsciiPtr("!")), hasher_md5);
  catalog::DirectoryEntry value;

  unsigned i = 0;
  while (st.KeepRunning()) {
    htable.Insert(values_md5_[i % st.range(0)], value);
    ++i;
  }

  st.SetItemsProcessed(i);
  uint64_t num_collisions;
  uint32_t max_collisions;
  htable.GetCollisionStats(&num_collisions, &max_collisions);
  SetCollisionLabel(num_collisions, max_collisions, i, &st);
}
BENCHMARK_REGISTER_F(BM_SmallHash, InsertMd5Dirent)->Repetitions(3)->Arg(40000);
