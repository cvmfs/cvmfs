/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <map>
#include <vector>

#include "malloc_arena.h"
#include "util/algorithm.h"
#include "util/prng.h"

using namespace std;  // NOLINT

class T_MallocArena : public ::testing::Test {
 protected:
  // 8MB SqliteMemoryManager arena
  static const unsigned kSmallArena = 8 * 1024 * 1024;
  static const unsigned kBigArena = 512 * 1024 * 1024;  // 512MB RAM Cache arena

  virtual void SetUp() { }

  virtual void TearDown() { }

  uint32_t MemChecksum(void *p, uint32_t size) {
    struct {
      void *a;
      uint32_t b;
    } values;
    memset(&values, 0, sizeof(values));
    values.a = p;
    values.b = size;
    return MurmurHash2(&values, sizeof(values), 0x07387a4f);
  }

  uint32_t SimulateMalloc(unsigned arena_size, unsigned N, unsigned S_max,
                          unsigned T_max) {
    MallocArena M(arena_size);
    Prng prng;
    prng.InitLocaltime();
    // prng.InitSeed(42);
    map<unsigned, vector<void *> *> schedule_free;
    unsigned t = 0;
    uint32_t total_alloc = 0;

    do {
      t++;
      if (schedule_free.find(t) != schedule_free.end()) {
        vector<void *> *ptrs = schedule_free[t];
        for (unsigned i = 0; i < ptrs->size(); ++i) {
          char *p = reinterpret_cast<char *>((*ptrs)[i]);
          // uint32_t p_size = malloc_usable_size(p);
          uint32_t p_size = M.GetSize(p);
          // printf("FREE %p size %u\n", p, p_size);
          uint32_t checksum = MemChecksum(p, p_size);
          EXPECT_EQ(checksum, *reinterpret_cast<uint32_t *>(p));
          EXPECT_EQ(checksum, *reinterpret_cast<uint32_t *>(
                                  p + p_size - sizeof(uint32_t)));
          // free(p);
          M.Free(p);
          total_alloc -= p_size;
        }
        delete ptrs;
        schedule_free.erase(schedule_free.find(t));
      }

      unsigned S = prng.Next(S_max) + 8;
      unsigned T = prng.Next(T_max) + 1;

      // char *p = reinterpret_cast<char *>(malloc(S));
      char *p = reinterpret_cast<char *>(M.Malloc(S));
      if (p == NULL)
        continue;
      EXPECT_EQ(0, reinterpret_cast<intptr_t>(p) % 8) << "S is " << S;
      uint32_t p_size = M.GetSize(p);
      // uint32_t p_size = malloc_usable_size(p);
      EXPECT_GE(p_size, S);
      uint32_t checksum = MemChecksum(p, p_size);
      *reinterpret_cast<uint32_t *>(p) = checksum;
      // printf("ALLOC %p size %u\n", p, p_size);
      *reinterpret_cast<uint32_t *>(p + p_size - sizeof(uint32_t)) = checksum;
      total_alloc += p_size;

      if (schedule_free.find(t + T) == schedule_free.end()) {
        vector<void *> *v = new vector<void *>;
        v->push_back(p);
        schedule_free[t + T] = v;
      } else {
        vector<void *> *v = schedule_free[t + T];
        v->push_back(p);
      }
    } while (t < N);

    // printf("Allocated in total: %ukB\n", total_alloc / 1024);
    return total_alloc;
  }
};


TEST_F(T_MallocArena, Basic) {
  MallocArena M(kSmallArena);

  EXPECT_TRUE(M.IsEmpty());

  vector<void *> ptrs;
  do {
    ptrs.push_back(M.Malloc(8192));
  } while (ptrs.back() != NULL);
  // More than 90% utiliziation
  unsigned num = ptrs.size();
  EXPECT_GE(num, (kSmallArena / 8192) * 90 / 100);
  EXPECT_FALSE(M.IsEmpty());

  // Free and again
  for (unsigned i = 0; i < ptrs.size() - 1; ++i) {
    M.Free(ptrs[i]);
  }
  EXPECT_TRUE(M.IsEmpty());
  ptrs.clear();
  do {
    ptrs.push_back(M.Malloc(8192));
  } while (ptrs.back() != NULL);
  EXPECT_EQ(num, ptrs.size());
  EXPECT_FALSE(M.IsEmpty());
}


TEST_F(T_MallocArena, MonteCarlo) {
  SimulateMalloc(kSmallArena, 10000, 12800, 100);
  SimulateMalloc(kSmallArena, 10000, 128000, 1000);
  SimulateMalloc(kSmallArena, 10000, 1280000, 10000);
  SimulateMalloc(kSmallArena, 10000, 12800000, 1000);
  SimulateMalloc(kSmallArena, 10000, 128000, 10000);
  SimulateMalloc(kSmallArena, 10000, 256000, 20000);
}


TEST_F(T_MallocArena, MCBigArena) {
  SimulateMalloc(kBigArena, 10000, 200, 100);
  SimulateMalloc(kBigArena, 10000, 2000, 1000);
  SimulateMalloc(kBigArena, 10000, 20000, 10000);
  SimulateMalloc(kBigArena, 10000, 200000, 1000);
  SimulateMalloc(kBigArena, 10000, 2000, 10000);
  SimulateMalloc(kBigArena, 10000, 4000, 20000);
}


TEST_F(T_MallocArena, MonteCarloSlow) {
  SimulateMalloc(kSmallArena, 1000000, 200, 100);
  SimulateMalloc(kSmallArena, 1000000, 2000, 1000);
  SimulateMalloc(kSmallArena, 1000000, 20000, 10000);
  SimulateMalloc(kSmallArena, 1000000, 200000, 1000);
  SimulateMalloc(kSmallArena, 1000000, 2000, 10000);
  SimulateMalloc(kSmallArena, 1000000, 4000, 20000);
}
