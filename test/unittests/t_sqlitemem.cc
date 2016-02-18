/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <malloc.h>

#include <cstdlib>
#include <cstring>
#include <vector>
#include <map>

#include "../../cvmfs/murmur.h"
#include "../../cvmfs/prng.h"
#include "../../cvmfs/sqlitemem.h"
#include "../../cvmfs/util.h"

using namespace std;  // NOLINT

namespace sqlite {

class T_Sqlitemem : public ::testing::Test {
 protected:
  virtual void SetUp() {
    EXPECT_FALSE(MemoryManager::HasInstance());
    mem_mgr_ = MemoryManager::GetInstance();
    EXPECT_TRUE(MemoryManager::HasInstance());
  }

  virtual void TearDown() {
    MemoryManager::CleanupInstance();
    EXPECT_FALSE(MemoryManager::HasInstance());
  }

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

  uint32_t SimulateMalloc(unsigned N, unsigned S_max, unsigned T_max) {
    MemoryManager::MallocArena M;
    Prng prng;
    prng.InitLocaltime();
    // prng.InitSeed(42);
    map< unsigned, vector<void *> * > schedule_free;
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

  MemoryManager *mem_mgr_;
};


TEST_F(T_Sqlitemem, LookasideBufferArena) {
  MemoryManager::LookasideBufferArena la_arena;
  EXPECT_TRUE(la_arena.IsEmpty());

  void *buffer = la_arena.GetBuffer();
  EXPECT_TRUE(buffer != NULL);
  EXPECT_TRUE(la_arena.Contains(buffer));
  EXPECT_FALSE(la_arena.Contains(reinterpret_cast<char *>(buffer) - 1));
  EXPECT_FALSE(la_arena.Contains(
    reinterpret_cast<char *>(buffer) +
    MemoryManager::LookasideBufferArena::kArenaSize));
  EXPECT_TRUE(la_arena.Contains(
    reinterpret_cast<char *>(buffer) +
    MemoryManager::LookasideBufferArena::kArenaSize) - 1);
  EXPECT_FALSE(la_arena.IsEmpty());
  la_arena.PutBuffer(buffer);
  EXPECT_TRUE(la_arena.IsEmpty());

  // Allocate everything sequentially
  unsigned N = MemoryManager::LookasideBufferArena::kBuffersPerArena;
  void *buffer_multi[N];
  for (unsigned i = 0; i < N; ++i) {
    buffer_multi[i] = la_arena.GetBuffer();
    EXPECT_TRUE(buffer_multi[i] != NULL);
    EXPECT_TRUE(la_arena.Contains(buffer_multi[i]));
  }
  EXPECT_FALSE(la_arena.IsEmpty());
  EXPECT_EQ(NULL, la_arena.GetBuffer());  // Arena should be full

  // Deallocate half in random order
  Prng prng;
  prng.InitLocaltime();
  vector<unsigned> bufferids;
  for (unsigned i = 0; i < N; ++i)
    bufferids.push_back(i);
  bufferids = Shuffle(bufferids, &prng);
  for (unsigned i = 0; i < N/2; ++i) {
    la_arena.PutBuffer(buffer_multi[bufferids[i]]);
  }

  // Allocate again until full
  for (unsigned i = 0; i < N/2; ++i) {
    buffer_multi[bufferids[i]] = la_arena.GetBuffer();
    EXPECT_TRUE(buffer_multi[bufferids[i]] != NULL);
    EXPECT_TRUE(la_arena.Contains(buffer_multi[bufferids[i]]));
  }
  EXPECT_EQ(NULL, la_arena.GetBuffer());  // Arena should be full

  // Deallocate all in random order
  for (unsigned i = 0; i < N; ++i) {
    la_arena.PutBuffer(buffer_multi[bufferids[i]]);
  }
  EXPECT_TRUE(la_arena.IsEmpty());
}


TEST_F(T_Sqlitemem, LookasideBuffer) {
  unsigned N = MemoryManager::LookasideBufferArena::kBuffersPerArena;
  void *buffer_multi[N + 1];
  for (unsigned i = 0; i < N; ++i) {
    buffer_multi[i] = mem_mgr_->GetLookasideBuffer();
    EXPECT_TRUE(buffer_multi[i] != NULL);
  }
  EXPECT_EQ(1U, mem_mgr_->lookaside_buffer_arenas_.size());

  buffer_multi[N] = mem_mgr_->GetLookasideBuffer();
  EXPECT_TRUE(buffer_multi[N] != NULL);
  EXPECT_EQ(2U, mem_mgr_->lookaside_buffer_arenas_.size());

  for (unsigned i = 0; i < N; ++i) {
    mem_mgr_->PutLookasideBuffer(buffer_multi[i]);
  }
  EXPECT_EQ(1U, mem_mgr_->lookaside_buffer_arenas_.size());

  mem_mgr_->PutLookasideBuffer(buffer_multi[N]);
  EXPECT_EQ(1U, mem_mgr_->lookaside_buffer_arenas_.size());
}


TEST_F(T_Sqlitemem, MallocArena) {
  MemoryManager::MallocArena M;

  EXPECT_TRUE(M.IsEmpty());

  vector<void *> ptrs;
  do {
    ptrs.push_back(M.Malloc(8192));
  } while (ptrs.back() != NULL);
  // More than 90% utiliziation
  unsigned num = ptrs.size();
  EXPECT_GE(num, (M.kArenaSize / 8192) * 90 / 100);
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


TEST_F(T_Sqlitemem, MallocArenaMC) {
  SimulateMalloc(10000, 200, 100);
  SimulateMalloc(10000, 2000, 1000);
  SimulateMalloc(10000, 20000, 10000);
  SimulateMalloc(10000, 200000, 1000);
  SimulateMalloc(10000, 2000, 10000);
  SimulateMalloc(10000, 4000, 20000);
}


TEST_F(T_Sqlitemem, MallocArenaMCSlow) {
  SimulateMalloc(1000000, 200, 100);
  SimulateMalloc(1000000, 2000, 1000);
  SimulateMalloc(1000000, 20000, 10000);
  SimulateMalloc(1000000, 200000, 1000);
  SimulateMalloc(1000000, 2000, 10000);
  SimulateMalloc(1000000, 4000, 20000);
}


TEST_F(T_Sqlitemem, Malloc) {
  EXPECT_EQ(1U, mem_mgr_->malloc_arenas_.size());
  void *p[4];
  for (unsigned i = 0; i < 4; ++i) {
    p[i] = mem_mgr_->GetMemory(MemoryManager::MallocArena::kArenaSize / 4);
  }
  EXPECT_EQ(2U, mem_mgr_->malloc_arenas_.size());
  mem_mgr_->PutMemory(p[3]);
  EXPECT_EQ(1U, mem_mgr_->malloc_arenas_.size());
  mem_mgr_->PutMemory(p[2]);
  mem_mgr_->PutMemory(p[1]);
  mem_mgr_->PutMemory(p[0]);
  EXPECT_EQ(1U, mem_mgr_->malloc_arenas_.size());
}


}  // namespace sqlite
