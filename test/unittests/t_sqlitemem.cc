/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <map>
#include <vector>

#include "malloc_arena.h"
#include "sqlitemem.h"
#include "util/algorithm.h"
#include "util/murmur.hxx"
#include "util/pointer.h"
#include "util/prng.h"

using namespace std;  // NOLINT

class T_Sqlitemem : public ::testing::Test {
 protected:
  virtual void SetUp() {
    EXPECT_FALSE(SqliteMemoryManager::HasInstance());
    mem_mgr_ = SqliteMemoryManager::GetInstance();
    ASSERT_TRUE(SqliteMemoryManager::HasInstance());
  }

  virtual void TearDown() {
    SqliteMemoryManager::CleanupInstance();
    EXPECT_FALSE(SqliteMemoryManager::HasInstance());
  }

  SqliteMemoryManager *mem_mgr_;
};


TEST_F(T_Sqlitemem, LookasideBufferArena) {
  SqliteMemoryManager::LookasideBufferArena la_arena;
  EXPECT_TRUE(la_arena.IsEmpty());

  void *buffer = la_arena.GetBuffer();
  EXPECT_TRUE(buffer != NULL);
  EXPECT_TRUE(la_arena.Contains(buffer));
  EXPECT_FALSE(la_arena.Contains(reinterpret_cast<char *>(buffer) - 1));
  EXPECT_FALSE(la_arena.Contains(
      reinterpret_cast<char *>(buffer)
      + SqliteMemoryManager::LookasideBufferArena::kArenaSize));
  EXPECT_TRUE(
      la_arena.Contains(reinterpret_cast<char *>(buffer)
                        + SqliteMemoryManager::LookasideBufferArena::kArenaSize)
      - 1);
  EXPECT_FALSE(la_arena.IsEmpty());
  la_arena.PutBuffer(buffer);
  EXPECT_TRUE(la_arena.IsEmpty());

  // Allocate everything sequentially
  unsigned N = SqliteMemoryManager::LookasideBufferArena::kBuffersPerArena;
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
  for (unsigned i = 0; i < N / 2; ++i) {
    la_arena.PutBuffer(buffer_multi[bufferids[i]]);
  }

  // Allocate again until full
  for (unsigned i = 0; i < N / 2; ++i) {
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
  unsigned N = SqliteMemoryManager::LookasideBufferArena::kBuffersPerArena;
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


TEST_F(T_Sqlitemem, Malloc) {
  EXPECT_EQ(1U, mem_mgr_->malloc_arenas_.size());
  void *p[4];
  for (unsigned i = 0; i < 4; ++i) {
    p[i] = mem_mgr_->GetMemory(SqliteMemoryManager::kArenaSize / 4);
  }
  EXPECT_EQ(2U, mem_mgr_->malloc_arenas_.size());
  mem_mgr_->PutMemory(p[3]);
  EXPECT_EQ(1U, mem_mgr_->malloc_arenas_.size());
  mem_mgr_->PutMemory(p[2]);
  mem_mgr_->PutMemory(p[1]);
  mem_mgr_->PutMemory(p[0]);
  EXPECT_EQ(1U, mem_mgr_->malloc_arenas_.size());
}


TEST_F(T_Sqlitemem, Realloc) {
  unsigned char pattern_one = 0xFF;
  // At the end of the newly allocated blocks we have left-over tags from the
  // allocation procedure;  Don't compare the last 8 bytes for the pattern.
  unsigned char pattern_one_1kb[1016];
  memset(pattern_one_1kb, pattern_one, 1016);

  // Switch default memory arena against initialized one
  delete mem_mgr_->malloc_arenas_[0];
  mem_mgr_->malloc_arenas_[0] = MallocArena::CreateInitialized(
      SqliteMemoryManager::kArenaSize, pattern_one);

  // Allocate 2 times 1kB from the end of the arena and set both areas to zero
  void *p1 = mem_mgr_->GetMemory(1024);
  ASSERT_TRUE(p1 != NULL);
  EXPECT_GE(mem_mgr_->GetMemorySize(p1), 1024);
  EXPECT_LE(mem_mgr_->GetMemorySize(p1), 1032);
  EXPECT_EQ(0, memcmp(p1, pattern_one_1kb, 1016));
  void *p2 = mem_mgr_->GetMemory(1024);
  ASSERT_TRUE(p2 != NULL);
  EXPECT_GE(mem_mgr_->GetMemorySize(p2), 1024);
  EXPECT_LE(mem_mgr_->GetMemorySize(p2), 1032);
  EXPECT_EQ(0, memcmp(p2, pattern_one_1kb, 1016));
  memset(p1, 0, 1024);
  memset(p2, 0, 1024);
  EXPECT_EQ(0, memcmp(p2, p1, 1024));

  // Double space of p2 into new area
  p2 = mem_mgr_->xRealloc(p2, 2048);
  EXPECT_EQ(0, memcmp(p2, p1, 1024));
  EXPECT_EQ(0,
            memcmp(reinterpret_cast<char *>(p2) + 1024, pattern_one_1kb, 1016));
}


TEST_F(T_Sqlitemem, ReallocStress) {
  Prng prng;
  prng.InitSeed(42);
  vector<void *> ptrs;
  for (unsigned i = 0; i < 20000; ++i) {
    void *p = mem_mgr_->GetMemory(100 + prng.Next(50));
    ASSERT_TRUE(p != NULL);
    ptrs.push_back(p);
  }
  vector<void *> shuffled_ptrs = Shuffle(ptrs, &prng);
  for (unsigned i = 0; i < shuffled_ptrs.size(); ++i) {
    void *p = mem_mgr_->xRealloc(shuffled_ptrs[i], 150 + prng.Next(10));
    ASSERT_TRUE(p != NULL);
  }
}
