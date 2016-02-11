/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <vector>

#include "../../cvmfs/prng.h"
#include "../../cvmfs/sqlitemem.h"
#include "../../cvmfs/util.h"

using namespace std;

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

}  // namespace sqlite
