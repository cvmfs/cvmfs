/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <inttypes.h>
#include <stdint.h>

#include <cassert>
#include <cstring>
#include <map>
#include <vector>

#include "malloc_heap.h"
#include "util/async.h"
#include "util/murmur.hxx"
#include "util/prng.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

class T_MallocHeap : public ::testing::Test {
 protected:
  static const unsigned kSmallArena = 8 * 1024 * 1024;
  static const unsigned kBigArena = 512 * 1024 * 1024;  // 512MB RAM Cache arena

  virtual void SetUp() { }

  virtual void TearDown() { }

  static uint32_t MemChecksum(void *p, uint32_t size) {
    return MurmurHash2(p, size, 0x07387a4f);
  }

  void FillRandomly(void *ptr, unsigned nbytes, Prng *prng) {
    assert(nbytes >= sizeof(uint32_t));
    nbytes -= sizeof(uint32_t);
    for (uint32_t i = 0; i <= nbytes; i += sizeof(uint32_t)) {
      uint32_t *target = reinterpret_cast<uint32_t *>(
          reinterpret_cast<unsigned char *>(ptr) + i);
      *target = prng->Next(0xFFFFFFFF);
    }
  }

  class CallbackNull : public Callbackable<MallocHeap::BlockPtr> {
   public:
    void Ignore(const MallocHeap::BlockPtr &ptr) { }
  };

  class IntMap : public Callbackable<MallocHeap::BlockPtr> {
   public:
    IntMap() : num_moves(0) { }

    struct Info {
      Info() : ptr(NULL), checksum(0) { }
      Info(void *p, uint32_t c) : ptr(p), checksum(c) { }
      void *ptr;
      uint32_t checksum;
    };

    void OnBlockMove(const MallocHeap::BlockPtr &new_ptr) {
      unsigned id = *reinterpret_cast<unsigned *>(new_ptr.pointer);
      mem_digest[id].ptr = new_ptr.pointer;
      num_moves++;
    }

    unsigned num_moves;
    map<unsigned, Info> mem_digest;
  };
};


TEST_F(T_MallocHeap, Basic) {
  CallbackNull cb_null;
  MallocHeap M(kSmallArena,
               cb_null.MakeCallback(&CallbackNull::Ignore, &cb_null));
  M.Compact();

  EXPECT_EQ(0U, M.compacted_bytes());

  vector<void *> pointers;

  const unsigned N = 1000;
  for (unsigned i = 1; i <= N; ++i) {
    unsigned header = i;
    void *p = M.Allocate(i + sizeof(header), &header, sizeof(header));
    EXPECT_TRUE(p != NULL);
    pointers.push_back(p);
  }
  uint64_t stored_bytes = M.stored_bytes();
  uint64_t used_bytes = M.used_bytes();
  EXPECT_GE(stored_bytes, (N * (N - 1) / 2) + (N * sizeof(unsigned)));
  EXPECT_GT(used_bytes, stored_bytes);
  M.Compact();
  EXPECT_EQ(stored_bytes, M.stored_bytes());
  EXPECT_EQ(used_bytes, M.used_bytes());
  EXPECT_LE(stored_bytes, M.compacted_bytes());
  EXPECT_LE(M.compacted_bytes(), used_bytes);

  for (unsigned i = 1; i <= N; ++i)
    M.MarkFree(pointers[i - 1]);
  EXPECT_EQ(0U, M.compacted_bytes());
  M.Compact();
  EXPECT_EQ(0U, M.used_bytes());
  EXPECT_EQ(0U, M.stored_bytes());
}


TEST_F(T_MallocHeap, FillToFull) {
  CallbackNull cb_null;
  MallocHeap M(kSmallArena,
               cb_null.MakeCallback(&CallbackNull::Ignore, &cb_null));

  unsigned header = 0;
  void *p = M.Allocate(7 + sizeof(header), &header, sizeof(header));
  EXPECT_TRUE(p != NULL);

  unsigned remaining_bytes = M.capacity() - M.compacted_bytes();
  EXPECT_GT(remaining_bytes, 8 + sizeof(header));
  p = M.Allocate(remaining_bytes - 7, &header, sizeof(header));
  EXPECT_EQ(NULL, p);
  p = M.Allocate(remaining_bytes - 8, &header, sizeof(header));
  EXPECT_TRUE(p != NULL);
}


TEST_F(T_MallocHeap, Stress) {
  IntMap int_map;
  MallocHeap M(kBigArena, int_map.MakeCallback(&IntMap::OnBlockMove, &int_map));
  Prng prng;
  prng.InitLocaltime();
  // 512M can host ~100000 4k + 8 bytes blocks
  unsigned N = 100000;
  unsigned max_size = 4096;
  for (unsigned i = 0; i < N; ++i) {
    unsigned size = prng.Next(max_size - 16) + 16;
    size = RoundUp8(size);
    void *ptr = M.Allocate(size, &i, sizeof(i));
    ASSERT_TRUE(ptr != NULL);
    FillRandomly(reinterpret_cast<unsigned char *>(ptr) + sizeof(i),
                 size - sizeof(i), &prng);
    int_map.mem_digest[i] = IntMap::Info(ptr, MemChecksum(ptr, size));
  }

  // Create random holes
  for (unsigned i = 0; i < N; ++i) {
    uint32_t coin = prng.Next(512);
    if (coin < 256)
      continue;
    M.MarkFree(int_map.mem_digest[i].ptr);
    int_map.mem_digest.erase(i);
  }
  EXPECT_LT(0U, M.num_blocks());
  EXPECT_LT(M.num_blocks(), N);
  // One more to ensure non-overlapping gauge
  unsigned next_gen = N + 1;
  void *ptr = M.Allocate(4096, &next_gen, sizeof(next_gen));
  ASSERT_TRUE(ptr != NULL);
  int_map.mem_digest[next_gen] = IntMap::Info(ptr, MemChecksum(ptr, 4096));

  M.Compact();
  EXPECT_EQ(int_map.mem_digest.size(), M.num_blocks());
  EXPECT_GT(int_map.num_moves, 0U);
  EXPECT_LE(int_map.num_moves, int_map.mem_digest.size());

  // Verify survivor blocks
  map<unsigned, IntMap::Info>::const_iterator iter = int_map.mem_digest.begin();
  map<unsigned, IntMap::Info>::const_iterator i_end = int_map.mem_digest.end();
  for (; iter != i_end; ++iter) {
    EXPECT_EQ(MemChecksum(iter->second.ptr, M.GetSize(iter->second.ptr)),
              iter->second.checksum);
  }
}


TEST_F(T_MallocHeap, Fill) {
  IntMap int_map;
  MallocHeap M(kSmallArena,
               int_map.MakeCallback(&IntMap::OnBlockMove, &int_map));
  unsigned elem_size = 4096 - 8;
  unsigned num_elems = kSmallArena / 4096;

  for (unsigned i = 0; i < num_elems; ++i) {
    void *ptr = M.Allocate(elem_size, &i, sizeof(i));
    EXPECT_TRUE(ptr != NULL);
    int_map.mem_digest[i] = IntMap::Info(ptr, 0);
  }
  void *ptr = M.Allocate(4096, &num_elems, sizeof(num_elems));
  EXPECT_EQ(NULL, ptr);

  for (unsigned i = 0; i < num_elems; i += 2) {
    M.MarkFree(int_map.mem_digest[i].ptr);
  }
  ptr = M.Allocate(4096, &num_elems, sizeof(num_elems));
  EXPECT_EQ(NULL, ptr);

  M.Compact();
  for (unsigned i = 0; i < num_elems; i += 2) {
    void *ptr = M.Allocate(elem_size, &i, sizeof(i));
    EXPECT_TRUE(ptr != NULL);
    int_map.mem_digest[i] = IntMap::Info(ptr, 0);
  }
  ptr = M.Allocate(4096, &num_elems, sizeof(num_elems));
  EXPECT_EQ(NULL, ptr);
}


TEST_F(T_MallocHeap, HasSpaceFor) {
  IntMap int_map;
  MallocHeap M(kSmallArena,
               int_map.MakeCallback(&IntMap::OnBlockMove, &int_map));
  EXPECT_TRUE(M.HasSpaceFor(1));
  EXPECT_FALSE(M.HasSpaceFor(kSmallArena));
  EXPECT_TRUE(M.HasSpaceFor(kSmallArena - 8));

  unsigned elem_size = 4096 - 8;
  unsigned num_elems = kSmallArena / 4096;

  for (unsigned i = 0; i < num_elems; ++i) {
    EXPECT_TRUE(M.HasSpaceFor(elem_size));
    void *ptr = M.Allocate(elem_size, &i, sizeof(i));
    EXPECT_TRUE(ptr != NULL);
    int_map.mem_digest[i] = IntMap::Info(ptr, 0);
  }
  EXPECT_FALSE(M.HasSpaceFor(elem_size));

  M.MarkFree(int_map.mem_digest[0].ptr);
  EXPECT_FALSE(M.HasSpaceFor(elem_size));
  M.Compact();
  EXPECT_TRUE(M.HasSpaceFor(elem_size));
}


TEST_F(T_MallocHeap, Expand) {
  IntMap int_map;
  MallocHeap M(kSmallArena,
               int_map.MakeCallback(&IntMap::OnBlockMove, &int_map));
  unsigned i = 0;
  void *ptr = M.Allocate(8, &i, sizeof(i));
  EXPECT_TRUE(ptr != NULL);

  void *ptr2 = M.Expand(ptr, 8);
  EXPECT_TRUE(ptr2 != NULL);
  EXPECT_NE(ptr, ptr2);
  EXPECT_EQ(0, memcmp(ptr, ptr2, 8));

  void *ptr3 = M.Expand(ptr2, 16);
  EXPECT_TRUE(ptr3 != NULL);
  EXPECT_NE(ptr2, ptr3);
  EXPECT_EQ(0, memcmp(ptr2, ptr3, 8));

  EXPECT_DEATH(M.Expand(ptr, 4), ".*");
}
