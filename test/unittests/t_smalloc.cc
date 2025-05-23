/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <limits>
#include <vector>

#include "util/pointer.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

const size_t kSmallAllocation = 1024UL;
const size_t kBigAllocation = std::numeric_limits<size_t>::max();

TEST(T_Smalloc, SmallAlloc) {
  void *mem = smalloc(kSmallAllocation);
  EXPECT_NE(static_cast<void *>(NULL), mem);
  free(mem);
}

TEST(T_Smalloc, BigAlloc) {
  void *mem = NULL;
  EXPECT_DEATH(mem = smalloc(kBigAllocation), ".*");
  EXPECT_EQ(static_cast<void *>(NULL), mem);
}

TEST(T_Smalloc, SmallRealloc) {
  void *mem = smalloc(kSmallAllocation);
  mem = srealloc(mem, kSmallAllocation * 2);
  EXPECT_NE(static_cast<void *>(NULL), mem);
  mem = srealloc(mem, kSmallAllocation / 2);
  EXPECT_NE(static_cast<void *>(NULL), mem);
  free(mem);
}

TEST(T_Smalloc, BigRealloc) {
  UniquePtr<void> mem(smalloc(kSmallAllocation));
  ASSERT_DEATH(mem = srealloc(mem.weak_ref(), kBigAllocation), ".*");
  EXPECT_NE(static_cast<void *>(NULL), mem.weak_ref());
}

TEST(T_Smalloc, SmallCalloc) {
  int size = sizeof(char);
  void *mem = scalloc(kSmallAllocation, size);
  EXPECT_NE(static_cast<void *>(NULL), mem);
  free(mem);
}

TEST(T_Smalloc, BigCalloc) {
  int size = sizeof(char);
  void *mem = NULL;
  ASSERT_DEATH(mem = scalloc(kBigAllocation, size), ".*");
  EXPECT_EQ(static_cast<void *>(NULL), mem);
}

TEST(T_Smalloc, SmallMmap) {
  void *mem = smmap(kSmallAllocation);
  EXPECT_NE(MAP_FAILED, mem);
  size_t *details = reinterpret_cast<size_t *>(mem);
  size_t pages = ((kSmallAllocation + 2 * sizeof(size_t)) + 4095) / 4096;
  EXPECT_EQ(pages, *(details - 1));
  EXPECT_EQ(0xAAAAAAAA, *(details - 2));
  smunmap(mem);
  EXPECT_NE(MAP_FAILED, mem);
}

TEST(T_Smalloc, BigMmap) {
  void *mem = NULL;
  ASSERT_DEATH(mem = smmap(kBigAllocation - 8192), ".*Out Of Memory.*");
  ASSERT_DEATH(smunmap(mem), ".*");
}


TEST(T_Smalloc, Xmmap) {
  void *mem = NULL;
  mem = sxmmap(1);
  EXPECT_TRUE(mem != NULL);
  sxunmap(mem, 1);
  mem = NULL;

  mem = sxmmap(8192);
  EXPECT_TRUE(mem != NULL);
  sxunmap(mem, 8192);
  mem = NULL;
}


TEST(T_Smalloc, XmmapAlign) {
  vector<size_t> sizes;
  sizes.push_back(2 * 1024 * 1024);
  sizes.push_back(4 * 1024 * 1024);
  sizes.push_back(6 * 1024 * 1024);
  sizes.push_back(8 * 1024 * 1024);
  sizes.push_back(10 * 1024 * 1024);
  sizes.push_back(12 * 1024 * 1024);
  sizes.push_back(14 * 1024 * 1024);
  sizes.push_back(16 * 1024 * 1024);

  for (unsigned i = 0; i < 25; ++i) {
    for (unsigned j = 0; j < sizes.size(); ++j) {
      void *p = sxmmap_align(sizes[j]);
      EXPECT_EQ(0U, uintptr_t(p) % sizes[j]);
      memset(p, 0, sizes[j]);
      sxunmap(p, sizes[j]);
    }
  }
}


TEST(T_Smalloc, Null) {
  // Don't crash
  free(smalloc(0));
  free(scalloc(0, 1));
  free(scalloc(1, 0));
  free(srealloc(NULL, 0));
}
