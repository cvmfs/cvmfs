/**
 * This file is part of the CernVM File System.
 */


#include <gtest/gtest.h>
#include <limits>

#include "../../cvmfs/smalloc.h"
#include "../../cvmfs/util.h"


const size_t kSmallAllocation = 1024UL;
const size_t kBigAllocation = std::numeric_limits<size_t>::max();

TEST(T_Smalloc, SmallAlloc) {
  void *mem = smalloc(kSmallAllocation);
  EXPECT_NE(static_cast<void*>(NULL), mem);
  free(mem);
}

TEST(T_Smalloc, BigAlloc) {
  void *mem = NULL;
  EXPECT_DEATH(mem = smalloc(kBigAllocation), ".*");
  EXPECT_EQ(static_cast<void*>(NULL), mem);
}

TEST(T_Smalloc, SmallRealloc) {
  void *mem = smalloc(kSmallAllocation);
  mem = srealloc(mem, kSmallAllocation * 2);
  EXPECT_NE(static_cast<void*>(NULL), mem);
  mem = srealloc(mem, kSmallAllocation / 2);
  EXPECT_NE(static_cast<void*>(NULL), mem);
  free(mem);
}

TEST(T_Smalloc, BigRealloc) {
  UniquePtr<void> mem(smalloc(kSmallAllocation));
  ASSERT_DEATH(mem = srealloc(mem, kBigAllocation), ".*");
  EXPECT_NE(static_cast<void*>(NULL), mem.weak_ref());
}

TEST(T_Smalloc, SmallCalloc) {
  int size = sizeof(char);
  void *mem = scalloc(kSmallAllocation, size);
  EXPECT_NE(static_cast<void*>(NULL), mem);
  free(mem);
}

TEST(T_Smalloc, BigCalloc) {
  int size = sizeof(char);
  void *mem = NULL;
  ASSERT_DEATH(mem = scalloc(kBigAllocation, size), ".*");
  EXPECT_EQ(static_cast<void*>(NULL), mem);
}

TEST(T_Smalloc, SmallMmap) {
  void *mem = smmap(kSmallAllocation);
  EXPECT_NE(MAP_FAILED, mem);
  size_t *details = reinterpret_cast<size_t*>(mem);
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
