/**
 * This file is part of the CernVM File System.
 */

#include "gtest/gtest.h"

#include "../../cvmfs/smalloc.h"

/**
 * Tests for smalloc
 *
 * According to the specifications, malloc(0) will return either
 * "a null pointer or a unique pointer that can be successfully
 * passed to free()", so we cannot test it properly.
 * Due to our 'smalloc' implementation we cannot test it on 0, too.
 * 
 * Also we cannot properly test 'smalloc' on the largest size of 
 * memory that it could handle.
 * It depends on the platform that cvmfs is working on.
 */

TEST(T_Smalloc, SmallSizeAllocation) {
  void* p = smalloc(sizeof(int));
  EXPECT_TRUE(NULL != p);
  free(p);
}

TEST(T_Smalloc, LargeSizeAllocation) {
  void* p = smalloc(4194304);  // ~4MB
  EXPECT_TRUE(NULL != p);
  free(p);
}

/**
 * Can't be tested here because of assertions in smalloc implementation
 *
 * TEST(T_Smalloc, NegativeSizeAllocation) {
 *   EXPECT_TRUE(NULL == smalloc(-sizeof(int)));
 * } 
 */


/** Tests for srealloc */
TEST(T_Srealloc, ReallocationToLargerSize) {
  void* p = smalloc(sizeof(int));
  ASSERT_TRUE(NULL != p);
  p = srealloc(p, 100 * sizeof(int));
  EXPECT_TRUE(NULL != p);
  free(p);
}

TEST(T_Srealloc, RealocationToSmallerSize) {
  void* p = smalloc(100 * sizeof(int));
  ASSERT_TRUE(NULL != p);
  p = srealloc(p, sizeof(int));
  EXPECT_TRUE(NULL != p);
  free(p);
}

TEST(T_Srealloc, ReallocationOfNullPtr) {
  void* p = NULL;
  p = srealloc(p, sizeof(int));
  EXPECT_TRUE(NULL != p);
  free(p);
}

/**
 * Can't be tested here because of assertions in srealloc implementation
 *
 * TEST(T_Srealloc, ZeroSizeReallocation) {
 *   void* p = smalloc(sizeof(int));
 *   ASSERT_TRUE(NULL != p);
 *   p = srealloc(p, 0);
 *   EXPECT_TRUE(NULL == p);
 * }
 */


/** Tests for scalloc */
TEST(T_Scalloc, Callocation) {
  int* p = static_cast<int*>(scalloc(100, sizeof(int)));
  ASSERT_TRUE(NULL != p);

  for (const int* t = p; t < p + 100; ++t) {
    ASSERT_EQ(0, *t);
  }
  free(p);
}


/** Tests for smmap */
TEST(T_Smmap, Mmapping) {
  size_t size = 10;
  char* mem = static_cast<char*>(smmap(size));
  ASSERT_TRUE(MAP_FAILED != mem);

  char* p = mem - 2 * sizeof(size_t);
  EXPECT_EQ(0xAAAAAAAA, *(reinterpret_cast<size_t*>(p)));

  size_t pages = ((size + 2 * sizeof(size_t)) + 4095) / 4096;
  EXPECT_EQ(pages, *(reinterpret_cast<size_t*>(p) + 1));
}


/**
 * Tests for smunmap
 *
 * There's nothing we can test now.
 */
