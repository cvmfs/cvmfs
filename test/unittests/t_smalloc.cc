#include <gtest/gtest.h>

#include "../../cvmfs/smalloc.h"
#include <cassert>
#define SIZE_MAX 18446744073709551615UL

TEST(smalloc, simpleTest) {
  void *p = smalloc(1000);
  EXPECT_TRUE(p != NULL);     
  EXPECT_EQ(NULL, malloc(SIZE_MAX));
}

TEST(scalloc, scalloc_simple) {
  size_t alloc_len = 100;
  char *ptr = (char *)scalloc(1, alloc_len);
  EXPECT_TRUE(ptr != NULL);
  free(ptr);
  EXPECT_EQ(NULL, scalloc(-5, 100));
  EXPECT_EQ(NULL, smalloc(SIZE_MAX));
}

 TEST(scalloc, scalloc_overflow) {
  EXPECT_EQ(NULL, calloc(SIZE_MAX, SIZE_MAX));
  EXPECT_EQ(NULL, calloc(2, SIZE_MAX));
  EXPECT_EQ(NULL, calloc(SIZE_MAX, 2));
 }
 
TEST(malloc, realloc) {
  char *p = (char*)smalloc(100);
  EXPECT_TRUE(p!=NULL);
  p = (char*)srealloc(p, 300);
  EXPECT_TRUE(p != NULL);
}
