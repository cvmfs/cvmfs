#include <gtest/gtest.h>

#include "../../cvmfs/smalloc.h"

#include <cassert>

TEST(smalloc, simpleTest) {
  void *p = smalloc(1000);
  ASSERT_TRUE(p != NULL);     
  ASSERT_EQ(NULL, malloc(SIZE_MAX));
}

TEST(scalloc, scalloc_simple) {
  size_t alloc_len = 100;
  char *ptr = (char *)scalloc(1, alloc_len);
  ASSERT_TRUE(ptr != NULL);
  free(ptr);
  ASSERT_EQ(NULL, scalloc(-5, 100));
  ASSERT_EQ(NULL, smalloc(SIZE_MAX));
}

 TEST(scalloc, scalloc_overflow) {
  ASSERT_EQ(NULL, calloc(SIZE_MAX, SIZE_MAX));
  ASSERT_EQ(NULL, calloc(2, SIZE_MAX));
  ASSERT_EQ(NULL, calloc(SIZE_MAX, 2));
 }
 
TEST(malloc, realloc) {
  char *p = (char*)smalloc(100);
  ASSERT_TRUE(p!=NULL);
  p = (char*)srealloc(p, 300);
  ASSERT_TRUE(p != NULL);
  ASSERT_EQ(NULL, srealloc(NULL, SIZE_MAX));
  ASSERT_EQ(NULL, srealloc(p, SIZE_MAX));
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
