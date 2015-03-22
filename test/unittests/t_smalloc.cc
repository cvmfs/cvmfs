/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/smalloc.h"

// Debug switcher
// #define SMALLOC_DEBUG_

// Max size is 10MB
static const unsigned int max_size = 1024u * 1024u * 10u;

// Test smalloc
TEST(T_Smalloc, Smalloc) {
	/* we need not test size 0 because smalloc returns either NULL, or a
	 * unique pointer value that can later be successfully passed to free.
	 * The largest malloc size is depending on the specific OS and
	 * configuration so here we set 10MB is the largest test size.
	 * */
  for (unsigned int size = 1u; size <= max_size; size = size * 512u) {
#ifdef SMALLOC_DEBUG_
    printf("size is %u.\n", size);
#endif
    void *mem = smalloc(size);
    EXPECT_TRUE(mem != NULL);
    free(mem);
  }
}

// Test srealloc
TEST(T_Smalloc, Srealloc) {
	/* Srealloc(ptr, size) changes the size of memory block pointed to ptr to size
	 * bytes. Size may be larger or smaller than old size. If ptr is NULL,
	 * then it is the same as smalloc(). If size is zero and ptr is not
	 * NULL, it is equivalent to free().*/
  for (unsigned int size = 1u; size <= max_size; size = size * 1024) {
#ifdef SMALLOC_DEBUG_
    printf("size is %u.\n", size);
#endif
    // Test ptr is NULL
    void *mem = srealloc(NULL, size);
    EXPECT_TRUE(mem != NULL);
#ifdef SMALLOC_DEBUG_
    printf("999 %p\n", mem);
#endif
    // Test size is larger than old size
    mem = srealloc(mem, size + 1u);
    EXPECT_TRUE(mem != NULL);
#ifdef SMALLOC_DEBUG_
    printf("999 %p\n", mem);
#endif
    // Test size is smaller than old size
    if (size != 1u) {
			/*TODO: When size is 1u, srealloc would be asserted inside
			 * beecause it is not allowed the second parameter of srealloc is
       * zero. However, standard realloc(ptr, _size) permits _size is zero.
			 * When _size is zero, it is free. Therefore, this maybe a bug for
			 * current smalloc*/
      mem = srealloc(mem, size - 1u);
      EXPECT_TRUE(mem != NULL);
    }
#ifdef SMALLOC_DEBUG_
    printf("999 %p\n", mem);
#endif
    free(mem);
  }
}

// Test scalloc
TEST(T_Smalloc, Scalloc) {
	/* Scalloc(nmemb, size) allocates memory space for an array of nmemb elements of
	 * size bytes each. If nmemb or size is 0, then returns NULL or a
	 * unique pointer value. We cannot test the condition of nmemb or size
	 * is 0.
	 */
  for (unsigned int size = 1u, num = 1u; size <= max_size; num++) {
#ifdef SMALLOC_DEBUG_
    printf("size is %u.\n", size);
    printf("num is %u.\n", num);
#endif
    int *mem = static_cast<int *>(scalloc(num, size));
    EXPECT_FALSE(mem == NULL);
    for (const int *tmp = mem; tmp < num * size; tmp++) {
      EXPECT_EQ(*tmp, 0);
    }
    free(mem);
    size = size * 512u;
  }
}

// Test smmap and smunmap
TEST(T_Smalloc, SmmapAndSunmap) {
  for (unsigned int size = sizeof(size_t) * 2u; size <= 1024u * 1024u; ) {
#ifdef SMALLOC_DEBUG_
    printf("size is %u.\n", size);
#endif
    size_t pages = ((size + 2 * sizeof(size_t)) + 4095) / 4096;
    void *mem = smmap(size);
    EXPECT_FALSE(mem == NULL);
    unsigned char *area = static_cast<unsigned char *>(mem);
    area = area - sizeof(size_t);
    size_t _pages = *(reinterpret_cast<size_t *>(area));
    EXPECT_EQ(_pages, pages);
    area = area - sizeof(size_t);
    EXPECT_EQ(*(reinterpret_cast<size_t *>(area)), kMemMarker);
    smunmap(mem);
    size = size + 4096u;
  }
}

