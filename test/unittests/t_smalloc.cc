/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <stdlib.h>

#include "../../cvmfs/smalloc.h"

class T_Smalloc : public ::testing::Test {
 protected:
  virtual void SetUp() {
}
  virtual void TearDown() {
}
};


TEST_F(T_Smalloc, Test_positive) {
EXPECT_TRUE(smalloc(0) != NULL);
EXPECT_TRUE(smalloc(100) != NULL);
EXPECT_TRUE(smalloc(1024) != NULL);
}

TEST_F(T_Smalloc, memory_overload) {
ASSERT_DEATH(smalloc(100000000000000), "Out Of Memory");
}

TEST_F(T_Smalloc, srealloc_testing) {
void *r_mem = smalloc(0);
EXPECT_TRUE(srealloc(r_mem, 1000000) != NULL);
}

TEST_F(T_Smalloc, srealloc_testing_death) {
void *r_mem = smalloc(1024);
ASSERT_DEATH(srealloc(r_mem, 10000000000000), "Out Of Memory");
}

TEST_F(T_Smalloc, scalloc_testing) {
EXPECT_TRUE(scalloc(1024, 1000/1024) != NULL);
}

TEST_F(T_Smalloc, scalloc_testing_death) {
int64 test_size_ = 100000000000000;
ASSERT_DEATH(scalloc(1024, test_size_ / 1024L), "Out Of Memory");
}
