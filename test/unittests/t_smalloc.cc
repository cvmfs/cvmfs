/**
 * This file is part of the CernVM File System.
 */
#include <gtest/gtest.h>
 
#include <stdlib.h>
 
#include "../../cvmfs/smalloc.h"
 
#define BIGNO 98489748654818644685
 
class T_Smalloc : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }
};
 
TEST_F(T_Smalloc, Basics) {
 void *mem = smalloc(1024);
 EXPECT_FALSE(mem==NULL); 
 EXPECT_EQ(NULL, smalloc(BIGNO));
 free(mem);
}
 
TEST_F(T_Srealloc, Basics) {
 void *mem =smalloc(1024); 
 EXPECT_FALSE(p==NULL); 
 mem = srealloc(mem, 512);
 EXPECT_FALSE(mem==NULL); 
 free(mem);
}
 
TEST_F(T_Scalloc, Basics) {
 size_t size=128; 
 int *mem=static_cast<int*>(scalloc(4, sizeof(int));
 EXPECT_FALSE(mem==NULL);
 EXPECT_TRUE(NULL==smalloc(-2,BIGNO));
 EXPECT_TRUE(NULL==smalloc((rand()%10)*BIGNO , (rand()%5)*BIGNO);
 free(mem);
}
