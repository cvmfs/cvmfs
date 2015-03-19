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
 void *mem = smalloc(1024);//allocating 1kB of memory
 EXPECT_FALSE(mem==NULL); //successful allocation
 EXPECT_EQ(NULL, smalloc(BIGNO)); //failed allocation,
 free(mem);
}
 
TEST_F(T_Srealloc, Basics) {
 void *mem =smalloc(1024); //allocate memory
 EXPECT_FALSE(p==NULL); //successful allocation of memory
 mem = srealloc(mem, 512); //reallocate memory
 EXPECT_FALSE(mem==NULL); //successful reallocation of memory
 free(mem);
}
 
TEST_F(T_Scalloc, Basics) {
 size_t size=128; 
 int *mem=static_cast<int*>(scalloc(4, sizeof(int));
 EXPECT_FALSE(mem==NULL);//succcesful allocation of 4 elements of size equal to int
 EXPECT_TRUE(NULL==smalloc(-2,BIGNO));//failed allocation, overflow and negative items
 EXPECT_TRUE(NULL==smalloc((rand()%10)*BIGNO , (rand()%5)*BIGNO);//failed allocation, overflow
 free(mem);
}
