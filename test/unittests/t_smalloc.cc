#include <gtest/gtest.h>
#include <stdlib.h>

#include "../../cvmfs/smalloc.h"

class T_Smalloc : public ::testing::Test {
	protected: 
		virtual void Setup() {
	static const unsigned kNumSmall = 10;
    static const unsigned kNumBig = 10000000
	}
	}


TEST_F(T_Smalloc,Test_smalloc) {
unsigned N = kNumSmall;
  for (unsigned i = 0; i < N; ++i)
  {
   	void *r_mem = smalloc(N);
  	EXPECT_EQ(r_mem, NULL);
  	
  }
  
  EXPECT_EQ(smalloc(10000000000, NULL);
  EXPECT_EQ(smalloc(8),NULL);
  }





