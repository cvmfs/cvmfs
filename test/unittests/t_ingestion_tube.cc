/**
 * This file is part of the CernVM File System.
 */

#include <string>

#include "gtest/gtest.h"
#include "util/tube.h"

using namespace std;  // NOLINT

class T_Ingestion_Tube : public ::testing::Test {
 protected:
  virtual void SetUp() {
    s1 = "s1";
    s2 = "s2";
    s3 = "s3";
    s4 = "s4";
  }
  Tube<string> tube_;
  string s1;
  string s2;
  string s3;
  string s4;
};

TEST_F(T_Ingestion_Tube, Queue) {
  tube_.EnqueueBack(&s1);
  tube_.EnqueueBack(&s2);
  EXPECT_EQ(&s1, tube_.PopFront());
  tube_.EnqueueBack(&s3);
  tube_.EnqueueBack(&s2);
  EXPECT_EQ(3U, tube_.size());
  EXPECT_EQ(&s2, tube_.PopFront());
  EXPECT_EQ(&s3, tube_.PopFront());
  EXPECT_EQ(&s2, tube_.PopFront());
  EXPECT_TRUE(tube_.IsEmpty());
  tube_.EnqueueBack(&s4);
  tube_.EnqueueBack(&s1);
  EXPECT_EQ(&s4, tube_.PopFront());
  EXPECT_EQ(&s1, tube_.PopFront());
  EXPECT_TRUE(tube_.IsEmpty());
}

TEST_F(T_Ingestion_Tube, InvertedQueue) {
  tube_.EnqueueFront(&s1);
  tube_.EnqueueFront(&s2);
  EXPECT_EQ(&s1, tube_.PopBack());
  tube_.EnqueueFront(&s3);
  tube_.EnqueueFront(&s2);
  EXPECT_EQ(3U, tube_.size());
  EXPECT_EQ(&s2, tube_.PopBack());
  EXPECT_EQ(&s3, tube_.PopBack());
  EXPECT_EQ(&s2, tube_.PopBack());
  EXPECT_TRUE(tube_.IsEmpty());
  tube_.EnqueueFront(&s4);
  tube_.EnqueueFront(&s1);
  EXPECT_EQ(&s4, tube_.PopBack());
  EXPECT_EQ(&s1, tube_.PopBack());
  EXPECT_TRUE(tube_.IsEmpty());
}

TEST_F(T_Ingestion_Tube, Stack) {
  tube_.EnqueueFront(&s1);
  tube_.EnqueueFront(&s2);
  EXPECT_EQ(&s2, tube_.PopFront());
  EXPECT_EQ(&s1, tube_.PopFront());
  EXPECT_TRUE(tube_.IsEmpty());
  tube_.EnqueueFront(&s3);
  tube_.EnqueueFront(&s2);
  tube_.EnqueueFront(&s4);
  EXPECT_EQ(&s4, tube_.PopFront());
  tube_.EnqueueFront(&s1);
  EXPECT_EQ(3U, tube_.size());
  EXPECT_EQ(&s1, tube_.PopFront());
  EXPECT_EQ(&s2, tube_.PopFront());
  EXPECT_EQ(&s3, tube_.PopFront());
  EXPECT_TRUE(tube_.IsEmpty());
}

TEST_F(T_Ingestion_Tube, InvertedStack) {
  tube_.EnqueueBack(&s1);
  tube_.EnqueueBack(&s2);
  EXPECT_EQ(&s2, tube_.PopBack());
  EXPECT_EQ(&s1, tube_.PopBack());
  EXPECT_TRUE(tube_.IsEmpty());
  tube_.EnqueueBack(&s3);
  tube_.EnqueueBack(&s2);
  tube_.EnqueueBack(&s4);
  EXPECT_EQ(&s4, tube_.PopBack());
  tube_.EnqueueBack(&s1);
  EXPECT_EQ(3U, tube_.size());
  EXPECT_EQ(&s1, tube_.PopBack());
  EXPECT_EQ(&s2, tube_.PopBack());
  EXPECT_EQ(&s3, tube_.PopBack());
  EXPECT_TRUE(tube_.IsEmpty());
}

TEST_F(T_Ingestion_Tube, Deque) {
  tube_.EnqueueBack(&s3);
  tube_.EnqueueBack(&s2);
  tube_.EnqueueFront(&s2);
  tube_.EnqueueBack(&s1);
  tube_.EnqueueFront(&s1);
  EXPECT_EQ(5U, tube_.size());
  EXPECT_EQ(&s1, tube_.PopFront());
  EXPECT_EQ(&s2, tube_.PopFront());
  EXPECT_EQ(&s1, tube_.PopBack());
  EXPECT_EQ(&s3, tube_.PopFront());
  EXPECT_EQ(&s2, tube_.PopFront());
  EXPECT_TRUE(tube_.IsEmpty());
}

TEST_F(T_Ingestion_Tube, QueueDeferItem) {
  tube_.EnqueueBack(&s1);
  tube_.EnqueueBack(&s2);
  Tube<string>::Link *to_defer = tube_.EnqueueBack(&s3);
  tube_.EnqueueBack(&s4);
  EXPECT_EQ(&s1, tube_.PopFront());
  EXPECT_EQ(&s3, tube_.Slice(to_defer));
  tube_.EnqueueBack(&s3);
  EXPECT_EQ(&s2, tube_.PopFront());
  EXPECT_EQ(&s4, tube_.PopFront());
  EXPECT_EQ(&s3, tube_.PopFront());
  EXPECT_TRUE(tube_.IsEmpty());
}
