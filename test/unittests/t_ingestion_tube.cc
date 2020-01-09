/**
 * This file is part of the CernVM File System.
 */

#include "gtest/gtest.h"

#include <string>

#include "ingestion/tube.h"

using namespace std;  // NOLINT

class T_Ingestion_Tube : public ::testing::Test {
 protected:
  Tube<string> tube_;
  string s1 = "s1";
  string s2 = "s2";
  string s3 = "s3";
  string s4 = "s4";
};

TEST_F(T_Ingestion_Tube, Queue) {
  tube_.Enqueue(&s1);
  tube_.Enqueue(&s2);
  EXPECT_EQ(&s1, tube_.Pop());
  tube_.Enqueue(&s3);
  tube_.Enqueue(&s2);
  EXPECT_EQ(3U, tube_.size());
  EXPECT_EQ(&s2, tube_.Pop());
  EXPECT_EQ(&s3, tube_.Pop());
  EXPECT_EQ(&s2, tube_.Pop());
  EXPECT_TRUE(tube_.IsEmpty());
  tube_.Enqueue(&s4);
  tube_.Enqueue(&s1);
  EXPECT_EQ(&s4, tube_.Pop());
  EXPECT_EQ(&s1, tube_.Pop());
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
  EXPECT_EQ(&s2, tube_.Pop());
  EXPECT_EQ(&s1, tube_.Pop());
  EXPECT_TRUE(tube_.IsEmpty());
  tube_.EnqueueFront(&s3);
  tube_.EnqueueFront(&s2);
  tube_.EnqueueFront(&s4);
  EXPECT_EQ(&s4, tube_.Pop());
  tube_.EnqueueFront(&s1);
  EXPECT_EQ(3U, tube_.size());
  EXPECT_EQ(&s1, tube_.Pop());
  EXPECT_EQ(&s2, tube_.Pop());
  EXPECT_EQ(&s3, tube_.Pop());
  EXPECT_TRUE(tube_.IsEmpty());
}

TEST_F(T_Ingestion_Tube, InvertedStack) {
  tube_.Enqueue(&s1);
  tube_.Enqueue(&s2);
  EXPECT_EQ(&s2, tube_.PopBack());
  EXPECT_EQ(&s1, tube_.PopBack());
  EXPECT_TRUE(tube_.IsEmpty());
  tube_.Enqueue(&s3);
  tube_.Enqueue(&s2);
  tube_.Enqueue(&s4);
  EXPECT_EQ(&s4, tube_.PopBack());
  tube_.Enqueue(&s1);
  EXPECT_EQ(3U, tube_.size());
  EXPECT_EQ(&s1, tube_.PopBack());
  EXPECT_EQ(&s2, tube_.PopBack());
  EXPECT_EQ(&s3, tube_.PopBack());
  EXPECT_TRUE(tube_.IsEmpty());
}

TEST_F(T_Ingestion_Tube, Deque) {
  tube_.Enqueue(&s3);
  tube_.Enqueue(&s2);
  tube_.EnqueueFront(&s2);
  tube_.Enqueue(&s1);
  tube_.EnqueueFront(&s1);
  EXPECT_EQ(5U, tube_.size());
  EXPECT_EQ(&s1, tube_.Pop());
  EXPECT_EQ(&s2, tube_.Pop());
  EXPECT_EQ(&s1, tube_.PopBack());
  EXPECT_EQ(&s3, tube_.Pop());
  EXPECT_EQ(&s2, tube_.Pop());
  EXPECT_TRUE(tube_.IsEmpty());
}

TEST_F(T_Ingestion_Tube, QueueDeferItem) {
  tube_.Enqueue(&s1);
  tube_.Enqueue(&s2);
  Tube<string>::Link *to_defer = tube_.Enqueue(&s3);
  tube_.Enqueue(&s4);
  EXPECT_EQ(&s1, tube_.Pop());
  EXPECT_EQ(&s3, tube_.Slice(to_defer));
  tube_.Enqueue(&s3);
  EXPECT_EQ(&s2, tube_.Pop());
  EXPECT_EQ(&s4, tube_.Pop());
  EXPECT_EQ(&s3, tube_.Pop());
  EXPECT_TRUE(tube_.IsEmpty());
}
