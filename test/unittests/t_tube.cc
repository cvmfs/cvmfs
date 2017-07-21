/**
 * This file is part of the CernVM File System.
 */

#include "gtest/gtest.h"

#include "ingestion/tube.h"

using namespace std;  // NOLINT

namespace {
class DummyItem {
};
}

class T_Tube : public ::testing::Test {
 protected:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }

  Tube<DummyItem> tube_;
};


TEST_F(T_Tube, Fifo) {
  DummyItem a, b, c;
  EXPECT_EQ(0U, tube_.size());
  EXPECT_TRUE(tube_.IsEmpty());
  Tube<DummyItem>::Link *l1 = tube_.Enqueue(&a);
  EXPECT_EQ(&a, l1->item());
  EXPECT_EQ(1U, tube_.size());
  EXPECT_FALSE(tube_.IsEmpty());
  Tube<DummyItem>::Link *l2 = tube_.Enqueue(&b);
  EXPECT_NE(l1, l2);
  tube_.Enqueue(&c);
  EXPECT_EQ(3U, tube_.size());

  DummyItem *x = tube_.Slice(l2);
  EXPECT_EQ(2U, tube_.size());
  EXPECT_EQ(&b, x);
  x = tube_.Pop();
  EXPECT_EQ(1U, tube_.size());
  EXPECT_EQ(&a, x);
  x = tube_.Pop();
  EXPECT_EQ(&c, x);
  EXPECT_TRUE(tube_.IsEmpty());
}
