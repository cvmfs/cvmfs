/**
 * This file is part of the CernVM File System.
 */

#include "gtest/gtest.h"

#include "ingestion/tube.h"

using namespace std;  // NOLINT

namespace {
class DummyItem {
 public:
  DummyItem() : tag_(-1) { }
  int64_t tag() { return tag_; }
  int64_t tag_;
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
  Tube<DummyItem>::Link *l1 = tube_.EnqueueBack(&a);
  EXPECT_EQ(&a, l1->item());
  EXPECT_EQ(1U, tube_.size());
  EXPECT_FALSE(tube_.IsEmpty());
  Tube<DummyItem>::Link *l2 = tube_.EnqueueBack(&b);
  EXPECT_NE(l1, l2);
  tube_.EnqueueBack(&c);
  EXPECT_EQ(3U, tube_.size());

  DummyItem *x = tube_.Slice(l2);
  EXPECT_EQ(2U, tube_.size());
  EXPECT_EQ(&b, x);
  x = tube_.PopFront();
  EXPECT_EQ(1U, tube_.size());
  EXPECT_EQ(&a, x);
  x = tube_.PopFront();
  EXPECT_EQ(&c, x);
  EXPECT_TRUE(tube_.IsEmpty());
}


TEST_F(T_Tube, Group) {
  DummyItem a1, a2, b, c;
  a1.tag_ = a2.tag_ = 0;
  b.tag_ = 1;
  c.tag_ = 2;

  TubeGroup<DummyItem> grp1;
  Tube<DummyItem> *t1 = new Tube<DummyItem>();
  grp1.TakeTube(t1);
  grp1.Activate();
  grp1.Dispatch(&a1);
  grp1.Dispatch(&b);
  EXPECT_EQ(2U, t1->size());
  DummyItem *x = t1->PopFront();
  EXPECT_EQ(&a1, x);
  x = t1->PopFront();
  EXPECT_EQ(&b, x);

  TubeGroup<DummyItem> grp2;
  Tube<DummyItem> *t2 = new Tube<DummyItem>();
  Tube<DummyItem> *t3 = new Tube<DummyItem>();
  grp2.TakeTube(t2);
  grp2.TakeTube(t3);
  grp2.Activate();
  grp2.Dispatch(&a1);
  grp2.Dispatch(&b);
  grp2.Dispatch(&a2);
  grp2.Dispatch(&c);
  EXPECT_EQ(3U, t2->size());
  EXPECT_EQ(1U, t3->size());
  x = t2->PopFront();  EXPECT_EQ(&a1, x);
  x = t2->PopFront();  EXPECT_EQ(&a2, x);
  x = t2->PopFront();  EXPECT_EQ(&c, x);
  x = t3->PopFront();  EXPECT_EQ(&b, x);
}
