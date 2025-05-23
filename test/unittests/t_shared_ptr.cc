/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "util/shared_ptr.h"
#include "util/single_copy.h"

class ClassSharedMember {
 public:
  ClassSharedMember(int *counter, int value)
      : counter_(counter), value_(new int(value)) {
    (*counter_)++;
  }
  ~ClassSharedMember() { (*counter_)--; }

  // Need to defined special copy-constructor in order to increment counter_...
  ClassSharedMember(const ClassSharedMember &other)
      : counter_(other.counter_), value_(other.value_) {
    (*counter_)++;
  }

  int value_use_count() const { return value_.UseCount(); }

 private:
  int *counter_;
  SharedPtr<int> value_;
};

// Test class that allows tracking the number of instances
class TestData {
 public:
  explicit TestData(int *val) : val_(val) { (*val_)++; }
  ~TestData() { (*val_)--; }

 private:
  int *val_;
};

class T_SharedPtr : public ::testing::Test {
 protected:
  // At the beginning of each test case, there should be no living instances of
  // TestData
  void SetUp() {
    counter1_ = 0;
    counter2_ = 0;
  }
  // After each test case, there should be no more living instances of TestData.
  void TearDown() {
    ASSERT_EQ(counter1_, 0);
    ASSERT_EQ(counter2_, 0);
  }

  int counter1_;
  int counter2_;
};

// Basic usage of shared_ptr
TEST_F(T_SharedPtr, SharedPtr) {
  // New instance of TestData managed by shared_ptr
  SharedPtr<TestData> shared_data1(new TestData(&counter1_));
  // Counter should indicated the new instance
  EXPECT_EQ(counter1_, 1);
  // The reference should be unique at this point
  EXPECT_TRUE(shared_data1.Unique());
  {
    // Make a new reference to the existing TestData instance
    SharedPtr<TestData> shared_data2 = shared_data1;
    // Counter hasn't been incremented, since we are still dealing with a single
    // TestData instance
    EXPECT_EQ(counter1_, 1);
    // The shared_ptr reference is no longer unique...
    EXPECT_FALSE(shared_data1.Unique());
    // And the use count of the shared references is 2
    EXPECT_EQ(shared_data1.UseCount(), 2);
    EXPECT_EQ(shared_data2.UseCount(), 2);
  }
  // shared_data2 has been destroyed. The reference is again unique.
  EXPECT_TRUE(shared_data1.Unique());
}

// Working with classes which contain shared_ptr members
TEST_F(T_SharedPtr, SharedPtrMembers) {
  // Create an instance of ClassSharedMember
  ClassSharedMember data1(&counter1_, 1234);
  EXPECT_EQ(counter1_, 1);

  {
    // Create a new instance of ClassSharedMember using the copy-constructor
    ClassSharedMember data2(data1);
    EXPECT_EQ(counter1_, 2);
    // The shared reference has 2 uses, by data1 and data2
    EXPECT_EQ(data1.value_use_count(), 2);
  }

  // data2 has been destroyed.
  EXPECT_EQ(counter1_, 1);
  EXPECT_EQ(data1.value_use_count(), 1);
}

TEST_F(T_SharedPtr, Resetting) {
  // New instance of TestData managed by shared_ptr
  SharedPtr<TestData> shared_data1(new TestData(&counter1_));
  // Counter should indicated the new instance
  EXPECT_EQ(counter1_, 1);
  // The reference should be unique at this point
  EXPECT_TRUE(shared_data1.Unique());

  SharedPtr<TestData> shared_data2(shared_data1);

  EXPECT_EQ(1, counter1_);
  EXPECT_FALSE(shared_data1.Unique());
  EXPECT_EQ(2, shared_data1.UseCount());

  shared_data1.Reset();

  EXPECT_EQ(1, counter1_);
  EXPECT_TRUE(shared_data2.Unique());
  EXPECT_EQ(1, shared_data2.UseCount());

  shared_data1 = shared_data2;

  EXPECT_EQ(1, counter1_);
  EXPECT_FALSE(shared_data1.Unique());
  EXPECT_EQ(2, shared_data1.UseCount());

  shared_data2.Reset(new TestData(&counter2_));

  EXPECT_EQ(1, counter1_);
  EXPECT_EQ(1, counter2_);
  EXPECT_TRUE(shared_data1.Unique());
  EXPECT_EQ(1, shared_data1.UseCount());
  EXPECT_TRUE(shared_data2.Unique());
  EXPECT_EQ(1, shared_data2.UseCount());
}
