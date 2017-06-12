/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "util/shared_ptr.h"
//#include "util/weak_ptr.h"
#include "util/single_copy.h"


class ClassSharedMember {
 public:
  ClassSharedMember(int* counter, int value)
      : counter_(counter), value_(new int(value)) {
    (*counter_)++;
  }
  ~ClassSharedMember() { (*counter_)--; }

  // Need to defined special copy-constructor in order to increment counter_...
  ClassSharedMember(const ClassSharedMember& other)
      : counter_(other.counter_), value_(other.value_) {
    (*counter_)++;
  }

  int value_use_count() const { return value_.use_count(); }

 private:
  int* counter_;
  SharedPtr<int> value_;
};

// Test class that allows tracking the number of instances
class TestData {
 public:
  explicit TestData(int* val) : val_(val) { (*val_)++; }
  ~TestData() { (*val_)--; }

 private:
  int* val_;
};

class T_SmartPtr : public ::testing::Test {
 protected:
  // At the beginning of each test case, there should be no living instances of
  // TestData
  void SetUp() { counter_ = 0; }
  // After each test case, there should be no more living instances of TestData.
  void TearDown() { ASSERT_EQ(counter_, 0); }

  int counter_;
};

// Basic usage of shared_ptr
TEST_F(T_SmartPtr, SharedPtr) {
  // New instance of TestData managed by shared_ptr
  SharedPtr<TestData> shared_data1(new TestData(&counter_));
  // Counter should indicated the new instance
  EXPECT_EQ(counter_, 1);
  // The reference should be unique at this point
  EXPECT_TRUE(shared_data1.unique());
  {
    // Make a new reference to the existing TestData instance
    SharedPtr<TestData> shared_data2 = shared_data1;
    // Counter hasn't been incremented, since we are still dealing with a single
    // TestData instance
    EXPECT_EQ(counter_, 1);
    // The shared_ptr reference is no longer unique...
    EXPECT_FALSE(shared_data1.unique());
    // And the use count of the shared references is 2
    EXPECT_EQ(shared_data1.use_count(), 2);
    EXPECT_EQ(shared_data2.use_count(), 2);
  }
  // shared_data2 has been destroyed. The reference is again unique.
  EXPECT_TRUE(shared_data1.unique());
}

// Working with classes which contain shared_ptr members
TEST_F(T_SmartPtr, SharedPtrMembers) {
  // Create an instance of ClassSharedMember
  ClassSharedMember data1(&counter_, 1234);
  EXPECT_EQ(counter_, 1);

  {
    // Create a new instance of ClassSharedMember using the copy-constructor
    ClassSharedMember data2(data1);
    EXPECT_EQ(counter_, 2);
    // The shared reference has 2 uses, by data1 and data2
    EXPECT_EQ(data1.value_use_count(), 2);
  }

  // data2 has been destroyed.
  EXPECT_EQ(counter_, 1);
  EXPECT_EQ(data1.value_use_count(), 1);
}

/*
// Using WeakPtr
TEST_F(T_SmartPtr, WeakPtr) {
  // We create an empty weak_ptr in the outer scope
  WeakPtr<TestData> weak_ref;
  {
    // A new TestData instance in the inner scope, managed by a shared_ptr
    SharedPtr<TestData> shared_data1(new TestData(&counter_));
    // Create a weak reference to the shared_data
    weak_ref = shared_data1;
    // A weak reference does not take ownership
    EXPECT_EQ(counter_, 1);
    // The shared (strong) reference is still unique
    EXPECT_TRUE(shared_data1.unique());
    {
      // We can use the lock() method to transform the weak_ptr into a
      // shared_ptr
      SharedPtr<TestData> shared_data2 = weak_ref.lock();
      // Shared reference is no longer unique
      EXPECT_FALSE(shared_data1.unique());
      // Use count was incremented
      EXPECT_EQ(shared_data1.use_count(), 2);
      EXPECT_EQ(shared_data2.use_count(), 2);
    }
    // shared_data2 was destroyed, the shared reference is unique again
    EXPECT_TRUE(shared_data1.unique());
  }
  // At this point we only have a weak_ptr to the original data, all the strong
  // shared_ptr references have been destroyed. The lock() method will return an
  // empty (null) shared_ptr.
  SharedPtr<TestData> shared_data3 = weak_ref.lock();
  EXPECT_FALSE(shared_data3);
}
*/
