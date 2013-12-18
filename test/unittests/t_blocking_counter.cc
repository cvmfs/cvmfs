#include <gtest/gtest.h>
#include <pthread.h>
#include <unistd.h>

#include "../../cvmfs/util_concurrency.h"

class T_BlockingCounter : public ::testing::Test {
 protected:
  T_BlockingCounter() : max_value_(100),
                        int_counter_(max_value_) {}

  virtual void SetUp() {}

  static void *concurrent_incrementer(void *counter) {
    BlockingIntCounter &cntr =
      *static_cast<BlockingIntCounter*>(counter);

    T_BlockingCounter::concurrent_state_ = 1;
    ++cntr;
    T_BlockingCounter::concurrent_state_ = 2;

    return NULL;
  }

  static void *concurrent_zero_waiter(void *counter) {
    BlockingIntCounter &cntr =
      *static_cast<BlockingIntCounter*>(counter);

    cntr.WaitForZero();
    ++cntr;
    T_BlockingCounter::concurrent_state_ = 1;

    cntr.WaitForZero();
    ++cntr;
    T_BlockingCounter::concurrent_state_ = 2;

    cntr.WaitForZero();
    T_BlockingCounter::concurrent_state_ = 3;

    return NULL;
  }

 protected:
  int max_value_;
  BlockingIntCounter int_counter_;
  static int concurrent_state_;
};

int T_BlockingCounter::concurrent_state_ = 0;


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_BlockingCounter, Initialize) {
  EXPECT_EQ (  0, int_counter_);
  EXPECT_EQ (100, int_counter_.MaximalValue());
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_BlockingCounter, Increment) {
  const int postcrement = int_counter_++;
  EXPECT_EQ (1, int_counter_);
  EXPECT_EQ (0, postcrement);

  const int precrement = ++int_counter_;
  EXPECT_EQ (2, int_counter_);
  EXPECT_EQ (2, precrement);

  int_counter_.Increment();
  EXPECT_EQ (3, int_counter_);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_BlockingCounter, Assignment) {
  const int val = max_value_ / 2;
  int_counter_ = val;
  EXPECT_EQ (val, int_counter_);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_BlockingCounter, Decrement) {
  const int val = max_value_ / 2;
  int_counter_ = val;

  const int postcrement = int_counter_--;
  EXPECT_EQ (val - 1, int_counter_);
  EXPECT_EQ (val,     postcrement);

  const int precrement = --int_counter_;
  EXPECT_EQ (val - 2, int_counter_);
  EXPECT_EQ (val - 2, precrement);

  int_counter_.Decrement();
  EXPECT_EQ (val - 3, int_counter_);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_BlockingCounter, IncrementAndWait) {
  int_counter_ = max_value_;

  T_BlockingCounter::concurrent_state_ = 0;
  EXPECT_EQ (0, T_BlockingCounter::concurrent_state_);

  pthread_t thread;
  const int res = pthread_create(&thread,
                                  NULL,
                                 &T_BlockingCounter::concurrent_incrementer,
                                  static_cast<void*>(&int_counter_));
  ASSERT_EQ (0, res);

  sleep(1);
  EXPECT_EQ (1, T_BlockingCounter::concurrent_state_);

  --int_counter_;
  sleep(1);
  EXPECT_EQ (2,          T_BlockingCounter::concurrent_state_);
  EXPECT_EQ (max_value_, int_counter_);

  const int killed = pthread_kill(thread, 0);
  EXPECT_EQ (ESRCH, killed) << "Thread did not exit properly";
  if (killed != ESRCH) {
    pthread_cancel(thread);
  } else {
    pthread_join(thread, NULL);
  }
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_BlockingCounter, BecomeZero) {
  T_BlockingCounter::concurrent_state_ = 0;
  EXPECT_EQ (0, T_BlockingCounter::concurrent_state_);

  ASSERT_EQ (0, int_counter_);
  pthread_t thread;
  const int res = pthread_create(&thread,
                                  NULL,
                                 &T_BlockingCounter::concurrent_zero_waiter,
                                  static_cast<void*>(&int_counter_));
  ASSERT_EQ (0, res);

  sleep(1);
  EXPECT_EQ (1,  T_BlockingCounter::concurrent_state_) << "Waited, even though "
                                                       << "counter was zero!";
  EXPECT_NE (0, int_counter_);

  int_counter_ = 1;
  --int_counter_;
  EXPECT_EQ (0, int_counter_);
  sleep(1);
  EXPECT_EQ (2,  T_BlockingCounter::concurrent_state_) << "Thread did not wake "
                                                       << "up for zero counter!";
  EXPECT_NE (0, int_counter_);

  int_counter_ = 0;
  EXPECT_EQ (0, int_counter_);
  sleep(1);
  EXPECT_EQ (3,  T_BlockingCounter::concurrent_state_) << "Thread didn't wake up "
                                                       << "for zero'ed counter!";

  const int killed = pthread_kill(thread, 0);
  EXPECT_EQ (ESRCH, killed) << "Thread did not exit properly";
  if (killed != ESRCH) {
    pthread_cancel(thread);
  }
}
