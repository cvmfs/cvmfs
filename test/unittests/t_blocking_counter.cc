#include <gtest/gtest.h>
#include <pthread.h>
#include <unistd.h>

#include "../../cvmfs/util_concurrency.h"

class T_BlockingCounter : public ::testing::Test {
 protected:
  T_BlockingCounter() : max_value_(100),
                        uint_counter_(max_value_) {}

  virtual void SetUp() {}

  static void *concurrent_incrementer(void *counter) {
    BlockingCounter<unsigned int> &cntr =
      *static_cast<BlockingCounter<unsigned int>*>(counter);

    T_BlockingCounter::concurrent_state_ = 1;
    ++cntr;
    T_BlockingCounter::concurrent_state_ = 2;

    return NULL;
  }

  static void *concurrent_zero_waiter(void *counter) {
    BlockingCounter<unsigned int> &cntr =
      *static_cast<BlockingCounter<unsigned int>*>(counter);

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
  unsigned int max_value_;
  BlockingCounter<unsigned int> uint_counter_;
  static int concurrent_state_;
};

int T_BlockingCounter::concurrent_state_ = 0;


TEST_F(T_BlockingCounter, Initialize) {
  EXPECT_EQ (  0u, uint_counter_);
  EXPECT_EQ (100u, uint_counter_.MaximalValue());
}


TEST_F(T_BlockingCounter, Increment) {
  const unsigned int postcrement = uint_counter_++;
  EXPECT_EQ (1u, uint_counter_);
  EXPECT_EQ (0u, postcrement);

  const unsigned int precrement = ++uint_counter_;
  EXPECT_EQ (2u, uint_counter_);
  EXPECT_EQ (2u, precrement);

  uint_counter_.Increment();
  EXPECT_EQ (3u, uint_counter_);
}


TEST_F(T_BlockingCounter, Assignment) {
  const unsigned int val = max_value_ / 2;
  uint_counter_ = val;
  EXPECT_EQ (val, uint_counter_);
}


TEST_F(T_BlockingCounter, Decrement) {
  const unsigned int val = max_value_ / 2;
  uint_counter_ = val;

  const unsigned int postcrement = uint_counter_--;
  EXPECT_EQ (val - 1, uint_counter_);
  EXPECT_EQ (val,     postcrement);

  const unsigned int precrement = --uint_counter_;
  EXPECT_EQ (val - 2, uint_counter_);
  EXPECT_EQ (val - 2, precrement);

  uint_counter_.Decrement();
  EXPECT_EQ (val - 3, uint_counter_);
}


TEST_F(T_BlockingCounter, IncrementAndWait) {
  uint_counter_ = max_value_;

  T_BlockingCounter::concurrent_state_ = 0;
  EXPECT_EQ (0, T_BlockingCounter::concurrent_state_);

  pthread_t thread;
  const int res = pthread_create(&thread,
                                  NULL,
                                 &T_BlockingCounter::concurrent_incrementer,
                                  static_cast<void*>(&uint_counter_));
  ASSERT_EQ (0, res);

  sleep(1);
  EXPECT_EQ (1, T_BlockingCounter::concurrent_state_);

  --uint_counter_;
  sleep(1);
  EXPECT_EQ (2,          T_BlockingCounter::concurrent_state_);
  EXPECT_EQ (max_value_, uint_counter_);

  const int killed = pthread_kill(thread, 0);
  EXPECT_EQ (ESRCH, killed) << "Thread did not exit properly";
  if (killed == ESRCH) {
    pthread_cancel(thread);
  }
}


TEST_F(T_BlockingCounter, BecomeZero) {
  T_BlockingCounter::concurrent_state_ = 0;
  EXPECT_EQ (0, T_BlockingCounter::concurrent_state_);

  ASSERT_EQ (0u, uint_counter_);
  pthread_t thread;
  const int res = pthread_create(&thread,
                                  NULL,
                                 &T_BlockingCounter::concurrent_zero_waiter,
                                  static_cast<void*>(&uint_counter_));
  ASSERT_EQ (0, res);

  sleep(1);
  EXPECT_EQ (1,  T_BlockingCounter::concurrent_state_) << "Waited, even though "
                                                       << "counter was zero!";
  EXPECT_NE (0u, uint_counter_);

  uint_counter_ = 1u;
  --uint_counter_;
  EXPECT_EQ (0u, uint_counter_);
  sleep(1);
  EXPECT_EQ (2,  T_BlockingCounter::concurrent_state_) << "Thread did not wake "
                                                       << "up for zero counter!";
  EXPECT_NE (0u, uint_counter_);

  uint_counter_ = 0u;
  EXPECT_EQ (0u, uint_counter_);
  sleep(1);
  EXPECT_EQ (3,  T_BlockingCounter::concurrent_state_) << "Thread didn't wake up "
                                                       << "for zero'ed counter!";

  const int killed = pthread_kill(thread, 0);
  EXPECT_EQ (ESRCH, killed) << "Thread did not exit properly";
  if (killed == ESRCH) {
    pthread_cancel(thread);
  }
}
