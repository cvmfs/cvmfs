/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>
#include <pthread.h>
#include <unistd.h>

#include "../../cvmfs/util_concurrency.h"

class T_BlockingCounter : public ::testing::Test {
 protected:
  T_BlockingCounter() : max_value_(100),
                        int_counter_(max_value_)
  {
    EXPECT_TRUE(int_counter_.HasMaximalValue());
    EXPECT_EQ(0, int_counter_);
    EXPECT_EQ(max_value_, int_counter_.maximal_value());
  }

  virtual void SetUp() {}

  static void *concurrent_incrementer(void *counter) {
    SynchronizingCounter<int64_t> &cntr =
      *static_cast<SynchronizingCounter<int64_t>*>(counter);

    T_BlockingCounter::concurrent_state_ = 1;
    ++cntr;
    T_BlockingCounter::concurrent_state_ = 2;

    return NULL;
  }

  static void *concurrent_zero_waiter(void *counter) {
    SynchronizingCounter<int64_t> &cntr =
      *static_cast<SynchronizingCounter<int64_t>*>(counter);

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

  static void *concurrent_increment_zero_wait(void *counter) {
    SynchronizingCounter<int64_t> &cntr =
      *static_cast<SynchronizingCounter<int64_t>*>(counter);
    T_BlockingCounter::concurrent_state_ = 1;

    cntr.Increment();
    T_BlockingCounter::concurrent_state_ = 2;

    cntr.WaitForZero();
    T_BlockingCounter::concurrent_state_ = 3;

    return NULL;
  }


  struct thread_state {
    thread_state() : counter(NULL), state(0) {}
    SynchronizingCounter<int64_t> *counter;
    int                 state;
  };

  static void *concurrent_orchestrate_thread(void *arg) {
    thread_state &state = *static_cast<thread_state*>(arg);
    state.state = 1;

    state.counter->Increment();
    state.state = 2;

    return NULL;
  }

 protected:
  int max_value_;
  SynchronizingCounter<int64_t> int_counter_;
  static int concurrent_state_;
};

int T_BlockingCounter::concurrent_state_ = 0;


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_BlockingCounter, Initialize) {
  EXPECT_EQ(  0, int_counter_);
  EXPECT_EQ(100, int_counter_.maximal_value());
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_BlockingCounter, Increment) {
  const int postcrement = int_counter_++;
  EXPECT_EQ(1, int_counter_);
  EXPECT_EQ(0, postcrement);

  const int precrement = ++int_counter_;
  EXPECT_EQ(2, int_counter_);
  EXPECT_EQ(2, precrement);

  int_counter_.Increment();
  EXPECT_EQ(3, int_counter_);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_BlockingCounter, Assignment) {
  const int val = max_value_ / 2;
  int_counter_ = val;
  EXPECT_EQ(val, int_counter_);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_BlockingCounter, Decrement) {
  const int val = max_value_ / 2;
  int_counter_ = val;

  const int postcrement = int_counter_--;
  EXPECT_EQ(val - 1, int_counter_);
  EXPECT_EQ(val,     postcrement);

  const int precrement = --int_counter_;
  EXPECT_EQ(val - 2, int_counter_);
  EXPECT_EQ(val - 2, precrement);

  int_counter_.Decrement();
  EXPECT_EQ(val - 3, int_counter_);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_BlockingCounter, IncrementAndWait) {
  int_counter_ = max_value_;

  T_BlockingCounter::concurrent_state_ = 0;
  EXPECT_EQ(0, T_BlockingCounter::concurrent_state_);

  pthread_t thread;
  const int res = pthread_create(&thread,
                                  NULL,
                                 &T_BlockingCounter::concurrent_incrementer,
                                  static_cast<void*>(&int_counter_));
  ASSERT_EQ(0, res);

  sleep(1);
  EXPECT_EQ(1, T_BlockingCounter::concurrent_state_);

  --int_counter_;
  sleep(1);
  EXPECT_EQ(2,          T_BlockingCounter::concurrent_state_);
  EXPECT_EQ(max_value_, int_counter_);

  const int killed = pthread_kill(thread, 0);
  EXPECT_EQ(ESRCH, killed) << "Thread did not exit properly";
  if (killed != ESRCH) {
    pthread_cancel(thread);
  } else {
    pthread_join(thread, NULL);
  }
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_BlockingCounter, BecomeZeroSlow) {
  T_BlockingCounter::concurrent_state_ = 0;
  EXPECT_EQ(0, T_BlockingCounter::concurrent_state_);

  ASSERT_EQ(0, int_counter_);
  pthread_t thread;
  const int res = pthread_create(&thread,
                                  NULL,
                                 &T_BlockingCounter::concurrent_zero_waiter,
                                  static_cast<void*>(&int_counter_));
  ASSERT_EQ(0, res);

  sleep(1);
  EXPECT_EQ(1,  T_BlockingCounter::concurrent_state_) << "Waited, even though "
                                                       << "counter was zero!";
  EXPECT_NE(0, int_counter_);

  int_counter_ = 1;
  --int_counter_;
  EXPECT_EQ(0, int_counter_);
  sleep(1);
  EXPECT_EQ(2,  T_BlockingCounter::concurrent_state_) << "Thread did not wake "
                                                       << "up for zero counter!";
  EXPECT_NE(0, int_counter_);

  int_counter_ = 0;
  EXPECT_EQ(0, int_counter_);
  sleep(1);
  EXPECT_EQ(3,  T_BlockingCounter::concurrent_state_) << "Thread didn't wake up "
                                                       << "for zero'ed counter!";

  const int killed = pthread_kill(thread, 0);
  EXPECT_EQ(ESRCH, killed) << "Thread did not exit properly";
  if (killed != ESRCH) {
    pthread_cancel(thread);
  } else {
    pthread_join(thread, NULL);
  }
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_BlockingCounter, BlockOnIncrementAndWaitForZeroSlow) {
  T_BlockingCounter::concurrent_state_ = 0;
  EXPECT_EQ(0, T_BlockingCounter::concurrent_state_);

  int_counter_ = max_value_;
  EXPECT_EQ(max_value_, int_counter_);

  pthread_t thread;
  const int res = pthread_create(&thread,
                                  NULL,
                                 &T_BlockingCounter::concurrent_increment_zero_wait,
                                  static_cast<void*>(&int_counter_));
  ASSERT_EQ(0, res);

  sleep(1);
  EXPECT_EQ(1, T_BlockingCounter::concurrent_state_) << "Thread didn't start";
  EXPECT_NE(0, int_counter_);

  int_counter_--;
  EXPECT_EQ(max_value_ - 1, int_counter_);
  sleep(1);
  EXPECT_EQ(2, T_BlockingCounter::concurrent_state_);
  EXPECT_EQ(max_value_, int_counter_);

  int decr_state = 0;
  for (int i = 0; i < max_value_; ++i) {
    if (decr_state == 0) --int_counter_;
    if (decr_state == 1) int_counter_--;
    if (decr_state == 2) int_counter_.Decrement();
    if (decr_state == 3) int_counter_ = int_counter_ - 1; // not thread safe!
    ++decr_state;
    decr_state = decr_state % 4;
  }

  sleep(1);
  EXPECT_EQ(3,  T_BlockingCounter::concurrent_state_);

  const int killed = pthread_kill(thread, 0);
  EXPECT_EQ(ESRCH, killed) << "Thread did not exit properly";
  if (killed != ESRCH) {
    pthread_cancel(thread);
  } else {
    pthread_join(thread, NULL);
  }
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_BlockingCounter, OrchestrateMultipleWaitingThreadsSlow) {
  int_counter_ = max_value_;
  EXPECT_EQ(max_value_, int_counter_);

  const int thread_count = 10;
  ASSERT_LE (5, thread_count);
  pthread_t     threads[thread_count];
  thread_state  states[thread_count];

  for (int i = 0; i < thread_count; ++i) {
    states[i].counter = &int_counter_;
    const int ret = pthread_create(&threads[i],
                                    NULL,
                                   &T_BlockingCounter::concurrent_orchestrate_thread,
                                    static_cast<void*>(&states[i]));
    ASSERT_EQ(0, ret);
  }
  sleep(1);

  for (int i = 0; i < thread_count; ++i) {
    EXPECT_EQ(1, states[i].state) << "Thread didn't start properly";
  }

  for (int i = 0; i <= 5; ++i) {
    if (i == 0) int_counter_ = max_value_;
    if (i == 1) int_counter_--;
    if (i == 2) int_counter_.Decrement();
    if (i == 3) int_counter_ = max_value_ - 1;
    if (i == 4) int_counter_--;
    if (i == 5) int_counter_ = 0;

    sleep(1);

    int continued = 0;
    for (int j = 0; j < thread_count; ++j) {
      if (states[j].state == 2) {
        ++continued;
      }
    }

    if (i < 5) EXPECT_EQ(i,            continued);
    else       EXPECT_EQ(thread_count, continued);
  }

  for (int i = 0; i < thread_count; ++i) {
    const int killed = pthread_kill(threads[i], 0);
    EXPECT_EQ(ESRCH, killed) << "Thread did not exit properly";
    if (killed != ESRCH) {
      pthread_cancel(threads[i]);
    } else {
      pthread_join(threads[i], NULL);
    }
  }
}
