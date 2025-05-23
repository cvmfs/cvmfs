/**
 * This file is part of the CernVM File System.
 */

#include <errno.h>
#include <gtest/gtest.h>
#include <pthread.h>
#include <unistd.h>

#include "util/concurrency.h"

class T_BlockingCounter : public ::testing::Test {
 protected:
  T_BlockingCounter() : max_value_(100), int_counter_(max_value_) {
    EXPECT_TRUE(int_counter_.HasMaximalValue());
    EXPECT_EQ(0, int_counter_.Get());
    EXPECT_EQ(max_value_, int_counter_.maximal_value());
  }

  virtual void SetUp() { }

  static void *concurrent_incrementer(void *counter) {
    SynchronizingCounter<int64_t>
        &cntr = *static_cast<SynchronizingCounter<int64_t> *>(counter);

    T_BlockingCounter::concurrent_state_ = 1;
    ++cntr;
    T_BlockingCounter::concurrent_state_ = 2;

    return NULL;
  }

  static void *concurrent_zero_waiter(void *counter) {
    SynchronizingCounter<int64_t>
        &cntr = *static_cast<SynchronizingCounter<int64_t> *>(counter);

    cntr.WaitForZero();
    ++cntr;
    T_BlockingCounter::concurrent_state_ = 1;

    return NULL;
  }

  struct thread_state {
    thread_state() : counter(NULL), state(0) { }
    SynchronizingCounter<int64_t> *counter;
    int state;
  };

  static void *concurrent_orchestrate_thread(void *arg) {
    thread_state &state = *static_cast<thread_state *>(arg);
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


TEST_F(T_BlockingCounter, Initialize) {
  EXPECT_EQ(0, int_counter_.Get());
  EXPECT_EQ(100, int_counter_.maximal_value());
}


TEST_F(T_BlockingCounter, Increment) {
  const int postcrement = int_counter_++;
  EXPECT_EQ(1, int_counter_.Get());
  EXPECT_EQ(0, postcrement);

  const int precrement = ++int_counter_;
  EXPECT_EQ(2, int_counter_.Get());
  EXPECT_EQ(2, precrement);

  int_counter_.Increment();
  EXPECT_EQ(3, int_counter_.Get());
}


TEST_F(T_BlockingCounter, Assignment) {
  const int val = max_value_ / 2;
  int_counter_ = val;
  EXPECT_EQ(val, int_counter_.Get());
}


TEST_F(T_BlockingCounter, Decrement) {
  const int val = max_value_ / 2;
  int_counter_ = val;

  const int postcrement = int_counter_--;
  EXPECT_EQ(val - 1, int_counter_.Get());
  EXPECT_EQ(val, postcrement);

  const int precrement = --int_counter_;
  EXPECT_EQ(val - 2, int_counter_.Get());
  EXPECT_EQ(val - 2, precrement);

  int_counter_.Decrement();
  EXPECT_EQ(val - 3, int_counter_.Get());
}


TEST_F(T_BlockingCounter, IncrementAndWait) {
  int_counter_ = max_value_;

  T_BlockingCounter::concurrent_state_ = 0;
  EXPECT_EQ(0, T_BlockingCounter::concurrent_state_);

  pthread_t thread;
  const int res = pthread_create(&thread,
                                 NULL,
                                 &T_BlockingCounter::concurrent_incrementer,
                                 static_cast<void *>(&int_counter_));
  ASSERT_EQ(0, res);

  --int_counter_;
  pthread_join(thread, NULL);

  EXPECT_EQ(2, T_BlockingCounter::concurrent_state_);
  EXPECT_EQ(max_value_, int_counter_.Get());
}


TEST_F(T_BlockingCounter, BecomeZero) {
  T_BlockingCounter::concurrent_state_ = 0;
  EXPECT_EQ(0, T_BlockingCounter::concurrent_state_);

  int_counter_ = 1;
  pthread_t thread;
  const int res = pthread_create(&thread,
                                 NULL,
                                 &T_BlockingCounter::concurrent_zero_waiter,
                                 static_cast<void *>(&int_counter_));
  ASSERT_EQ(0, res);

  --int_counter_;
  pthread_join(thread, NULL);
  EXPECT_EQ(1, T_BlockingCounter::concurrent_state_);
  EXPECT_EQ(1, int_counter_.Get());
}


TEST_F(T_BlockingCounter, OrchestrateMultipleWaitingThreads) {
  int_counter_ = max_value_;
  EXPECT_EQ(max_value_, int_counter_.Get());

  const int thread_count = 10;
  ASSERT_LE(5, thread_count);
  pthread_t threads[thread_count];
  thread_state states[thread_count];

  for (int i = 0; i < thread_count; ++i) {
    states[i].counter = &int_counter_;
    const int ret = pthread_create(
        &threads[i], NULL, &T_BlockingCounter::concurrent_orchestrate_thread,
        static_cast<void *>(&states[i]));
    ASSERT_EQ(0, ret);
  }

  for (int i = 0; i < thread_count; ++i) {
    int_counter_.Decrement();
  }

  for (int i = 0; i < thread_count; ++i) {
    pthread_join(threads[i], NULL);
    EXPECT_EQ(2, states[i].state);
  }
}
