#include <gtest/gtest.h>
#include <cassert>
#include <unistd.h>

#include "../../cvmfs/util.h"
#include "../../cvmfs/util_concurrency.h"


struct thread_args {
  int                    state;
  SynchronizingIntCounter *counter;
};


class T_SynchronizingCounter : public ::testing::Test {
 public:
  T_SynchronizingCounter() :
    threads_(NULL), states_(NULL), n_threads_(0) {}
  virtual ~T_SynchronizingCounter() {
    Clear();
  }

 protected:
  void StartThreads(const unsigned int     number_of_threads,
                    SynchronizingIntCounter &counter_to_test,
                    void *(*thread_function)(void*)) {
    n_threads_ = number_of_threads;
    threads_   = new pthread_t[n_threads_];
    states_    = new thread_args[n_threads_];

    for (unsigned int i = 0; i < n_threads_; ++i) {
      states_[i].state   = 0;
      states_[i].counter = &counter_to_test;
      const int r = pthread_create(&threads_[i], NULL,
                                   thread_function, &states_[i]);
      ASSERT_EQ (0, r) << "Failed to spawn thread";
    }
  }

  void CheckStateValues(const int expected_value,
                        const std::string &error_message = "") {
    for (unsigned int i = 0; i < n_threads_; ++i) {
      EXPECT_EQ (expected_value, states_[i].state) << error_message;
    }
  }

  void JoinThreads() {
    for (unsigned int i = 0; i < n_threads_; ++i) {
      const int killed = pthread_kill(threads_[i], 0);
      EXPECT_EQ (ESRCH, killed) << "Thread did not exit properly";
      if (killed != ESRCH) {
        pthread_cancel(threads_[i]);
      } else {
        pthread_join(threads_[i], NULL);
      }
    }

    Clear();
  }

  void Clear() {
    if (threads_ != NULL) {
      delete[] threads_;
      threads_ = NULL;
    }

    if (states_ != NULL) {
      delete[] states_;
      states_ = NULL;
    }
  }

 private:
  pthread_t   *threads_;
  thread_args *states_;
  unsigned int n_threads_;
};


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_SynchronizingCounter, Initialize) {
  SynchronizingIntCounter counter;
  EXPECT_EQ (0, counter);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_SynchronizingCounter, Increment) {
  SynchronizingIntCounter counter;
  EXPECT_EQ (0, counter);

  counter++;
  EXPECT_EQ (1, counter);

  int value = 0;
  value = counter++;
  EXPECT_EQ (1, value);
  EXPECT_EQ (2, counter);

  value = ++counter;
  EXPECT_EQ (3, value);
  EXPECT_EQ (3, counter);

  counter.Increment();
  EXPECT_EQ (4, counter);

  value = counter.Increment();
  EXPECT_EQ (5, value);
  EXPECT_EQ (5, counter);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_SynchronizingCounter, Assign) {
  SynchronizingIntCounter counter;
  EXPECT_EQ (0, counter);

  counter = 100;
  EXPECT_EQ (100, counter);

  counter = 0;
  EXPECT_EQ (0, counter);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_SynchronizingCounter, Decrement) {
  SynchronizingIntCounter counter;
  EXPECT_EQ (0, counter);

  counter = 100;
  EXPECT_EQ (100, counter);

  counter--;
  EXPECT_EQ (99, counter);

  int value = 0;
  value = counter--;
  EXPECT_EQ (99, value);
  EXPECT_EQ (98, counter);

  value = --counter;
  EXPECT_EQ (97, value);
  EXPECT_EQ (97, counter);

  counter.Decrement();
  EXPECT_EQ (96, counter);

  value = counter.Decrement();
  EXPECT_EQ (95, value);
  EXPECT_EQ (95, counter);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


void *thread_wait_for_assignment(void *arg) {
  thread_args &state = *static_cast<thread_args*>(arg);
  state.state = 1;

  state.counter->WaitForZero();
  state.state = 2;

  return NULL;
}

TEST_F(T_SynchronizingCounter, WaitForAssignment) {
  SynchronizingIntCounter counter;
  EXPECT_EQ (0, counter);

  counter = 1;

  StartThreads(5, counter, thread_wait_for_assignment);
  sleep(1);

  CheckStateValues(1, "Thread didn't start properly");
  EXPECT_EQ (1, counter);

  counter = 0;
  EXPECT_EQ (0, counter);
  sleep(1);

  CheckStateValues(2, "Thread didn't continue properly");
  EXPECT_EQ (0, counter);

  JoinThreads();
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


void *thread_wait_for_decrement(void *arg) {
  thread_args &state = *static_cast<thread_args*>(arg);
  state.state = 1;

  state.counter->WaitForZero();
  state.state = 2;
  sleep(1);
  state.counter->Increment();

  state.counter->WaitForZero();
  state.state = 3;
  sleep(1);
  state.counter->Increment();

  state.counter->WaitForZero();
  state.state = 4;

  return NULL;
}

TEST_F(T_SynchronizingCounter, WaitForDecrement) {
  const unsigned int n_threads = 5;
  SynchronizingIntCounter counter;
  EXPECT_EQ (0, counter);

  counter = 1;

  StartThreads(n_threads, counter, thread_wait_for_decrement);
  sleep(1);

  CheckStateValues(1, "Thread didn't start properly");
  EXPECT_EQ (1, counter);

  counter--;
  sleep(2);
  CheckStateValues(2, "Thread didn't continue properly on post-decrement");
  EXPECT_EQ(static_cast<int>(n_threads), counter);

  for (unsigned int i = 0; i < n_threads - 1; ++i) {
    --counter;
  }
  sleep(2);
  CheckStateValues(2, "Threads prematurely continued!");

  --counter;
  sleep(2);
  CheckStateValues(3, "Threads didn't continue properly on pre-decrement");
  EXPECT_EQ(static_cast<int>(n_threads), counter);

  counter = 1;
  sleep(1);
  CheckStateValues(3, "Threads prematurely continued on assignment!");

  counter.Decrement();
  sleep(1);
  CheckStateValues(4, "Threads didn't continue properly on Decrement()");

  JoinThreads();
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


void *thread_wait_for_increment(void *arg) {
  thread_args &state = *static_cast<thread_args*>(arg);
  state.state = 1;

  state.counter->WaitForZero();
  state.state = 2;
  sleep(1);
  state.counter->Decrement();

  state.counter->WaitForZero();
  state.state = 3;
  sleep(1);
  state.counter->Decrement();

  state.counter->WaitForZero();
  state.state = 4;

  return NULL;
}

TEST_F(T_SynchronizingCounter, WaitForIncrement) {
  const unsigned int n_threads = 5;
  SynchronizingIntCounter counter;
  EXPECT_EQ (0, counter);

  counter = -1;

  StartThreads(n_threads, counter, thread_wait_for_increment);
  sleep(1);

  CheckStateValues(1, "Thread didn't start properly");
  EXPECT_EQ (-1, counter);

  counter++;
  sleep(2);
  CheckStateValues(2, "Thread didn't continue properly on post-decrement");
  EXPECT_EQ(-static_cast<int>(n_threads), counter);

  for (unsigned int i = 0; i < n_threads - 1; ++i) {
    ++counter;
  }
  sleep(2);
  CheckStateValues(2, "Threads prematurely continued!");

  ++counter;
  sleep(2);
  CheckStateValues(3, "Threads didn't continue properly on pre-decrement");
  EXPECT_EQ(-static_cast<int>(n_threads), counter);

  counter = -1;
  sleep(1);
  CheckStateValues(3, "Threads prematurely continued on assignment!");

  counter.Increment();
  sleep(1);
  CheckStateValues(4, "Threads didn't continue properly on Increment()");

  JoinThreads();
}

