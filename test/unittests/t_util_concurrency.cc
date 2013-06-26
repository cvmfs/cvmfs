#include <gtest/gtest.h>
#include <errno.h>

#include "../../cvmfs/util_concurrency.h"

//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//

class DummyLocker {
 public:
  DummyLocker() : locked(false) {}
  void Lock() const {
    locked = true;
  }
  void Unlock() const {
    locked = false;
  }

 public:
  mutable bool locked;
};

TEST(T_UtilConcurrency, ArbitraryLockGurad) {
  DummyLocker locker;
  ASSERT_FALSE (locker.locked);

  {
    LockGuard<DummyLocker> lock(locker);
    EXPECT_TRUE (locker.locked) << "LockGuard didn't lock";
  }

  EXPECT_FALSE (locker.locked) << "LockGuard didn't unlock";
}


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//


TEST(T_UtilConcurrency, MutexLockGuard) {
  pthread_mutex_t mutex;
  int retcode = pthread_mutex_init(&mutex, NULL);
  ASSERT_EQ (0, retcode);

  {
    MutexLockGuard lock(mutex);

    retcode = pthread_mutex_trylock(&mutex);
    EXPECT_EQ (EBUSY, retcode) << "MutexLockGuard didn't lock";
  }

  retcode = pthread_mutex_trylock(&mutex);
  EXPECT_EQ (0, retcode) << "MutexLockGuard didn't unlock";

  retcode = pthread_mutex_unlock(&mutex);
  EXPECT_EQ (0, retcode);

  retcode = pthread_mutex_destroy(&mutex);
  EXPECT_EQ (0, retcode);
}


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//


TEST(T_UtilConcurrency, ReadLockGuard) {
  pthread_rwlock_t rwlock;
  int retcode = pthread_rwlock_init(&rwlock, NULL);
  ASSERT_EQ (0, retcode);

  {
    ReadLockGuard lock(rwlock);

    retcode = pthread_rwlock_tryrdlock(&rwlock);
    EXPECT_EQ (0, retcode) << "ReadLockGuard prevents additional read lock";

    retcode = pthread_rwlock_unlock(&rwlock);
    EXPECT_EQ (0, retcode);

    retcode = pthread_rwlock_trywrlock(&rwlock);
    EXPECT_EQ (EBUSY, retcode) << "ReadLockGuard allows concurrent read and write";
  }

  retcode = pthread_rwlock_trywrlock(&rwlock);
  EXPECT_EQ (0, retcode) << "ReadLockGuard didn't unlock";

  retcode = pthread_rwlock_unlock(&rwlock);
  EXPECT_EQ (0, retcode);

  retcode = pthread_rwlock_destroy(&rwlock);
  EXPECT_EQ (0, retcode);
}


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//


TEST(T_UtilConcurrency, WriteLockGuard) {
  pthread_rwlock_t rwlock;
  int retcode = pthread_rwlock_init(&rwlock, NULL);
  ASSERT_EQ (0, retcode);

  {
    WriteLockGuard lock(rwlock);

    retcode = pthread_rwlock_tryrdlock(&rwlock);
    EXPECT_EQ (EBUSY, retcode) << "WriteLockGuard didn't lock - rdlock possible";
    if (0 == retcode) {
      retcode = pthread_rwlock_unlock(&rwlock);
      ASSERT_EQ (0, retcode);
    }

    retcode = pthread_rwlock_trywrlock(&rwlock);
    EXPECT_EQ (EBUSY, retcode) << "WriteLockGuard didn't lock - wrlock possible";
    if (0 == retcode) {
      retcode = pthread_rwlock_unlock(&rwlock);
      ASSERT_EQ (0, retcode);
    }
  }

  retcode = pthread_rwlock_trywrlock(&rwlock);
  EXPECT_EQ (0, retcode) << "WriteLockGuard didn't unlock";

  retcode = pthread_rwlock_unlock(&rwlock);
  EXPECT_EQ (0, retcode);

  retcode = pthread_rwlock_destroy(&rwlock);
  EXPECT_EQ (0, retcode);
}


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//


class DummyLockable : public Lockable {};

TEST(T_UtilConcurrency, Lockable) {
  DummyLockable lockable;
  int retcode = 0;

  {
    LockGuard<DummyLockable> lock(lockable);

    retcode = lockable.TryLock();
    EXPECT_EQ (EBUSY, retcode) << "Lockable didn't lock";
  }

  retcode = lockable.TryLock();
  EXPECT_EQ (0, retcode) << "Lockable didn't unlock";
}


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//


void CallbackFn(bool* const &param) { *param = true; }

TEST(T_UtilConcurrency, SimpleCallback) {
  bool callback_called = false;

  Callback<bool*> callback(&CallbackFn);
  callback(&callback_called);
  EXPECT_TRUE (callback_called);
}


class DummyCallbackDelegate {
 public:
  DummyCallbackDelegate() : callback_result(-1) {}
  void CallbackMd(const int &value) { callback_result = value; }

 public:
  int callback_result;
};

TEST(T_UtilConcurrency, BoundCallback) {
  DummyCallbackDelegate delegate;
  ASSERT_EQ (-1, delegate.callback_result);

  BoundCallback<int, DummyCallbackDelegate> callback(
                              &DummyCallbackDelegate::CallbackMd,
                              &delegate);
  callback(42);
  EXPECT_EQ (42, delegate.callback_result);
}


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//


class DummyCallbackable : public Callbackable<int> {
 public:
  DummyCallbackable() : callback_result(-1) {}
  void CallbackMd(const int &value) { callback_result = value; }

 public:
  int callback_result;
};

TEST(T_UtilConcurrency, Callbackable) {
  DummyCallbackable callbackable;
  ASSERT_EQ (-1, callbackable.callback_result);

  DummyCallbackable::callback_t *callback =
    DummyCallbackable::MakeCallback(&DummyCallbackable::CallbackMd,
                                    &callbackable);
  (*callback)(1337);

  EXPECT_EQ (1337, callbackable.callback_result);
}


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//


class DummyObservable : public Observable<int> {
 public:
  void DoNotification(const int value) {
    NotifyListeners(value);
  }
};

class DummyObserver {
 public:
  DummyObserver() : observation_result(-1) {}
  void CallbackMd(const int &value) { observation_result = value; }

 public:
  int observation_result;
};

int g_fn_observation_result = -1;
void ObserverFn(const int &value) { g_fn_observation_result = value; }

TEST(T_UtilConcurrency, Observable) {
  DummyObserver   observer;
  DummyObservable observee;

  ASSERT_EQ (-1, observer.observation_result);
  ASSERT_EQ (-1, g_fn_observation_result);

  DummyObservable::callback_t *bound_callback =
    observee.RegisterListener(&DummyObserver::CallbackMd, &observer);
  DummyObservable::callback_t *static_callback =
    observee.RegisterListener(&ObserverFn);

  static const DummyObservable::callback_t *null_clb = NULL;

  ASSERT_NE (null_clb, bound_callback);
  ASSERT_NE (null_clb, static_callback);

  observee.DoNotification(314);
  EXPECT_EQ (314, observer.observation_result) << "observing class not notified";
  EXPECT_EQ (314, g_fn_observation_result) << "observing static function not notified";

  observee.UnregisterListener(static_callback);
  observee.DoNotification(1);
  EXPECT_EQ (1,   observer.observation_result);
  EXPECT_EQ (314, g_fn_observation_result);

  observee.UnregisterListener(bound_callback);
  observee.DoNotification(-100);
  EXPECT_EQ (1,   observer.observation_result);
  EXPECT_EQ (314, g_fn_observation_result);

  bound_callback =
    observee.RegisterListener(&DummyObserver::CallbackMd, &observer);
  ASSERT_NE (null_clb, bound_callback);
  observee.DoNotification(4);
  EXPECT_EQ (4,   observer.observation_result);
  EXPECT_EQ (314, g_fn_observation_result);

  static_callback = observee.RegisterListener(&ObserverFn);
  ASSERT_NE (null_clb, static_callback);
  observee.DoNotification(123457);
  EXPECT_EQ (123457, observer.observation_result);
  EXPECT_EQ (123457, g_fn_observation_result);

  observee.UnregisterListeners();
  observee.DoNotification(0);
  EXPECT_EQ (123457, observer.observation_result);
  EXPECT_EQ (123457, g_fn_observation_result);
}


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//


TEST(T_UtilConcurrency, SingleThreadedFifoChannel) {
  const size_t max_length = 100;
  FifoChannel<int> fifo_queue(max_length, 1);
  ASSERT_TRUE (fifo_queue.IsEmpty());
  EXPECT_EQ   (max_length, fifo_queue.GetMaximalItemCount());

  for (int i = 0; i < static_cast<int>(max_length) - 1; ++i) {
    fifo_queue.Enqueue(i * 10);
  }
  EXPECT_FALSE (fifo_queue.IsEmpty());
  EXPECT_EQ    (size_t(max_length - 1), fifo_queue.GetItemCount());

  for (int i = 0; i < static_cast<int>(max_length) - 1; ++i) {
    const int result = fifo_queue.Dequeue();
    EXPECT_EQ (i * 10, result);
  }
  EXPECT_TRUE (fifo_queue.IsEmpty());

  for (int i = 0; i < static_cast<int>(max_length) - 1; ++i) {
    fifo_queue.Enqueue(1);
  }
  const unsigned int dropped_items = fifo_queue.Drop();
  EXPECT_EQ   (max_length - 1, dropped_items);
  EXPECT_TRUE (fifo_queue.IsEmpty());
  EXPECT_EQ   (size_t(0), fifo_queue.GetItemCount());
}


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//


const int g_kill_signal     = -1;
const int g_base_value      = 5;
const int g_cpu_burn_cycles = 100;
const int g_insert_cycles   = 1000000;

struct consumer_data {
  consumer_data(FifoChannel<int> *channel) :
    channel(channel), checksum(0) {}

  FifoChannel<int>  *channel;
  unsigned int       checksum;
};

void* producer(void *data) {
  FifoChannel<int> *channel = static_cast<FifoChannel<int>*>(data);

  for (int i = 0; i < g_insert_cycles; ++i) {
    channel->Enqueue(g_base_value);
  }

  return data;
}

void* consumer(void *data) {
  consumer_data *params = static_cast<consumer_data*>(data);
  int value = 0;

  while (true) {
    value = params->channel->Dequeue();
    if (value == g_kill_signal) break;
    for (int i = 0; i < g_cpu_burn_cycles; ++i) ++value;
    params->checksum += value;
  }

  return data;
}

TEST(T_UtilConcurrency, MultiThreadedFifoChannel) {
  const int consumer_thread_count = 10;

  FifoChannel<int> channel(300, 100);

  pthread_t       producer_thread;
  pthread_t       consumer_threads[consumer_thread_count];
  consumer_data  *output_data[consumer_thread_count];

  int retval = 0;

  // create output data structures
  for (int i = 0; i < consumer_thread_count; ++i) {
    output_data[i] = new consumer_data(&channel);
  }

  // spawn producer thread
  retval = pthread_create(&producer_thread,
                          NULL,
                          &producer,
                          static_cast<void*>(&channel));
  ASSERT_EQ (0, retval);

  // spawn consumer threads
  for (int i = 0; i < consumer_thread_count; ++i) {
    retval = pthread_create(&consumer_threads[i],
                            NULL,
                            &consumer,
                            static_cast<void*>(output_data[i]));
    ASSERT_EQ (0, retval);
  }

  // wait for the producer to finish producing
  retval = pthread_join(producer_thread, NULL);
  ASSERT_EQ (0, retval);

  // send kill signals for the consumers to finish
  for (int i = 0; i < consumer_thread_count; ++i) {
    channel.Enqueue(g_kill_signal);
  }

  // wait for the consumers to terminate
  for (int i = 0; i < consumer_thread_count; ++i) {
    retval = pthread_join(consumer_threads[i], NULL);
    ASSERT_EQ (0, retval);
  }

  // do the checks
  EXPECT_TRUE (channel.IsEmpty());

  unsigned int checksum = 0;
  for (int i = 0; i < consumer_thread_count; ++i) {
    checksum += output_data[i]->checksum;
    delete output_data[i];
    output_data[i] = NULL;
  }
  const unsigned int expected_checksum = g_insert_cycles * g_base_value      +
                                         g_insert_cycles * g_cpu_burn_cycles;
  EXPECT_EQ (expected_checksum, checksum);
}
