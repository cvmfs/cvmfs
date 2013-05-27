#include <gtest/gtest.h>

#include "../../cvmfs/util_concurrency.h"


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
