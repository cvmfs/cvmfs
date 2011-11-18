#ifndef THREAD_SAFE_H
#define THREAD_SAFE_H 1

#include <pthread.h>

namespace cvmfs {

/**
 *  base class for thread safe objects
 *  it implements a simple mutex mechanism to make sure the object
 *  is only accessed by one thread at a time
 *
 *  Usage:
 *    1. inherit from ThreadSafe
 *    2. use the macro LOCKED_SCOPE as _first_ statement in any critical scope
 *       i.e:  <method signature>() { LOCKED_SCOPE; doCrazyStuff(); return; }
 *             this will secure the whole method scope with the mutex lock
 *
 *    you can also use the protected methods Lock() and Unlock() for
 *    manual locking, but this is discouraged!
 */
class ThreadSafeMutex {
 public:
  ThreadSafeMutex();
  virtual ~ThreadSafeMutex();
  
 protected:
  inline void Lock() { pthread_mutex_lock(&mutex_); }
  inline void Unlock() { pthread_mutex_unlock(&mutex_); }
 
 protected:
  pthread_mutex_t mutex_;
};

/**
 *  use this macro to mark scopes to secure by the mutex
 *  see also: the description of ThreadSafe
 */
#define LOCKED_SCOPE LockGuard _guard_ = LockGuard((pthread_mutex_t *)&mutex_)

/**
*  simple mutex lock mechanism.
*  create a stack object of LockGuard while passing the mutex to lock
*  if the LockGuard runs out of scope (destructor called) it will
*  unlock the mutex automatically
*/
class LockGuard {
 private:
  pthread_mutex_t *mutex_;

 public:
  LockGuard(pthread_mutex_t *mutex_);
  ~LockGuard();
};

/**
 *  Use this class for objects which should be read and write lockable
 *  CAUTION!!
 *   - Up- and downgrading locks may introduce race conditions!
 *   - currently this type is not supported by the LockGuard
 *
 *  Usage: see ThreadSafeMutex, works exactly the same
 */
class ThreadSafeReadWrite {
 public:
  ThreadSafeReadWrite();
  virtual ~ThreadSafeReadWrite();
  
 protected:
  inline void ReadLock() const { pthread_rwlock_rdlock((pthread_rwlock_t*)&read_write_lock_); }
  inline void WriteLock() const { pthread_rwlock_wrlock((pthread_rwlock_t*)&read_write_lock_); }
  inline void Unlock() const { pthread_rwlock_unlock((pthread_rwlock_t*)&read_write_lock_); }
  
  /**
   *  to upgrade a lock we have to give away the read lock and acquire a write lock afterwards
   *  this may introduce race conditions, as these two steps are not performed atomically
   *  keep this in mind !!
   */
  inline void UpgradeLock() const { Unlock(); WriteLock(); }
  inline void DowngradeLock() const { Unlock(); ReadLock(); }
  
 private:
  pthread_rwlock_t read_write_lock_;
};

}

#endif /* THREAD_SAFE_H */
