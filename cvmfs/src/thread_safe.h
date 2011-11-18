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
class ThreadSafe {
 public:
  ThreadSafe();
  virtual ~ThreadSafe();
  
 protected:
  inline void Lock() { pthread_mutex_lock(&mutex_); }
  inline void Unlock() { pthread_mutex_unlock(&mutex_); }
 
 protected:
  pthread_mutex_t mutex_;
};

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
 *  use this macro to mark scopes to secure by the mutex
 *  see also: the description of ThreadSafe
 */
#define LOCKED_SCOPE LockGuard _guard_ = LockGuard((pthread_mutex_t *)&mutex_)

}

#endif /* THREAD_SAFE_H */
