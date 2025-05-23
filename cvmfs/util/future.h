/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_FUTURE_H_
#define CVMFS_UTIL_FUTURE_H_

#include <pthread.h>

#include <cassert>

#include "util/mutex.h"
#include "util/single_copy.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * This is a simple implementation of a Future wrapper template.
 * It is used as a proxy for results that are computed asynchronously and might
 * not be available on the first access.
 * Since this is a very simple implementation one needs to use the Future's
 * Get() and Set() methods to obtain the containing data resp. to write it.
 * Note: More than a single call to Set() is prohibited!
 *       If Get() is called before Set() the calling thread will block until
 *       the value has been set by a different thread.
 *
 * @param T  the value type wrapped by this Future template
 */
template<typename T>
class Future : SingleCopy {
 public:
  Future() : object_was_set_(false) {
    int retval = pthread_mutex_init(&mutex_, NULL);
    assert(retval == 0);
    retval = pthread_cond_init(&object_set_, NULL);
    assert(retval == 0);
  }

  ~Future() {
    int retval = pthread_cond_destroy(&object_set_);
    assert(retval == 0);
    retval = pthread_mutex_destroy(&mutex_);
    assert(retval == 0);
  }

  /**
   * Save an asynchronously computed value into the Future. This potentially
   * unblocks threads that already wait for the value.
   * @param object  the value object to be set
   */
  void Set(const T &object) {
    MutexLockGuard guard(mutex_);
    assert(!object_was_set_);
    object_ = object;
    object_was_set_ = true;
    pthread_cond_broadcast(&object_set_);
  }

  /**
   * Retrieves the wrapped value object. If the value is not yet available it
   * will automatically block until a different thread calls Set().
   * @return  the containing value object
   */
  T &Get() {
    Wait();
    return object_;
  }

  const T &Get() const {
    Wait();
    return object_;
  }

 private:
  void Wait() const {
    MutexLockGuard guard(mutex_);
    while (!object_was_set_) {
      pthread_cond_wait(&object_set_, &mutex_);
    }
  }

  T object_;
  mutable pthread_mutex_t mutex_;
  mutable pthread_cond_t object_set_;
  bool object_was_set_;
};

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_FUTURE_H_
