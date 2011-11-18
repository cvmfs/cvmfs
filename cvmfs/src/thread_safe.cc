#include "thread_safe.h"

namespace cvmfs {

LockGuard::LockGuard(pthread_mutex_t *mutex) : mutex_(mutex) {
  pthread_mutex_lock(mutex_);
}

LockGuard::~LockGuard() {
  pthread_mutex_unlock(mutex_);
}

ThreadSafe::ThreadSafe() {
  pthread_mutex_init(&mutex_, NULL);
}

ThreadSafe::~ThreadSafe() {
  pthread_mutex_destroy(&mutex_);
}

}
