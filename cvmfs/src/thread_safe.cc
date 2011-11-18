#include "thread_safe.h"

namespace cvmfs {

LockGuard::LockGuard(pthread_mutex_t *mutex) : mutex_(mutex) {
  pthread_mutex_lock(mutex_);
}

LockGuard::~LockGuard() {
  pthread_mutex_unlock(mutex_);
}

ThreadSafeMutex::ThreadSafeMutex() {
  pthread_mutex_init(&mutex_, NULL);
}

ThreadSafeMutex::~ThreadSafeMutex() {
  pthread_mutex_destroy(&mutex_);
}

ThreadSafeReadWrite::ThreadSafeReadWrite() {
  pthread_rwlock_init(&read_write_lock_, NULL);
}

ThreadSafeReadWrite::~ThreadSafeReadWrite() {
  pthread_rwlock_destroy(&read_write_lock_);
}

}
