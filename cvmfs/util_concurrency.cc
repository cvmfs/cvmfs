/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "util_concurrency.h"

#include <unistd.h>

#include <cassert>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

unsigned int GetNumberOfCpuCores() {
  const int numCPU = sysconf(_SC_NPROCESSORS_ONLN);

  if (numCPU <= 0) {
    LogCvmfs(kLogSpooler, kLogWarning, "Unable to determine the available "
                                       "number of processors in the system... "
                                       "falling back to default '%d'",
             kFallbackNumberOfCpus);
    return kFallbackNumberOfCpus;
  }

  return static_cast<unsigned int>(numCPU);
}

Signal::Signal() : fired_(false) {
  int retval = pthread_mutex_init(&lock_, NULL);
  assert(retval == 0);
  retval = pthread_cond_init(&signal_, NULL);
  assert(retval == 0);
}


Signal::~Signal() {
  pthread_cond_destroy(&signal_);
  pthread_mutex_destroy(&lock_);
}


void Signal::Wait() {
  MutexLockGuard guard(lock_);
  while (!fired_) {
    int retval = pthread_cond_wait(&signal_, &lock_);
    assert(retval == 0);
  }
}


void Signal::Wakeup() {
  MutexLockGuard guard(lock_);
  fired_ = true;
  int retval = pthread_cond_broadcast(&signal_);
  assert(retval == 0);
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
