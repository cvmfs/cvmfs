/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "bridge/ds_stubs.h"

#include <cassert>

void compat::DentryTrackerV1::Lock() const {
  int retval = pthread_mutex_lock(lock_);
  assert(retval == 0);
}

void compat::DentryTrackerV1::Unlock() const {
  int retval = pthread_mutex_unlock(lock_);
  assert(retval == 0);
}

compat::DentryTrackerV1::~DentryTrackerV1() {
  // We know that the tracker for migration is not in spawned state
  assert(pipe_terminate_[1] < 0);
  pthread_mutex_destroy(lock_);
  free(lock_);
}
