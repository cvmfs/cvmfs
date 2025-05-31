/**
 * This file is part of the CernVM File System.
 *
 * Exponential backoff (sleep) with cutoff.
 */


#include "backoff.h"

#include <ctime>

#include "util/logging.h"
#include "util/posix.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

void BackoffThrottle::Init(const unsigned init_delay_ms,
                           const unsigned max_delay_ms,
                           const unsigned reset_after_ms) {
  init_delay_ms_ = init_delay_ms;
  max_delay_ms_ = max_delay_ms;
  reset_after_ms_ = reset_after_ms;
  prng_.InitLocaltime();

  lock_ = reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  const int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);

  Reset();
}


BackoffThrottle::~BackoffThrottle() {
  pthread_mutex_destroy(lock_);
  free(lock_);
}


void BackoffThrottle::Reset() {
  pthread_mutex_lock(lock_);
  delay_range_ = 0;
  last_throttle_ = 0;
  pthread_mutex_unlock(lock_);
}


void BackoffThrottle::Throttle() {
  const time_t now = time(NULL);

  pthread_mutex_lock(lock_);
  if (unsigned(now - last_throttle_) < reset_after_ms_ / 1000) {
    if (delay_range_ < max_delay_ms_) {
      if (delay_range_ == 0)
        delay_range_ = init_delay_ms_;
      else
        delay_range_ *= 2;
    }
    unsigned delay = prng_.Next(delay_range_) + 1;
    if (delay > max_delay_ms_)
      delay = max_delay_ms_;

    pthread_mutex_unlock(lock_);
    LogCvmfs(kLogCvmfs, kLogDebug, "backoff throttle %d ms", delay);
    SafeSleepMs(delay);
    pthread_mutex_lock(lock_);
  }
  last_throttle_ = now;
  pthread_mutex_unlock(lock_);
}
