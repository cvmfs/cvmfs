/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_BACKOFF_H_
#define CVMFS_BACKOFF_H_

#include <pthread.h>

#include "prng.h"
#include "util.h"

/**
 * When Throttle() is called in quick succession, the exponential backoff will
 * start (sleep).  The class forgets a call to Throttle() after reset_after_ms
 * milliseconds.
 */
class BackoffThrottle : public SingleCopy {
 public:
  static const unsigned kDefaultInitDelay = 32;  /**< 32ms */
  static const unsigned kDefaultMaxDelay = 2000; /**< Maximum 2 seconds */
  /**
   * Clear memory after 10s
   */
  static const unsigned kDefaultResetAfter = 10000;

  BackoffThrottle() {
    Init(kDefaultInitDelay, kDefaultMaxDelay, kDefaultResetAfter);
  }
  BackoffThrottle(const unsigned init_delay_ms,
                  const unsigned max_delay_ms,
                  const unsigned reset_after_ms)
  {
    Init(init_delay_ms, max_delay_ms, reset_after_ms);
  }
  ~BackoffThrottle();
  void Throttle();
  void Reset();

 private:
  void Init(const unsigned init_delay_ms,
            const unsigned max_delay_ms,
            const unsigned reset_after_ms);
  unsigned delay_range_;
  unsigned init_delay_ms_;
  unsigned max_delay_ms_;
  unsigned reset_after_ms_;
  time_t last_throttle_;
  Prng prng_;
  pthread_mutex_t *lock_;
};

#endif  // CVMFS_BACKOFF_H_
