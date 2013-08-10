/**
 * This file is part of the CernVM File System.
 *
 * Exponential backoff (sleep) with cutoff.
 */

#include "cvmfs_config.h"
#include "backoff.h"

#include <ctime>
#include "util.h"

using namespace std;  // NOLINT

void BackoffThrottle::Init(const unsigned init_delay_ms,
                           const unsigned max_delay_ms,
                           const unsigned reset_after_ms)
{
  Reset();
  init_delay_ms_ = init_delay_ms;
  max_delay_ms_ = max_delay_ms;
  reset_after_ms_ = reset_after_ms;
  prng_.InitLocaltime();
}


void BackoffThrottle::Reset() {
  delay_range_ = 0;
  last_throttle_ = 0;
}


void BackoffThrottle::Throttle() {
  time_t now = time(NULL);
  if (now - last_throttle_ < reset_after_ms_) {
    if (delay_range_ < max_delay_ms_) {
      if (delay_range_ == 0)
        delay_range_ = init_delay_ms_;
      else
        delay_range_ *= 2;
    }
    unsigned delay = prng_.Next(delay_range_) + 1;
    if (delay > max_delay_ms_)
      delay = max_delay_ms_;
    SafeSleepMs(delay);
  }
  last_throttle_ = now;
}
