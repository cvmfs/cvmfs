/**
 * This file is part of the CernVM File System.
 */

#include "supervisor.h"

#include "util/logging.h"
#include "util/platform.h"

Supervisor::Supervisor(uint64_t max_retries, uint64_t interval_sec)
    : max_retries_(max_retries), interval_(interval_sec) { }

Supervisor::~Supervisor() { }

bool Supervisor::Run() {
  uint64_t retries = 0;
  uint64_t t0 = platform_monotonic_time();
  bool result = false;
  do {
    result = Task();
    const uint64_t t1 = platform_monotonic_time();
    if (t1 - t0 < interval_) {
      retries += 1;
    } else {
      t0 = t1;
      retries = 0;
    }
  } while (!result && (retries <= max_retries_));

  return result;
}
