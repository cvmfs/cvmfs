/**
 * This file is part of the CernVM File System.
 *
 * Some common functions.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "util/algorithm.h"

using namespace std;  // NOLINT

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif


double DiffTimeSeconds(struct timeval start, struct timeval end) {
  // Time substraction, from GCC documentation
  if (end.tv_usec < start.tv_usec) {
    int nsec = (end.tv_usec - start.tv_usec) / 1000000 + 1;
    start.tv_usec -= 1000000 * nsec;
    start.tv_sec += nsec;
  }
  if (end.tv_usec - start.tv_usec > 1000000) {
    int nsec = (end.tv_usec - start.tv_usec) / 1000000;
    start.tv_usec += 1000000 * nsec;
    start.tv_sec -= nsec;
  }

  // Compute the time remaining to wait in microseconds.
  // tv_usec is certainly positive.
  uint64_t elapsed_usec = ((end.tv_sec - start.tv_sec)*1000000) +
  (end.tv_usec - start.tv_usec);
  return static_cast<double>(elapsed_usec)/1000000.0;
}



void StopWatch::Start() {
  assert(!running_);

  gettimeofday(&start_, NULL);
  running_ = true;
}


void StopWatch::Stop() {
  assert(running_);

  gettimeofday(&end_, NULL);
  running_ = false;
}


void StopWatch::Reset() {
  start_ = timeval();
  end_   = timeval();
  running_ = false;
}


double StopWatch::GetTime() const {
  assert(!running_);

  return DiffTimeSeconds(start_, end_);
}


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
