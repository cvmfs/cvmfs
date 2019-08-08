/**
 * This file is part of the CernVM File System.
 *
 * Some common functions.
 */

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "cvmfs_config.h"
#include "util/algorithm.h"
#include "util/string.h"


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

namespace {

static unsigned int CountDigits(uint64_t n) {
  return (unsigned int)floor(log10(n) + 1);
}

static std::string GenerateStars(unsigned int n) {
  return std::string(n, '*');
}

}  // anonymous namespace

Log2Histogram::Log2Histogram(unsigned int nbins) {
  assert(nbins != 0);
  this->bins_.assign(nbins + 1, 0);  // +1 for overflow bin.
  this->boundary_values_.assign(nbins + 1, 0);  // +1 to avoid big if statement

  unsigned int i;
  for (i = 1; i <= nbins; i++) {
    this->boundary_values_[i] = (1 << ((i - 1) + 1));
  }
}

std::vector<atomic_int32> UTLog2Histogram::GetBins(const Log2Histogram &h) {
  return h.bins_;
}

std::string Log2Histogram::ToString() {
  unsigned int i = 0;

  unsigned int max_left_boundary_count = 1;
  unsigned int max_right_boundary_count = 1;
  unsigned int max_value_count = 1;
  unsigned int max_stars = 0;
  unsigned int max_bins = 0;
  unsigned int total_stars = 38;
  unsigned int total_sum_of_bins = 0;

  for (i = 1; i <= this->bins_.size() - 1; i++) {
    max_left_boundary_count = std::max(max_left_boundary_count,
                                CountDigits(boundary_values_[i] / 2));
    max_right_boundary_count = std::max(max_right_boundary_count,
                                CountDigits(boundary_values_[i] - 1));
    max_value_count = std::max(max_value_count, CountDigits(this->bins_[i]));
    max_bins = std::max(max_bins, (unsigned int)
                                atomic_read32(&(this->bins_[i])));
    total_sum_of_bins += (unsigned int)atomic_read32(&(this->bins_[i]));
  }

  max_bins = std::max(max_bins, (unsigned int)atomic_read32(&(this->bins_[0])));
  total_sum_of_bins += (unsigned int)atomic_read32(&(this->bins_[0]));

  max_stars = max_bins * total_stars / total_sum_of_bins;

  std::string format = " %" + StringifyUint(max_left_boundary_count < 2 ?
                                  2 : max_left_boundary_count) +
                  "d -> %" + StringifyUint(max_right_boundary_count) +
                  "d :     %" + StringifyUint(max_value_count) + "d | %" +
                  StringifyUint(max_stars) + "s |\n";

  std::string title_format = " %" +
                  StringifyUint((max_left_boundary_count < 2 ?
                              2 : max_left_boundary_count) +
                              max_right_boundary_count +
                              4) +
                  "s | %" + StringifyUint(max_value_count + 4) +
                  "s | %" + StringifyUint(max_stars) + "s |\n";

  std::string overflow_format = "%" +
                  StringifyUint(max_left_boundary_count +
                              max_right_boundary_count +
                              5) +
                  "s : %" + StringifyUint(max_value_count + 4) +
                  "d | %" + StringifyUint(max_stars) + "s |\n";

  std::string result_string = "";

  const unsigned int kBufSize = 200;
  char buffer[kBufSize];
  memset(buffer, 0, sizeof(buffer));

  snprintf(buffer,
      kBufSize,
      title_format.c_str(),
      "usec",
      "count",
      "distribution");
  result_string += buffer;
  memset(buffer, 0, sizeof(buffer));

  for (i = 1; i <= this->bins_.size() - 1; i++) {
    unsigned int n_of_stars = (unsigned int)
                              atomic_read32(&(this->bins_[i])) *
                              total_stars / total_sum_of_bins;
    snprintf(buffer,
            kBufSize,
            format.c_str(),
            boundary_values_[i - 1],
            boundary_values_[i] - 1,
            (unsigned int)atomic_read32(&this->bins_[i]),
            GenerateStars(n_of_stars).c_str());
    result_string += buffer;
    memset(buffer, 0, sizeof(buffer));
  }

  unsigned int n_of_stars = (unsigned int)
                              atomic_read32(&(this->bins_[0])) *
                              total_stars / total_sum_of_bins;
  snprintf(buffer,
          kBufSize,
          overflow_format.c_str(),
          "overflow",
          (unsigned int)atomic_read32(&(this->bins_[0])),
          GenerateStars(n_of_stars).c_str());
  result_string += buffer;
  memset(buffer, 0, sizeof(buffer));

  return result_string;
}

void Log2Histogram::PrintLog2Histogram() {
  printf("%s", this->ToString().c_str());
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
