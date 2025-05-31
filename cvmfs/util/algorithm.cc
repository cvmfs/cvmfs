/**
 * This file is part of the CernVM File System.
 *
 * Some common functions.
 */

#ifndef __STDC_FORMAT_MACROS
// NOLINTNEXTLINE
#define __STDC_FORMAT_MACROS
#endif

#include "util/algorithm.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "util/string.h"


#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

bool HighPrecisionTimer::g_is_enabled = false;


double DiffTimeSeconds(struct timeval start, struct timeval end) {
  // Time subtraction, from GCC documentation
  if (end.tv_usec < start.tv_usec) {
    const int64_t nsec = (end.tv_usec - start.tv_usec) / 1000000 + 1;
    start.tv_usec -= 1000000 * nsec;
    start.tv_sec += nsec;
  }
  if (end.tv_usec - start.tv_usec > 1000000) {
    const int64_t nsec = (end.tv_usec - start.tv_usec) / 1000000;
    start.tv_usec += 1000000 * nsec;
    start.tv_sec -= nsec;
  }

  // Compute the time remaining to wait in microseconds.
  // tv_usec is certainly positive.
  const uint64_t elapsed_usec =
      ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec);
  return static_cast<double>(elapsed_usec) / 1000000.0;
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
  end_ = timeval();
  running_ = false;
}


double StopWatch::GetTime() const {
  assert(!running_);

  return DiffTimeSeconds(start_, end_);
}

namespace {

static unsigned int CountDigits(uint64_t n) {
  if (n == 0) {
    return 1;
  }
  return static_cast<unsigned int>(floor(log10(static_cast<double>(n)))) + 1;
}

static std::string GenerateStars(unsigned int n) { return std::string(n, '*'); }

}  // anonymous namespace

Log2Histogram::Log2Histogram(unsigned int nbins) {
  assert(nbins != 0);
  this->bins_.assign(nbins + 1, 0);             // +1 for overflow bin.
  this->boundary_values_.assign(nbins + 1, 0);  // +1 to avoid big if statement

  unsigned int i;
  for (i = 1; i <= nbins; i++) {
    this->boundary_values_[i] = (1 << ((i - 1) + 1));
  }
}

std::vector<atomic_int32> UTLog2Histogram::GetBins(const Log2Histogram &h) {
  return h.bins_;
}

unsigned int Log2Histogram::GetQuantile(float n) {
  const uint64_t total = this->N();
  // pivot is the index of the element corresponding to the requested quantile
  uint64_t pivot = static_cast<uint64_t>(static_cast<float>(total) * n);
  float normalized_pivot = 0.0;
  // now we iterate through all the bins
  // note that we _exclude_ the overflow bin
  unsigned int i = 0;
  for (i = 1; i <= this->bins_.size() - 1; i++) {
    const unsigned int bin_value =
        static_cast<unsigned int>(atomic_read32(&(this->bins_[i])));
    if (pivot <= bin_value) {
      normalized_pivot = static_cast<float>(pivot)
                         / static_cast<float>(bin_value);
      break;
    }
    pivot -= bin_value;
  }
  if (i >= this->bins_.size()) {
    return this->boundary_values_[this->bins_.size() - 1];
  }
  // now i stores the index of the bin corresponding to the requested quantile
  // and normalized_pivot is the element we want inside the bin
  const unsigned int min_value = this->boundary_values_[i - 1];
  const unsigned int max_value = this->boundary_values_[i];
  // and we return the linear interpolation
  return min_value
         + static_cast<unsigned int>(static_cast<float>(max_value - min_value)
                                     * normalized_pivot);
}

std::string Log2Histogram::ToString() {
  unsigned int i = 0;

  unsigned int max_left_boundary_count = 1;
  unsigned int max_right_boundary_count = 1;
  unsigned int max_value_count = 1;
  unsigned int max_stars = 0;
  unsigned int max_bins = 0;
  const unsigned int total_stars = 38;
  uint64_t total_sum_of_bins = 0;

  for (i = 1; i <= this->bins_.size() - 1; i++) {
    max_left_boundary_count = std::max(max_left_boundary_count,
                                       CountDigits(boundary_values_[i] / 2));
    max_right_boundary_count = std::max(max_right_boundary_count,
                                        CountDigits(boundary_values_[i] - 1));
    max_value_count = std::max(max_value_count,
                               CountDigits(atomic_read32(&(this->bins_[i]))));
    max_bins = std::max(
        max_bins, static_cast<unsigned int>(atomic_read32(&(this->bins_[i]))));
    total_sum_of_bins += static_cast<unsigned int>(
        atomic_read32(&(this->bins_[i])));
  }

  max_bins = std::max(
      max_bins, static_cast<unsigned int>(atomic_read32(&(this->bins_[0]))));
  total_sum_of_bins += static_cast<unsigned int>(
      atomic_read32(&(this->bins_[0])));

  if (total_sum_of_bins != 0) {
    max_stars = max_bins * total_stars / total_sum_of_bins;
  }

  const std::string format =
      " %" +
      StringifyUint(max_left_boundary_count < 2 ? 2 : max_left_boundary_count) +
      "d -> %" + StringifyUint(max_right_boundary_count) + "d :     %" +
      StringifyUint(max_value_count) + "d | %" +
      StringifyUint(max_stars < 12 ? 12 : max_stars) + "s |\n";

  const std::string title_format =
      " %" +
      StringifyUint(
          (max_left_boundary_count < 2 ? 2 : max_left_boundary_count) +
          max_right_boundary_count + 4) +
      "s | %" + StringifyUint(max_value_count + 4) + "s | %" +
      StringifyUint(max_stars < 12 ? 12 : max_stars) + "s |\n";

  const std::string overflow_format =
      "%" +
      StringifyUint(max_left_boundary_count + max_right_boundary_count + 5) +
      "s : %" + StringifyUint(max_value_count + 4) + "d | %" +
      StringifyUint(max_stars < 12 ? 12 : max_stars) + "s |\n";

  const std::string total_format =
      "%" +
      StringifyUint(max_left_boundary_count + max_right_boundary_count + 5 < 8
                        ? 8
                        : max_left_boundary_count + max_right_boundary_count +
                              5) +
      "s : %" + StringifyUint(max_value_count + 4) + "lld\n";

  std::string result_string = "";

  const unsigned int kBufSize = 300;
  char buffer[kBufSize];
  memset(buffer, 0, sizeof(buffer));

  snprintf(
      buffer, kBufSize, title_format.c_str(), "nsec", "count", "distribution");
  result_string += buffer;
  memset(buffer, 0, sizeof(buffer));

  for (i = 1; i <= this->bins_.size() - 1; i++) {
    unsigned int n_of_stars = 0;
    if (total_sum_of_bins != 0) {
      n_of_stars = static_cast<unsigned int>(atomic_read32(&(this->bins_[i])))
                   * total_stars / total_sum_of_bins;
    }

    snprintf(buffer,
             kBufSize,
             format.c_str(),
             boundary_values_[i - 1],
             boundary_values_[i] - 1,
             static_cast<unsigned int>(atomic_read32(&this->bins_[i])),
             GenerateStars(n_of_stars).c_str());
    result_string += buffer;
    memset(buffer, 0, sizeof(buffer));
  }

  unsigned int n_of_stars = 0;
  if (total_sum_of_bins != 0) {
    n_of_stars = static_cast<unsigned int>(atomic_read32(&(this->bins_[0])))
                 * total_stars / total_sum_of_bins;
  }

  snprintf(buffer,
           kBufSize,
           overflow_format.c_str(),
           "overflow",
           static_cast<unsigned int>(atomic_read32(&(this->bins_[0]))),
           GenerateStars(n_of_stars).c_str());
  result_string += buffer;
  memset(buffer, 0, sizeof(buffer));

  snprintf(buffer, kBufSize, total_format.c_str(), "total", total_sum_of_bins);
  result_string += buffer;
  memset(buffer, 0, sizeof(buffer));

  const float qs[15] = {.1,  .2, .25, .3,  .4,  .5,   .6,  .7,
                        .75, .8, .9,  .95, .99, .995, .999};
  snprintf(buffer, kBufSize,
           "\n\nQuantiles\n"
           "%0.4f,%0.4f,%0.4f,%0.4f,%0.4f,%0.4f,%0.4f,%0.4f,%0.4f,%0.4f,%0.4f,"
           "%0.4f,%0.4f,%0.4f,%0.4f\n"
           "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n"
           "End Quantiles"
           "\n-----------------------\n",
           qs[0], qs[1], qs[2], qs[3], qs[4], qs[5], qs[6], qs[7], qs[8], qs[9],
           qs[10], qs[11], qs[12], qs[13], qs[14], GetQuantile(qs[0]),
           GetQuantile(qs[1]), GetQuantile(qs[2]), GetQuantile(qs[3]),
           GetQuantile(qs[4]), GetQuantile(qs[5]), GetQuantile(qs[6]),
           GetQuantile(qs[7]), GetQuantile(qs[8]), GetQuantile(qs[9]),
           GetQuantile(qs[10]), GetQuantile(qs[11]), GetQuantile(qs[12]),
           GetQuantile(qs[13]), GetQuantile(qs[14]));

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
