/**
 * This file is part of the CernVM File System.
 *
 * Some common functions.
 */

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

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

Log2Hist::Log2Hist(uint n)
{
  assert(n != 0);
  this->number_of_bins = n;
  this->bins = new atomic_int32[n + 1]; // +1 for overflow bin.
  this->boundary_values = new uint[n + 1]; // +1 to avoid giant if statement
  memset(this->bins, 0, sizeof(atomic_int32) * (n + 1));
  memset(this->boundary_values, 0, sizeof(uint) * (n + 1));

  uint i;
  for (i = 1; i <= n; i++)
  {
    this->boundary_values[i] = (1 << ((i - 1) + 1));
  }
}

Log2Hist::~Log2Hist()
{
  delete[] bins;
  delete[] boundary_values;
}

atomic_int32* Log2Hist::GetBins()
{
  return this->bins;
}

string Log2Hist::Print()
{
  uint i = 0;

  uint max_left_boundary_count = 1;
  uint max_right_boundary_count = 1;
  uint max_value_count = 1;
  uint max_stars = 0;
  uint max_bins = 0;
  uint total_stars = 38;
  uint total_sum_of_bins = 0;

  for (i = 1; i <= number_of_bins; i++)
  {
    max_left_boundary_count = max(max_left_boundary_count,
                                count_digits(boundary_values[i] / 2));
    max_right_boundary_count = max(max_right_boundary_count,
                                count_digits(boundary_values[i] - 1));
    max_value_count = max(max_value_count, count_digits(bins[i]));
    max_bins = max(max_bins, (uint)atomic_read32(&bins[i]));
    total_sum_of_bins += (uint)atomic_read32(&bins[i]);
  }

  max_bins = max(max_bins, (uint)atomic_read32(&bins[0]));
  total_sum_of_bins += (uint)atomic_read32(&bins[0]);


  max_stars = max_bins * total_stars / total_sum_of_bins;

  string format = " %" + to_string(max_left_boundary_count < 2 ? 2 : max_left_boundary_count) +
                  "d -> %" + to_string(max_right_boundary_count) +
                  "d :     %" + to_string(max_value_count) + "d | %" +
                  to_string(max_stars) + "s |\n";

  string title_format = " %" +
                  to_string((max_left_boundary_count < 2 ? 2 : max_left_boundary_count) +
                              max_right_boundary_count +
                              4) +
                  "s | %" + to_string(max_value_count + 4) +
                  "s | %" + to_string(max_stars) + "s |\n";

  string overflow_format = "%" +
                  to_string(max_left_boundary_count +
                              max_right_boundary_count +
                              5) +
                  "s : %" + to_string(max_value_count + 4) +
                  "d | %" + to_string(max_stars) + "s |\n";

  string result_string = "";

  char buffer[BUFFSIZE];
  memset(buffer, 0, sizeof(buffer));

  snprintf(buffer,
      BUFFSIZE,
      title_format.c_str(),
      "usec",
      "count",
      "distribution");
  result_string += buffer;
  memset(buffer, 0, sizeof(buffer));

  for (i = 1; i <= number_of_bins; i++)
  {
    uint n_of_stars = (uint)atomic_read32(&bins[i]) * total_stars / total_sum_of_bins;
    snprintf(buffer,
            BUFFSIZE,
            format.c_str(),
            boundary_values[i - 1],
            boundary_values[i] - 1,
            (uint)atomic_read32(&bins[i]),
            generate_stars(n_of_stars).c_str());
    result_string += buffer;
    memset(buffer, 0, sizeof(buffer));
  }

  uint n_of_stars = (uint)atomic_read32(&bins[0]) * total_stars / total_sum_of_bins;
  snprintf(buffer,
          BUFFSIZE,
          overflow_format.c_str(),
          "overflow",
          (uint)atomic_read32(&bins[0]),
          generate_stars(n_of_stars).c_str());
  result_string += buffer;
  memset(buffer, 0, sizeof(buffer));

  return result_string;
}

void Log2Hist::PrintLog2Hist()
{
  printf("%s", this->Print().c_str());
  fflush(stdout);
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
