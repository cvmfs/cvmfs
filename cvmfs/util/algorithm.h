/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_ALGORITHM_H_
#define CVMFS_UTIL_ALGORITHM_H_

#include <sys/time.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "util/atomic.h"
#include "util/export.h"
#include "util/murmur.hxx"
#include "util/platform.h"
#include "util/prng.h"
#include "util/single_copy.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif


CVMFS_EXPORT double DiffTimeSeconds(struct timeval start, struct timeval end);

// Bitfield manipulation for different integer types T
template<typename T>
inline void SetBit(unsigned int bit, T *field) {
  *field |= static_cast<T>(1) << bit;
}

template<typename T>
inline void ClearBit(unsigned int bit, T *field) {
  *field &= ~(static_cast<T>(1) << bit);
}

template<typename T>
inline bool TestBit(unsigned int bit, const T field) {
  return field & (static_cast<T>(1) << bit);
}


/**
 * Knuth's random shuffle algorithm.
 */
template<typename T>
std::vector<T> Shuffle(const std::vector<T> &input, Prng *prng) {
  std::vector<T> shuffled(input);
  unsigned N = shuffled.size();
  // No shuffling for the last element
  for (unsigned i = 0; i < N; ++i) {
    const unsigned swap_idx = i + prng->Next(N - i);
    std::swap(shuffled[i], shuffled[swap_idx]);
  }
  return shuffled;
}


/**
 * Sorts the vector tractor and applies the same permutation to towed.  Both
 * vectors have to be of the same size.  Type T must be sortable (< operator).
 * Uses insertion sort (n^2), only efficient for small vectors.
 */
template<typename T, typename U>
void SortTeam(std::vector<T> *tractor, std::vector<U> *towed) {
  assert(tractor);
  assert(towed);
  assert(tractor->size() == towed->size());
  int N = tractor->size();

  // Insertion sort on both, tractor and towed
  for (int i = 1; i < N; ++i) {
    T val_tractor = (*tractor)[i];
    U val_towed = (*towed)[i];
    int pos;
    for (pos = i - 1; (pos >= 0) && ((*tractor)[pos] > val_tractor); --pos) {
      (*tractor)[pos + 1] = (*tractor)[pos];
      (*towed)[pos + 1] = (*towed)[pos];
    }
    (*tractor)[pos + 1] = val_tractor;
    (*towed)[pos + 1] = val_towed;
  }
}


template<typename hashed_type>
struct hash_murmur {
  size_t operator()(const hashed_type key) const {
#ifdef __x86_64__
    return MurmurHash64A(&key, sizeof(key), 0x9ce603115bba659bLLU);
#else
    return MurmurHash2(&key, sizeof(key), 0x07387a4f);
#endif
  }
};


/**
 * Very simple StopWatch implementation. Currently the implementation does not
 * allow a restart of a stopped watch. You should always reset the clock before
 * you reuse it.
 *
 * Stopwatch watch();
 * watch.Start();
 * // do nasty thing
 * watch.Stop();
 * printf("%f", watch.GetTime());
 */
class CVMFS_EXPORT StopWatch : SingleCopy {
 public:
  StopWatch() : running_(false) { }

  void Start();
  void Stop();
  void Reset();

  double GetTime() const;

 private:
  bool running_;
  timeval start_, end_;
};


/**
 * Log2Histogram is a simple implementation of
 * log2 histogram data structure which stores
 * and prints log2 histogram. It is used for
 * getting and printing latency metrics of
 * CVMFS fuse calls.
 *
 * Log2Histogram hist(2);
 * hist.Add(1);
 * hist.Add(2);
 * hist.PrintLog2Histogram();
 */

class CVMFS_EXPORT Log2Histogram {
  friend class UTLog2Histogram;

 public:
  explicit Log2Histogram(unsigned int nbins);

  void Add(uint64_t value) {
    unsigned int i;
    const unsigned int n = this->bins_.size() - 1;

    for (i = 1; i <= n; i++) {
      if (value < this->boundary_values_[i]) {
        atomic_inc32(&(this->bins_[i]));
        return;
      }
    }

    atomic_inc32(&(this->bins_[0]));  // add to overflow bin.
  }

  /**
   * Returns the total number of elements in the histogram
   */
  inline uint64_t N() {
    uint64_t n = 0;
    unsigned int i;
    for (i = 0; i <= this->bins_.size() - 1; i++) {
      n += static_cast<unsigned int>(atomic_read32(&(this->bins_[i])));
    }
    return n;
  }

  /**
   * compute the quantile of order n
   */
  unsigned int GetQuantile(float n);

  std::string ToString();

  void PrintLog2Histogram();

 private:
  std::vector<atomic_int32> bins_;
  // boundary_values_ handle the largest value a certain
  // bin can store in itself.
  std::vector<unsigned int> boundary_values_;
};

/**
 * UTLog2Histogram class is a helper for the unit tests
 * to extract internals from Log2Histogram.
 */
class CVMFS_EXPORT UTLog2Histogram {
 public:
  std::vector<atomic_int32> GetBins(const Log2Histogram &h);
};


class CVMFS_EXPORT HighPrecisionTimer : SingleCopy {
 public:
  static bool g_is_enabled;  // false by default

  explicit HighPrecisionTimer(Log2Histogram *recorder)
      : timestamp_start_(g_is_enabled ? platform_monotonic_time_ns() : 0)
      , recorder_(recorder) { }

  ~HighPrecisionTimer() {
    if (g_is_enabled)
      recorder_->Add(platform_monotonic_time_ns() - timestamp_start_);
  }

 private:
  uint64_t timestamp_start_;
  Log2Histogram *recorder_;
};

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_ALGORITHM_H_
