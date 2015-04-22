/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_STATISTICS_H_
#define CVMFS_STATISTICS_H_

#include <pthread.h>
#include <stdint.h>

#include <map>
#include <string>

#include "atomic.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif


namespace perf {

/**
 * A wrapper around an atomic 64bit signed integer.
 */
class Counter {
 public:
  Counter() { atomic_init64(&counter_); }
  void Inc() { atomic_inc64(&counter_); }
  void Dec() { atomic_dec64(&counter_); }
  int64_t Get() { return atomic_read64(&counter_); }
  void Set(const int64_t val) { atomic_write64(&counter_, val); }
  int64_t Xadd(const int64_t delta) { return atomic_xadd64(&counter_, delta); }

  std::string Print();
  std::string PrintK();
  std::string PrintKi();
  std::string PrintM();
  std::string PrintMi();
  std::string PrintRatio(Counter divider);
  std::string ToString();

 private:
  atomic_int64 counter_;
};

// perf::Func(Counter) is more clear to read in the code
inline void Dec(class Counter *counter) { counter->Dec(); }
inline void Inc(class Counter *counter) { counter->Inc(); }
inline void TryInc(class Counter *counter) {
  if (counter != NULL)
    counter->Inc();
}
inline int64_t Xadd(class Counter *counter, const int64_t delta) {
  return counter->Xadd(delta);
}

/**
 * A collection of Counter objects with a name and a description.  Counters in
 * a Statistics class have a name and a description.  Thread-safe.
 */
class Statistics {
 public:
  enum PrintOptions {
    kPrintSimple = 0,
    kPrintHeader
  };

  Statistics();
  ~Statistics();
  Counter *Register(const std::string &name, const std::string &desc);
  Counter *Lookup(const std::string &name);
  std::string LookupDesc(const std::string &name);
  std::string PrintList(const PrintOptions print_options);
 private:
  Statistics(const Statistics &other);
  Statistics& operator=(const Statistics &other);
  struct CounterInfo {
    explicit CounterInfo(const std::string &desc) : desc(desc) { }
    Counter counter;
    std::string desc;
  };
  std::map<std::string, CounterInfo *> counters_;
  pthread_mutex_t *lock_;
};

}  // namespace perf

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_STATISTICS_H_
