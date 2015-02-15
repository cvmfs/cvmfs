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
#include "util.h"

namespace perf {

/**
 * A wrapper around an atomic 64bit signed integer.
 */
class Counter {
 public:
  Counter() { atomic_init64(&counter_); }
  inline void Inc() { atomic_inc64(&counter_); }
  inline void Dec() { atomic_dec64(&counter_); }
  inline int64_t Get() { return atomic_read64(&counter_); }
  inline void Set(const int64_t val) { atomic_write64(&counter_, val); }
  inline int64_t Xadd(const int64_t delta) {
    return atomic_xadd64(&counter_, delta);
  }

  std::string Print() { return StringifyInt(Get()); }
  std::string PrintK() { return StringifyInt(Get() / 1000); }
  std::string PrintKi() { return StringifyInt(Get() / 1024); }
  std::string PrintM() { return StringifyInt(Get() / (1000 * 1000)); }
  std::string PrintMi() { return StringifyInt(Get() / (1024 * 1024)); }
  std::string PrintRatio(Counter divider) {
    double enumerator_value = Get();
    double divider_value = divider.Get();
    return StringifyDouble(enumerator_value / divider_value);
  }

 private:
  atomic_int64 counter_;
};


/**
 * A collection of Counter objects with a name and a description.  Counters in
 * a Statistics class have a name and a description.  Thread-safe.
 */
class Statistics : SingleCopy {
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
  struct CounterInfo {
    explicit CounterInfo(const std::string &desc) : desc(desc) { }
    Counter counter;
    std::string desc;
  };
  std::map<std::string, CounterInfo *> counters_;
  pthread_mutex_t *lock_;
};

}  // namespace perf

#endif  // CVMFS_STATISTICS_H_
