/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_TELEMETRY_AGGREGATOR_H_
#define CVMFS_TELEMETRY_AGGREGATOR_H_

#include <pthread.h>
#include <stdint.h>

#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "options.h"
#include "statistics.h"
#include "util/single_copy.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace perf {

class TelemetryAggregator : SingleCopy {
 public:
  static TelemetryAggregator *Create(Statistics* statistics, uint64_t send_rate,
                                     OptionsManager *options_mgr,
                                     std::string fqrn);
  virtual ~TelemetryAggregator();

  void Spawn();

 protected:
  Statistics* statistics_;
  const uint64_t maximum_send_rate_;
  OptionsManager *options_mgr_;
  int pipe_terminate_[2];
  pthread_t thread_telemetry_;
  std::string fqrn_;
  bool allow_spawning_;
  uint64_t monotonic_clock_;
  std::map<std::string, Statistics::CounterInfo> counters_;

  static void *MainTelemetry(void *data);

  TelemetryAggregator(Statistics* statistics, uint64_t maximum_send_rate,
                      OptionsManager *options_mgr, std::string fqrn) :
                      statistics_(statistics),
                      maximum_send_rate_(maximum_send_rate),
                      options_mgr_(options_mgr),
                      fqrn_(fqrn),
                      allow_spawning_(false),
                      monotonic_clock_(0) {
    pipe_terminate_[0] = pipe_terminate_[1] = -1;
    counters_.clear();
    memset(&thread_telemetry_, 0, sizeof(thread_telemetry_));
  }

  virtual void PushMetrics() {}
};

}  // namespace perf

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_TELEMETRY_AGGREGATOR_H_
