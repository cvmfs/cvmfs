/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_TELEMETRY_AGGREGATOR_INFLUX_H_
#define CVMFS_TELEMETRY_AGGREGATOR_INFLUX_H_

#include <pthread.h>
#include <stdint.h>

#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "options.h"
#include "statistics.h"
#include "telemetry_aggregator.h"
#include "util/single_copy.h"

namespace perf {

class TelemetryAggregatorInflux : TelemetryAggregator {
 public:
  TelemetryAggregatorInflux(Statistics* statistics,
                            uint64_t maximum_send_rate,
                            OptionsManager *options_mgr,
                            const std::string &fqrn);
  virtual ~TelemetryAggregatorInflux();

 private:
  std::map<std::string, int64_t> old_counters_;
  std::string influx_host_;
  int influx_port_;
  std::string influx_metric_name_;
  std::string influx_extra_fields_;
  std::string influx_extra_tags_;

  std::string MakePayload();
  std::string MakeDeltaPayload();
  TelemetryError SendToInflux(const std::string &payload);

  virtual void PushMetrics();
};

}  // namespace perf

#endif  // CVMFS_TELEMETRY_AGGREGATOR_INFLUX_H_
