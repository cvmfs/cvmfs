/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_TELEMETRY_AGGREGATOR_INFLUX_H_
#define CVMFS_TELEMETRY_AGGREGATOR_INFLUX_H_

#include <netdb.h>
#include <pthread.h>
#include <stdint.h>

#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "gtest/gtest_prod.h"
#include "options.h"
#include "statistics.h"
#include "telemetry_aggregator.h"
#include "util/single_copy.h"

namespace perf {

class TelemetryAggregatorInflux : TelemetryAggregator {
  FRIEND_TEST(T_TelemetryAggregator, EmptyCounters);
  FRIEND_TEST(T_TelemetryAggregator, FailCreate);
  FRIEND_TEST(T_TelemetryAggregator, ExtraFields_Tags);
  FRIEND_TEST(T_TelemetryAggregator, UpdateCounters_WithExtraFields_Tags);
  FRIEND_TEST(T_TelemetryAggregator, SendDeltaDisabled);
  FRIEND_TEST(T_TelemetryAggregator, SendDeltaEnabledByDefault);
  FRIEND_TEST(T_TelemetryAggregator, SendDeltaExplicitlyEnabled);

 public:
  TelemetryAggregatorInflux(Statistics *statistics,
                            int send_rate_sec,
                            OptionsManager *options_mgr,
                            MountPoint *mount_point,
                            const std::string &fqrn);
  virtual ~TelemetryAggregatorInflux();

 private:
  std::map<std::string, int64_t> old_counters_;
  std::string influx_host_;
  int influx_port_;
  std::string influx_metric_name_;
  std::string influx_extra_fields_;
  std::string influx_extra_tags_;
  bool send_delta_;
  int socket_fd_;
  struct addrinfo *res_;

  std::string MakePayload();
  std::string MakeDeltaPayload();
  TelemetryReturn OpenSocket();
  TelemetryReturn SendToInflux(const std::string &payload);

  virtual void PushMetrics();
};

}  // namespace perf

#endif  // CVMFS_TELEMETRY_AGGREGATOR_INFLUX_H_
