/**
 * This file is part of the CernVM File System.
 */

#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/socket.h>

#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "telemetry_aggregator_influx.h"
#include "util/logging.h"
#include "util/posix.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace perf {

TelemetryAggregatorInflux *TelemetryAggregatorInflux::Create(
                                Statistics* statistics,
                                uint64_t maximum_send_rate,
                                OptionsManager *options_mgr, std::string fqrn) {
  UniquePtr<TelemetryAggregatorInflux>
    telemetry(new TelemetryAggregatorInflux(statistics, maximum_send_rate,
                                      options_mgr, fqrn));

  LogCvmfs(kLogTalk, kLogDebug, "TelemetryAggregatorInflux created");

  return telemetry.Release();
}

TelemetryAggregatorInflux::~TelemetryAggregatorInflux() {
  allow_spawning_ = false;
}

TelemetryAggregatorInflux::TelemetryAggregatorInflux(Statistics* statistics,
                            uint64_t maximum_send_rate,
                            OptionsManager *options_mgr, std::string fqrn) :
                            TelemetryAggregator(statistics, maximum_send_rate,
                                                options_mgr, fqrn) {
  old_counters_.clear();

  int params = 0;

  if (options_mgr_->GetValue("CVMFS_INFLUX_HOST", &influx_host_)) {
      params++;
  }

  std::string opt;
  if (options_mgr_->GetValue("CVMFS_INFLUX_PORT", &opt)) {
      influx_port_ = atoi(opt.c_str());
      if (influx_port_ > 0 && influx_port_ < 65536) {
        params++;
      } else {
        LogCvmfs(kLogCvmfs, kLogDebug, "Invalid value for "
                                       "CVMFS_INFLUX_PORT [%s]", opt.c_str() );
      }
  }

  if (options_mgr_->GetValue("CVMFS_INFLUX_METRIC_NAME", &influx_metric_name_))
  {
      params++;
  }

  if (!options_mgr_->GetValue("CVMFS_INFLUX_EXTRA_TAGS", &influx_extra_tags_)) {
    influx_extra_tags_ = "";
  }

  if (!options_mgr_->GetValue("CVMFS_INFLUX_EXTRA_FIELDS",
                              &influx_extra_fields_)) {
    influx_extra_fields_ = "";
  }

  if (params == 3) {
    allow_spawning_ = true;
    LogCvmfs(kLogCvmfs, kLogDebug, "Enabling influx metrics. "
                                    "Send to [%s:%d] metric [%s]. Extra tags"
                                    "[%s]. Extra fields [%s].",
                                    influx_host_.c_str(),
                                    influx_port_,
                                    influx_metric_name_.c_str(),
                                    influx_extra_tags_.c_str(),
                                    influx_extra_fields_.c_str());
  } else {
    allow_spawning_ = false;
    LogCvmfs(kLogCvmfs, kLogDebug,
             "Not enabling influx metrics. Not all variables set");
  }
}

std::string TelemetryAggregatorInflux::MakePayload() {
  char buf[100];

  const uint64_t revision = counters_["catalog_revision"].counter.Get();
  snprintf(buf, sizeof(buf), "%lu", revision);
  std::string ret = "" + influx_metric_name_ + "_absolute,repo=" + fqrn_;

  if (influx_extra_tags_ != "") {
    ret += "," + influx_extra_tags_;
  }

  ret += " ";

  std::string tok = "";
  for (std::map<std::string, Statistics::CounterInfo>::iterator it
      = counters_.begin(), iEnd = counters_.end(); it != iEnd; it++) {
    int64_t value = it->second.counter.Get();
    if (value != 0) {
      snprintf(buf, sizeof(buf), "%ld", value);
      ret += tok + it->first + "=" + std::string(buf);

      tok = ",";
    }
  }
  if (influx_extra_fields_ != "") {
    ret += tok + influx_extra_fields_;
  }

  snprintf(buf, sizeof(buf), "%lu", monotonic_clock_);
  ret += " " + std::string(buf);

  return ret;
}

std::string TelemetryAggregatorInflux::MakeDeltaPayload() {
  char buf[100];

  const uint64_t revision = counters_["catalog_revision"].counter.Get();
  snprintf(buf, sizeof(buf), "%lu", revision);
  std::string ret = "" + influx_metric_name_ + "_delta,repo=" + fqrn_;

  if (influx_extra_tags_ != "") {
    ret += "," + influx_extra_tags_;
  }

  ret += " ";

  std::string tok = ",";
  for (std::map<std::string, Statistics::CounterInfo>::iterator it
      = counters_.begin(), iEnd = counters_.end(); it != iEnd; it++) {
    int64_t value = it->second.counter.Get();
    int64_t old_value = old_counters_.at(it->first).counter.Get();
    if (value != 0) {
      snprintf(buf, sizeof(buf), "%ld", value - old_value);
      ret += tok + it->first + "_delta=" + std::string(buf);
      tok = ",";
    }
  }
  if (influx_extra_fields_ != "") {
    ret += tok + influx_extra_fields_;
  }

  snprintf(buf, sizeof(buf), "%lu", monotonic_clock_);
  ret += " " + std::string(buf);

  return ret;
}

int TelemetryAggregatorInflux::SendToInflux(std::string payload) {
    const char *hostname = influx_host_.c_str();
    int port = influx_port_;

    struct addrinfo hints, *res;
    struct sockaddr_in *s = NULL;
    int err;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    err = getaddrinfo(influx_host_.c_str(), NULL, &hints, &res);
    if (err != 0 || res == NULL) {
       LogCvmfs(kLogCvmfs, kLogDebug,
                "Failed to resolve influx server [%s]. errno=%d",
                hostname, errno);
       return 1;
    }

    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0)  {
       LogCvmfs(kLogCvmfs, kLogDebug, "Failed to open socket");
       freeaddrinfo(res);
       return 2;
    }

    s = reinterpret_cast<sockaddr_in*>(res->ai_addr);
    s->sin_port = htons(port);

    int n = sendto(sockfd, payload.c_str(), strlen(payload.c_str()), 0,
                  (struct sockaddr*)s, sizeof(struct sockaddr_in));

    if (n < 0)  {
      close(sockfd);
      freeaddrinfo(res);
      LogCvmfs(kLogCvmfs, kLogDebug, "Failed to send to influx. errno=%d",
                                                                errno);
      return 3;
    }

    close(sockfd);
    freeaddrinfo(res);

    LogCvmfs(kLogCvmfs, kLogDebug, "INFLUX: POSTING [%s]", payload.c_str() );
    return 0;
}


void TelemetryAggregatorInflux::PushMetrics() {
  // create payload
  std::string payload = MakePayload();
  std::string delta_payload = "";
  if (old_counters_.size() > 0) {
    delta_payload = MakeDeltaPayload();
    payload += "\n" + delta_payload;
  }

  // send to influx
  SendToInflux(payload);

  // current counter is now old counter
  counters_.swap(old_counters_);
}

}  // namespace perf

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

