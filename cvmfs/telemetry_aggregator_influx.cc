/**
 * This file is part of the CernVM File System.
 */

#include "telemetry_aggregator_influx.h"

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

#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

namespace perf {

TelemetryAggregatorInflux::~TelemetryAggregatorInflux() {
  is_zombie_ = true;
}

TelemetryAggregatorInflux::TelemetryAggregatorInflux(Statistics* statistics,
                                                    uint64_t maximum_send_rate,
                                                    OptionsManager *options_mgr,
                                                    const std::string &fqrn) :
                      TelemetryAggregator(statistics, maximum_send_rate, fqrn) {
  int params = 0;

  if (options_mgr->GetValue("CVMFS_INFLUX_HOST", &influx_host_)) {
    if (influx_host_.size() > 1) {
      params++;
    } else {
      LogCvmfs(kLogTelemetry, kLogDebug | kLogSyslogWarn,
                                        "No value given for CVMFS_INFLUX_HOST");
    }
  }

  std::string opt;
  if (options_mgr->GetValue("CVMFS_INFLUX_PORT", &opt)) {
      influx_port_ = String2Int64(opt.c_str());
      if (influx_port_ > 0 && influx_port_ < 65536) {
        params++;
      } else {
        LogCvmfs(kLogTelemetry, kLogDebug | kLogSyslogWarn,
                      "Invalid value for CVMFS_INFLUX_PORT [%s]", opt.c_str());
      }
  }

  if (options_mgr->GetValue("CVMFS_INFLUX_METRIC_NAME", &influx_metric_name_))
  {
      params++;
  }

  if (!options_mgr->GetValue("CVMFS_INFLUX_EXTRA_TAGS", &influx_extra_tags_)) {
    influx_extra_tags_ = "";
  }

  if (!options_mgr->GetValue("CVMFS_INFLUX_EXTRA_FIELDS",
                              &influx_extra_fields_)) {
    influx_extra_fields_ = "";
  }

  if (params == 3) {
    is_zombie_ = false;
    LogCvmfs(kLogTelemetry, kLogDebug, "Enabling influx metrics. "
                                    "Send to [%s:%d] metric [%s]. Extra tags"
                                    "[%s]. Extra fields [%s].",
                                    influx_host_.c_str(),
                                    influx_port_,
                                    influx_metric_name_.c_str(),
                                    influx_extra_tags_.c_str(),
                                    influx_extra_fields_.c_str());
  } else {
    is_zombie_ = true;
    LogCvmfs(kLogTelemetry, kLogDebug | kLogSyslogWarn,
             "Not enabling influx metrics. Not all mandatory variables set: "
             "CVMFS_INFLUX_METRIC_NAME, CVMFS_INFLUX_HOST, CVMFS_INFLUX_PORT");
  }
}

/**
 * Influx dataformat
 * ( https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol/ )
 * Syntax
   <measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]
 *
 * Example
   myMeasurement,tag1=value1,tag2=value2 fieldKey="fieldValue" 1556813561098000000
*/
std::string TelemetryAggregatorInflux::MakePayload() {
  // measurement and tags
  std::string ret = "" + influx_metric_name_ + "_absolute,repo=" + fqrn_;

  if (influx_extra_tags_ != "") {
    ret += "," + influx_extra_tags_;
  }

  // fields
  ret += " ";
  const int64_t revision = counters_["catalog_revision"];
  ret += "catalog_revision=" + StringifyUint(revision);

  std::string tok = ",";
  for (std::map<std::string, int64_t>::iterator it
      = counters_.begin(), iEnd = counters_.end(); it != iEnd; it++) {
    if (it->second != 0) {
      ret += tok + it->first + "=" + StringifyInt(it->second);

      tok = ",";
    }
  }
  if (influx_extra_fields_ != "") {
    ret += tok + influx_extra_fields_;
  }

  // timestamp
  ret += " " + StringifyUint(timestamp_);

  return ret;
}

/**
 * Influx dataformat
 * ( https://docs.influxdata.com/influxdb/cloud/reference/syntax/line-protocol/ )
 * Syntax
   <measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]
 *
 * Example
   myMeasurement,tag1=value1,tag2=value2 fieldKey="fieldValue" 1556813561098000000
*/
std::string TelemetryAggregatorInflux::MakeDeltaPayload() {
  // measurement and tags
  std::string ret = "" + influx_metric_name_ + "_delta,repo=" + fqrn_;

  if (influx_extra_tags_ != "") {
    ret += "," + influx_extra_tags_;
  }

  // fields
  ret += " ";
  const uint64_t revision = counters_["catalog_revision"];
  ret += "catalog_revision=" + StringifyUint(revision);

  std::string tok = ",";
  for (std::map<std::string, int64_t>::iterator it
      = counters_.begin(), iEnd = counters_.end(); it != iEnd; it++) {
    int64_t value = it->second;
    int64_t old_value = old_counters_.at(it->first);
    if (value != 0) {
      ret += tok + it->first + "=" + StringifyInt(value - old_value);
    }
  }

  if (influx_extra_fields_ != "") {
    ret += tok + influx_extra_fields_;
  }

  // timestamp
  ret += " " + StringifyUint(timestamp_);

  return ret;
}

TelemetryError TelemetryAggregatorInflux::SendToInflux(
                                            const std::string &payload) {
    const char *hostname = influx_host_.c_str();
    int port = influx_port_;

    struct addrinfo hints, *res;
    struct sockaddr_in *dest_addr = NULL;
    int err;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    err = getaddrinfo(influx_host_.c_str(), NULL, &hints, &res);
    if (err != 0 || res == NULL) {
       LogCvmfs(kLogTelemetry, kLogDebug | kLogSyslogErr,
                "Failed to resolve influx server [%s]. errno=%d",
                hostname, errno);
       return TelemetryError::kTelemetryHostAddress;
    }

    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0)  {
       LogCvmfs(kLogTelemetry, kLogDebug | kLogSyslogErr,
                                              "Failed to open socket");
       freeaddrinfo(res);
       return TelemetryError::kTelemetrySocket;
    }

    dest_addr = reinterpret_cast<sockaddr_in*>(res->ai_addr);
    dest_addr->sin_port = htons(port);

    ssize_t num_bytes_sent = sendto(sockfd,
                                  payload.data(),
                                  payload.size(),
                                  0,
                                  reinterpret_cast<struct sockaddr*>(dest_addr),
                                  sizeof(struct sockaddr_in));

    close(sockfd);
    freeaddrinfo(res);

    if (num_bytes_sent < 0)  {
      LogCvmfs(kLogTelemetry, kLogDebug | kLogSyslogErr,
                                "Failed to send to influx. errno=%d", errno);
      return TelemetryError::kTelemetrySend;
    } else if (static_cast<size_t>(num_bytes_sent) != payload.size())  {
      LogCvmfs(kLogTelemetry, kLogDebug | kLogSyslogErr,
                    "Incomplete send. Bytes transferred: %d. Bytes expected %d",
                    num_bytes_sent, payload.size());
      return TelemetryError::kTelemetrySend;
    }

    LogCvmfs(kLogTelemetry, kLogDebug, "INFLUX: POSTING [%s]", payload.c_str());
    return TelemetryError::kTelemetrySuccess;
}


void TelemetryAggregatorInflux::PushMetrics() {
  // create payload
  std::string payload = MakePayload();
  std::string delta_payload = "";
  if (old_counters_.size() > 0) {
    delta_payload = MakeDeltaPayload();
    payload = payload + "\n" + delta_payload;
  }

  // send to influx
  SendToInflux(payload);

  // current counter is now old counter
  counters_.swap(old_counters_);
}

}  // namespace perf
