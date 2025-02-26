/**
 * This file is part of the CernVM File System.
 */

#include "telemetry_aggregator_influx.h"

#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

namespace perf {

TelemetryAggregatorInflux::TelemetryAggregatorInflux(Statistics* statistics,
                                                    int send_rate_sec,
                                                    OptionsManager *options_mgr,
                                                    MountPoint* mount_point,
                                                    const std::string &fqrn) :
                      TelemetryAggregator(statistics, send_rate_sec,
                                          mount_point, fqrn),
                      influx_extra_fields_(""), influx_extra_tags_(""),
                      socket_fd_(-1), res_(NULL) {
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
      influx_port_ = static_cast<int>(String2Int64(opt.c_str()));
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
    TelemetryReturn ret = OpenSocket();
    if (ret != kTelemetrySuccess) {
      is_zombie_ = true;
      LogCvmfs(kLogTelemetry, kLogDebug | kLogSyslogWarn,
               "Not enabling influx metrics. Error while open socket. %d", ret);
    }
  } else {
    is_zombie_ = true;
    LogCvmfs(kLogTelemetry, kLogDebug | kLogSyslogWarn,
             "Not enabling influx metrics. Not all mandatory variables set: "
             "CVMFS_INFLUX_METRIC_NAME, CVMFS_INFLUX_HOST, CVMFS_INFLUX_PORT");
  }
}

TelemetryAggregatorInflux::~TelemetryAggregatorInflux() {
  if (socket_fd_ >= 0) {
      close(socket_fd_);
  }
  if (res_) {
    freeaddrinfo(res_);
  }
}

/**
 * Creates a string in the influx data format containing the absolute values
 * of the counters. Counters are only included if their absolute value is > 0.
 *
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
  std::string ret = influx_metric_name_ + "_absolute,repo=" + fqrn_;

  if (influx_extra_tags_ != "") {
    ret += "," + influx_extra_tags_;
  }

  // fields
  ret += " ";
  bool add_token = false;
  for (std::map<std::string, int64_t>::iterator it
      = counters_.begin(), iEnd = counters_.end(); it != iEnd; it++) {
    if (it->second != 0) {
      if (add_token) {
        ret += ",";
      }
      ret += it->first + "=" + StringifyInt(it->second);
      add_token = true;
    }
  }
  if (influx_extra_fields_ != "") {
    if (add_token) {
      ret += ",";
    }
    ret += influx_extra_fields_;
    add_token = true;
  }

  // timestamp
  if (add_token) {
    ret += " ";
  }
  ret += StringifyUint(timestamp_);

  return ret;
}

/**
 * Creates a string in the influx data format containing the delta between
 * 2 measurements of the same counter. Counters are only included if their
 * absolute value is > 0 (delta can be 0).
 *
 * NOTE: As influx_extra_fields_ are static, they are excluded of this
 *       delta format
 *
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
  bool add_token = false;
  for (std::map<std::string, int64_t>::iterator it
      = counters_.begin(), iEnd = counters_.end(); it != iEnd; it++) {
    int64_t value = it->second;
    if (value != 0) {
      int64_t old_value;
      try {
        old_value = old_counters_.at(it->first);
      } catch(const std::out_of_range& ex) {
        old_value = 0;
      }
      if (add_token) {
        ret += ",";
      }
      ret += it->first + "=" + StringifyInt(value - old_value);
      add_token = true;
    }
  }

  // timestamp
  if (add_token) {
    ret += " ";
  }
  ret += StringifyUint(timestamp_);

  return ret;
}

TelemetryReturn TelemetryAggregatorInflux::OpenSocket() {
    const char *hostname = influx_host_.c_str();
    struct addrinfo hints;
    int err;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    err = getaddrinfo(influx_host_.c_str(), NULL, &hints, &res_);
    if (err != 0 || res_ == NULL) {
       LogCvmfs(kLogTelemetry, kLogDebug | kLogSyslogErr,
                "Failed to resolve influx server [%s]. errno=%d",
                hostname, errno);
       return kTelemetryFailHostAddress;
    }

    socket_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_fd_ < 0)  {
       LogCvmfs(kLogTelemetry, kLogDebug | kLogSyslogErr,
                                              "Failed to open socket");
       freeaddrinfo(res_);
       return kTelemetryFailSocket;
    }

    return kTelemetrySuccess;
}

TelemetryReturn TelemetryAggregatorInflux::SendToInflux(
                                            const std::string &payload) {
    struct sockaddr_in *dest_addr = NULL;
    dest_addr = reinterpret_cast<sockaddr_in*>(res_->ai_addr);
    dest_addr->sin_port = htons(influx_port_);

    ssize_t num_bytes_sent = sendto(socket_fd_,
                                  payload.data(),
                                  payload.size(),
                                  0,
                                  reinterpret_cast<struct sockaddr*>(dest_addr),
                                  sizeof(struct sockaddr_in));

    if (num_bytes_sent < 0)  {
      LogCvmfs(kLogTelemetry, kLogDebug | kLogSyslogErr,
                                "Failed to send to influx. errno=%d", errno);
      return kTelemetryFailSend;
    } else if (static_cast<size_t>(num_bytes_sent) != payload.size())  {
      LogCvmfs(kLogTelemetry, kLogDebug | kLogSyslogErr,
                  "Incomplete send. Bytes transferred: %zd. Bytes expected %lu",
                  num_bytes_sent, payload.size());
      return kTelemetryFailSend;
    }

    LogCvmfs(kLogTelemetry, kLogDebug, "INFLUX: POSTING [%s]", payload.c_str());
    return kTelemetrySuccess;
}


void TelemetryAggregatorInflux::PushMetrics() {
  // create payload
  std::string payload = MakePayload();
  std::string delta_payload = "";
  if (old_counters_.size() > 0) {
    delta_payload = MakeDeltaPayload();
    payload = payload + "\n" + delta_payload;
  }
  payload += "\n";

  // send to influx
  SendToInflux(payload);

  // current counter is now old counter
  counters_.swap(old_counters_);
}

}  // namespace perf
