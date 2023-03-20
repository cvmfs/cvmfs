/**
 * This file is part of the CernVM File System.
 */

#include "telemetry_aggregator.h"

#include <errno.h>
#include <poll.h>
#include <unistd.h>

#include "util/exception.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/pointer.h"
#include "util/posix.h"

#include "telemetry_aggregator_influx.h"
namespace perf {

TelemetryAggregator* TelemetryAggregator::Create(Statistics* statistics,
                                                 int send_rate,
                                                 OptionsManager *options_mgr,
                                                 const std::string &fqrn,
                                                 const TelemetrySelector type) {
  UniquePtr<TelemetryAggregatorInflux> telemetryInflux;
  UniquePtr<TelemetryAggregator> *telemetry;

  switch (type) {
    case kTelemetryInflux:
      telemetryInflux = new TelemetryAggregatorInflux(statistics, send_rate,
                                  options_mgr, fqrn);
      telemetry = reinterpret_cast<UniquePtr<TelemetryAggregator>*>
                                                            (&telemetryInflux);
    break;
    default:
      LogCvmfs(kLogTelemetry, kLogDebug,
                      "No implementation available for given telemetry class.");
      return NULL;
    break;
  }

  if (telemetry->weak_ref()->is_zombie_) {
    LogCvmfs(kLogTelemetry, kLogDebug | kLogSyslogErr,
      "Requested telemetry will NOT be used. "
      "It was not constructed correctly.");
    return NULL;
  }

  LogCvmfs(kLogTelemetry, kLogDebug, "TelemetryAggregator created.");
  return telemetry->Release();
}

TelemetryAggregator::~TelemetryAggregator() {
  if (pipe_terminate_[1] >= 0) {
    char t = 'T';
    WritePipe(pipe_terminate_[1], &t, 1);
    pthread_join(thread_telemetry_, NULL);
    ClosePipe(pipe_terminate_);
  }
}

void TelemetryAggregator::Spawn() {
  assert(pipe_terminate_[0] == -1);
  assert(send_rate_sec_ > 0);
  MakePipe(pipe_terminate_);
  int retval = pthread_create(&thread_telemetry_, NULL, MainTelemetry, this);
  assert(retval == 0);
  LogCvmfs(kLogTelemetry, kLogDebug, "Spawning of telemetry thread.");
}

void *TelemetryAggregator::MainTelemetry(void *data) {
  TelemetryAggregator *telemetry = reinterpret_cast<TelemetryAggregator*>(data);
  Statistics *statistics = telemetry->statistics_;

  struct pollfd watch_term;
  watch_term.fd = telemetry->pipe_terminate_[0];
  watch_term.events = POLLIN | POLLPRI;
  int timeout_ms = telemetry->send_rate_sec_ * 1000;
  uint64_t deadline_sec = platform_monotonic_time()
                          + telemetry->send_rate_sec_;
  while (true) {
    // sleep and check if end - blocking wait for "send_rate_sec_" seconds
    watch_term.revents = 0;
    int retval = poll(&watch_term, 1, timeout_ms);
    if (retval < 0) {
      if (errno == EINTR) {  // external interrupt occurred - no error for us
        if (timeout_ms >= 0) {
          uint64_t now = platform_monotonic_time();
          timeout_ms = (now > deadline_sec) ? 0 :
                                  static_cast<int>((deadline_sec - now) * 1000);
        }
        continue;
      }
      PANIC(kLogSyslogErr | kLogDebug, "Error in telemetry thread. "
                                       "Poll returned %d", retval);
    }

    // reset timeout and deadline of poll
    timeout_ms = telemetry->send_rate_sec_ * 1000;
    deadline_sec = platform_monotonic_time() + telemetry->send_rate_sec_;

    // aggregate + send stuff
    if (retval == 0) {
      statistics->SnapshotCounters(&telemetry->counters_,
                                   &telemetry->timestamp_);
      telemetry->PushMetrics();
      continue;
    }

    // stop thread due to poll event
    assert(watch_term.revents != 0);

    char c = 0;
    ReadPipe(telemetry->pipe_terminate_[0], &c, 1);
    assert(c == 'T');
    break;
  }
  LogCvmfs(kLogTelemetry, kLogDebug, "Stopping telemetry thread");
  return NULL;
}

}  // namespace perf
