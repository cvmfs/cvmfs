/**
 * This file is part of the CernVM File System.
 */

#include <errno.h>
#include <poll.h>
#include <unistd.h>

#include "telemetry_aggregator.h"

#include "util/logging.h"
#include "util/platform.h"
#include "util/pointer.h"
#include "util/posix.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace perf {

TelemetryAggregator *TelemetryAggregator::Create(Statistics* statistics,
                                            uint64_t send_rate,
                                            OptionsManager *options_mgr,
                                            std::string fqrn) {
  UniquePtr<TelemetryAggregator>
    telemetry(new TelemetryAggregator(statistics, send_rate,
                                      options_mgr, fqrn));

  LogCvmfs(kLogTalk, kLogDebug, "TELEMETRY: TelemetryAggregator created");

  return telemetry.Release();
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
  if (allow_spawning_ == true) {
    assert(pipe_terminate_[0] == -1);
    assert(maximum_send_rate_ > 0);
    MakePipe(pipe_terminate_);
    int retval = pthread_create(&thread_telemetry_, NULL, MainTelemetry, this);
    assert(retval == 0);
    LogCvmfs(kLogCvmfs, kLogDebug,
              "TELEMETRY: Spawning of telemetry thread.");
  } else {
    LogCvmfs(kLogCvmfs, kLogDebug,
              "TELEMETRY: Spawning of telemetry thread not allowed. "
              "All parameters set?");
  }
}

void *TelemetryAggregator::MainTelemetry(void *data) {
  TelemetryAggregator *telemetry = reinterpret_cast<TelemetryAggregator*>(data);
  Statistics *statistics = telemetry->statistics_;
  OptionsManager *option_mgr = telemetry->options_mgr_;

  struct pollfd watch_term;
  watch_term.fd = telemetry->pipe_terminate_[0];
  watch_term.events = POLLIN | POLLPRI;
  int timeout_ms = telemetry->maximum_send_rate_ * 1000;
  uint64_t deadline = platform_monotonic_time() + telemetry->maximum_send_rate_;
  while (true) {
    // sleep and check if end - blocking wait for "maximum_send_rate_" seconds
    watch_term.revents = 0;
    int retval = poll(&watch_term, 1, timeout_ms);
    if (retval < 0) {
      if (errno == EINTR) {  // external interrupt occured
        if (timeout_ms >= 0) {
          uint64_t now = platform_monotonic_time();
          timeout_ms = (now > deadline) ? 0 : (deadline - now) * 1000;
        }
        continue;
      }
      abort();
    }

    // aggregate + send stuff
    if (retval == 0) {
      statistics->SnapshotCounters(&telemetry->counters_,
                                   &telemetry->monotonic_clock_);
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
  LogCvmfs(kLogCvmfs, kLogDebug, "TELEMETRY: "
                                 "Stopping TelemetryAggregator thread");
  return NULL;
}


}  // namespace perf

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
