/**
 * This file is part of the CernVM File System.
 */

#include "telemetry_aggregator.h"

#include <errno.h>
#include <poll.h>
#include <unistd.h>

#include "glue_buffer.h"
#include "shortstring.h"
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
                                                 MountPoint* mount_point,
                                                 const std::string &fqrn,
                                                 const TelemetrySelector type) {
  UniquePtr<TelemetryAggregatorInflux> telemetryInflux;
  UniquePtr<TelemetryAggregator> *telemetry;

  switch (type) {
    case kTelemetryInflux:
      telemetryInflux = new TelemetryAggregatorInflux(statistics, send_rate,
                                                options_mgr, mount_point, fqrn);
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

void TelemetryAggregator::ManuallyUpdateSelectedCounters() {
  if (!mount_point_) {
    return;
  }

  // Manually setting the inode tracker numbers
  glue::InodeTracker::Statistics inode_stats =
                            mount_point_->inode_tracker()->GetStatistics();
  glue::DentryTracker::Statistics dentry_stats =
                            mount_point_->dentry_tracker()->GetStatistics();
  glue::PageCacheTracker::Statistics page_cache_stats =
                            mount_point_->page_cache_tracker()->GetStatistics();
  mount_point_->statistics()->Lookup("inode_tracker.n_insert")->
                               Set(atomic_read64(&inode_stats.num_inserts));
  mount_point_->statistics()->Lookup("inode_tracker.n_remove")->
                               Set(atomic_read64(&inode_stats.num_removes));
  mount_point_->statistics()->Lookup("inode_tracker.no_reference")->
                               Set(atomic_read64(&inode_stats.num_references));
  mount_point_->statistics()->Lookup("inode_tracker.n_hit_inode")->
                               Set(atomic_read64(&inode_stats.num_hits_inode));
  mount_point_->statistics()->Lookup("inode_tracker.n_hit_path")->
                               Set(atomic_read64(&inode_stats.num_hits_path));
  mount_point_->statistics()->Lookup("inode_tracker.n_miss_path")->
                               Set(atomic_read64(&inode_stats.num_misses_path));
  mount_point_->statistics()->Lookup("dentry_tracker.n_insert")->
                               Set(dentry_stats.num_insert);
  mount_point_->statistics()->Lookup("dentry_tracker.n_remove")->
                               Set(dentry_stats.num_remove);
  mount_point_->statistics()->Lookup("dentry_tracker.n_prune")->
                               Set(dentry_stats.num_prune);
  mount_point_->statistics()->Lookup("page_cache_tracker.n_insert")->
                               Set(page_cache_stats.n_insert);
  mount_point_->statistics()->Lookup("page_cache_tracker.n_remove")->
                               Set(page_cache_stats.n_remove);
  mount_point_->statistics()->Lookup("page_cache_tracker.n_open_direct")->
                               Set(page_cache_stats.n_open_direct);
  mount_point_->statistics()->Lookup("page_cache_tracker.n_open_flush")->
                               Set(page_cache_stats.n_open_flush);
  mount_point_->statistics()->Lookup("page_cache_tracker.n_open_cached")->
                               Set(page_cache_stats.n_open_cached);
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
      telemetry->ManuallyUpdateSelectedCounters();
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
