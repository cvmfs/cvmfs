/**
 * This file is part of the CernVM File System.
 */


#include "fuse_remount.h"

#include <errno.h>
#include <poll.h>
#include <unistd.h>

#include <cassert>
#include <cstdlib>
#include <cstring>

#include "backoff.h"
#include "catalog_mgr_client.h"
#include "fuse_inode_gen.h"
#include "lru_md.h"
#include "mountpoint.h"
#include "options.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/pipe.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT


FuseRemounter::Status FuseRemounter::ChangeRoot(const shash::Any &root_hash) {
  if (mountpoint_->catalog_mgr()->GetRootHash() == root_hash)
    return kStatusUp2Date;

  const FenceGuard fence_guard(&fence_maintenance_);
  if (IsInMaintenanceMode())
    return kStatusMaintenance;

  int32_t expected_val = 0;
  if (drainout_mode_.compare_exchange_strong(expected_val, 1)) {
    // As of this point, fuse callbacks return zero as cache timeout
    LogCvmfs(kLogCvmfs, kLogDebug, "chroot, draining out meta-data caches");
    invalidator_handle_.Reset();
    invalidator_->InvalidateInodes(&invalidator_handle_);
    drainout_mode_.fetch_add(1);
    // drainout_mode_ == 2, IsInDrainoutMode is now 'true'
  } else {
    LogCvmfs(kLogCvmfs, kLogDebug, "already in drainout mode, leaving");
    return kStatusDraining;
  }

  int32_t drainout_code = 0;
  BackoffThrottle throttle;
  do {
    TryFinish(root_hash);
    drainout_code = drainout_mode_.load();
    if (drainout_code == 0)
      break;
    throttle.Throttle();
  } while (true);

  if (mountpoint_->catalog_mgr()->GetRootHash() == root_hash)
    return kStatusUp2Date;
  return kStatusFailGeneral;
}


/**
 * Executed by the trigger thread, or triggered from cvmfs_talk.  Moves into
 * drainout mode if a new catalog is available online.
 */
FuseRemounter::Status FuseRemounter::Check() {
  const FenceGuard fence_guard(&fence_maintenance_);
  if (IsInMaintenanceMode())
    return kStatusMaintenance;

  LogCvmfs(kLogCvmfs, kLogDebug,
           "catalog TTL expired, checking revision against blacklists");
  const BlacklistReloadStatus blres = ReloadBlacklistsBounded();
  if (blres == kBlReloadTimedOut) {
    // The blacklists live on the config repository mount, which can block
    // indefinitely.  Fail like a failed catalog reload instead of hanging
    // the single remount trigger thread forever.
    LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
             "blacklist reload timed out, keeping catalog revision");
    SetOfflineMode(true);
    catalogs_valid_until_ = time(NULL) + MountPoint::kShortTermTTL;
    SetAlarm(MountPoint::kShortTermTTL);
    return kStatusFailGeneral;
  }
  if (blres == kBlReloadFailed) {
    // Old skip-on-failure semantics (mountpoint.cc TODO).
    LogCvmfs(kLogCvmfs, kLogSyslogWarn | kLogDebug,
             "failed to reload blacklists, continuing with the last loaded "
             "blacklist");
  }
  // On kBlReloadFailed the last fully loaded blacklist stays in effect.
  if (blres == kBlReloadOk
      && mountpoint_->catalog_mgr()->IsRevisionBlacklisted()) {
    PANIC(kLogDebug | kLogSyslogErr,
          "repository revision blacklisted, aborting");
  }

  LogCvmfs(kLogCvmfs, kLogDebug, "remounting root catalog");
  const catalog::LoadReturn retval = mountpoint_->catalog_mgr()
                                         ->RemountDryrun();
  switch (retval) {
    case catalog::kLoadNew: {
      SetOfflineMode(false);
      int32_t expected_val = 0;
      if (drainout_mode_.compare_exchange_strong(expected_val, 1)) {
        // As of this point, fuse callbacks return zero as cache timeout
        LogCvmfs(kLogCvmfs, kLogDebug,
                 "new catalog revision available, "
                 "draining out meta-data caches");
        invalidator_handle_.Reset();
        invalidator_->InvalidateInodes(&invalidator_handle_);
        drainout_mode_.fetch_add(1);
        // drainout_mode_ == 2, IsInDrainoutMode is now 'true'
      } else {
        LogCvmfs(kLogCvmfs, kLogDebug, "already in drainout mode, leaving");
      }
      // TryFinish() normally runs from a fuse callback; on an idle mount
      // that may never happen, so re-arm to avoid parking forever.
      SetAlarm(MountPoint::kShortTermTTL);
      return kStatusDraining;
    }
    case catalog::kLoadFail:
    case catalog::kLoadNoSpace:
      LogCvmfs(kLogCvmfs, kLogDebug,
               "reload failed (%s), applying short term TTL",
               catalog::Code2Ascii(retval));
      SetOfflineMode(true);
      catalogs_valid_until_ = time(NULL) + MountPoint::kShortTermTTL;
      SetAlarm(MountPoint::kShortTermTTL);
      return (retval == catalog::kLoadFail) ? kStatusFailGeneral
                                            : kStatusFailNoSpace;
    case catalog::kLoadUp2Date: {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "catalog up to date (could be offline mode)");
      SetOfflineMode(mountpoint_->catalog_mgr()->offline_mode());
      const unsigned ttl = offline_mode_ ? MountPoint::kShortTermTTL
                                         : mountpoint_->GetEffectiveTtlSec();
      catalogs_valid_until_ = time(NULL) + ttl;
      SetAlarm(ttl);
      return kStatusUp2Date;
    }
    default:
      PANIC(NULL);
  }
}


/**
 * Used from the TalkManager.  Continuously calls 'check' until it returns with
 * "up to date" or a failure.
 */
FuseRemounter::Status FuseRemounter::CheckSynchronously() {
  BackoffThrottle throttle;
  while (true) {
    const Status status = Check();
    switch (status) {
      case kStatusDraining:
        TryFinish();
        break;
      default:
        return status;
    }
    throttle.Throttle();
  }
}


void FuseRemounter::EnterMaintenanceMode() {
  fence_maintenance_.Drain();
  int32_t expected_val = 0;
  maintenance_mode_.compare_exchange_strong(expected_val, 1);
  fence_maintenance_.Open();

  // All running Check() and TryFinish() methods returned.  Both methods now
  // return immediately as noops.

  // Flush caches before reload of fuse module
  invalidator_handle_.Reset();
  invalidator_->InvalidateInodes(&invalidator_handle_);
  invalidator_handle_.WaitFor();
}

FuseRemounter::FuseRemounter(MountPoint *mountpoint,
                             cvmfs::InodeGenerationInfo *inode_generation_info,
                             struct fuse_session **fuse_session,
                             bool fuse_notify_invalidation)
    : mountpoint_(mountpoint)
    , inode_generation_info_(inode_generation_info)
    , invalidator_(new FuseInvalidator(mountpoint, fuse_session,
                                       fuse_notify_invalidation))
    , invalidator_handle_(static_cast<int>(mountpoint->kcache_timeout_sec()))
    , fence_(new Fence())
    , offline_mode_(false)
    , catalogs_valid_until_(MountPoint::kIndefiniteDeadline) {
  memset(&thread_remount_trigger_, 0, sizeof(thread_remount_trigger_));
  pipe_remount_trigger_[0] = pipe_remount_trigger_[1] = -1;
  memset(&thread_blacklist_reload_, 0, sizeof(thread_blacklist_reload_));
  pipe_blacklist_reload_ = NULL;
  pipe_blacklist_reload_result_ = NULL;
  pipe_blacklist_reload_terminate_ = NULL;
  blacklist_reloader_started_.store(false);
  blacklist_reload_request_id_ = 0;
  drainout_mode_.store(0);
  maintenance_mode_.store(0);
  critical_section_.store(0);
}

FuseRemounter::~FuseRemounter() {
  if (HasRemountTrigger()) {
    char quit = 'Q';
    WritePipe(pipe_remount_trigger_[1], &quit, 1);
    pthread_join(thread_remount_trigger_, NULL);
    ClosePipe(pipe_remount_trigger_);
  }
  StopBlacklistReloader();
  delete invalidator_;
  delete fence_;
}


/**
 * Triggers the Check() method when the catalog TTL expires.  Works essentially
 * as an alarm() timer.
 */
void *FuseRemounter::MainRemountTrigger(void *data) {
  FuseRemounter *remounter = reinterpret_cast<FuseRemounter *>(data);
  LogCvmfs(kLogCvmfs, kLogDebug, "starting remount trigger");
  char c;
  int timeout_ms = -1;
  uint64_t deadline = 0;
  struct pollfd watch_ctrl;
  watch_ctrl.fd = remounter->pipe_remount_trigger_[0];
  watch_ctrl.events = POLLIN | POLLPRI;
  while (true) {
    watch_ctrl.revents = 0;
    const int retval = poll(&watch_ctrl, 1, timeout_ms);
    if (retval < 0) {
      if (errno == EINTR) {
        if (timeout_ms >= 0) {
          const uint64_t now = platform_monotonic_time();
          timeout_ms = (now > deadline) ? 0 : (deadline - now) * 1000;
        }
        continue;
      }
      PANIC(kLogSyslogErr | kLogDebug,
            "remount trigger connection failure (%d)", errno);
    }

    if (retval == 0) {
      remounter->Check();
      timeout_ms = -1;
      continue;
    }

    assert(watch_ctrl.revents != 0);

    ReadPipe(remounter->pipe_remount_trigger_[0], &c, 1);
    if (c == 'Q')
      break;
    assert(c == 'T');
    ReadPipe(remounter->pipe_remount_trigger_[0], &timeout_ms, sizeof(int));
    deadline = platform_monotonic_time() + timeout_ms / 1000;
  }
  LogCvmfs(kLogCvmfs, kLogDebug, "stopping remount trigger");
  return NULL;
}


void FuseRemounter::SetAlarm(int timeout) {
  // Remounting could be called for a non auto-update repository
  if (!HasRemountTrigger())
    return;

  timeout *= 1000;  // timeout given in ms
  const unsigned buf_size = 1 + sizeof(int);
  char buf[buf_size];
  buf[0] = 'T';
  memcpy(&buf[1], &timeout, sizeof(timeout));
  WritePipe(pipe_remount_trigger_[1], buf, buf_size);
}


void FuseRemounter::SetOfflineMode(bool value) {
  if (value == offline_mode_)
    return;
  offline_mode_ = value;

  if (offline_mode_) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogWarn,
             "warning, could not apply updated catalog revision, "
             "entering offline mode");
    mountpoint_->file_system()->io_error_info()->AddIoError();
  } else {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslog, "recovered from offline mode");
  }
}

namespace {
/**
 * One bounded blacklist reload result, transported as a single record.
 */
struct BlacklistReloadResult {
  int64_t request_id;
  int32_t status;  // FuseRemounter::BlacklistReloadStatus
};
}  // namespace

/**
 * Dedicated helper thread that reloads the blacklists.  They live on the
 * config repository mount, which can block indefinitely;
 * ReloadBlacklistsBounded() bounds the wait for the result.
 */
void *FuseRemounter::MainBlacklistReload(void *data) {
  FuseRemounter *remounter = reinterpret_cast<FuseRemounter *>(data);
  LogCvmfs(kLogCvmfs, kLogDebug, "starting blacklist reload helper");
  struct pollfd watch_fds[2];
  watch_fds[0].fd = remounter->pipe_blacklist_reload_->GetReadFd();
  watch_fds[0].events = POLLIN | POLLPRI;
  watch_fds[1].fd = remounter->pipe_blacklist_reload_terminate_->GetReadFd();
  watch_fds[1].events = POLLIN | POLLPRI;
  for (;;) {
    watch_fds[0].revents = 0;
    watch_fds[1].revents = 0;
    const int retval = poll(watch_fds, 2, -1);
    if (retval < 0) {
      if (errno == EINTR)
        continue;
      PANIC(kLogSyslogErr | kLogDebug,
            "blacklist reload helper failure (%d)", errno);
    }
    if (watch_fds[1].revents != 0)
      break;  // termination request
    int64_t request_id;
    remounter->pipe_blacklist_reload_->Read(&request_id);
    const int status = remounter->mountpoint_->ReloadBlacklists()
                           ? static_cast<int>(kBlReloadOk)
                           : static_cast<int>(kBlReloadFailed);
    const BlacklistReloadResult result = {request_id, status};
    remounter->pipe_blacklist_reload_result_->Write(result);
  }
  LogCvmfs(kLogCvmfs, kLogDebug, "stopping blacklist reload helper");
  return NULL;
}

/**
 * Bounded variant of ReloadBlacklists(): runs the reload on the helper
 * thread and waits at most BlacklistReloadTimeoutSec().  Concurrent Check()
 * callers (trigger, talk, notification client) are serialized by the mutex.
 */
FuseRemounter::BlacklistReloadStatus FuseRemounter::ReloadBlacklistsBounded() {
  if (!blacklist_reloader_started_.load())
    return kBlReloadTimedOut;  // no helper yet: fail closed (unreachable;
                               // Spawn() precedes all Check() callers)
  pthread_mutex_lock(&blacklist_reload_mutex_);
  BlacklistReloadStatus status = kBlReloadFailed;
  bool done = false;
  const int64_t request_id = ++blacklist_reload_request_id_;
  pipe_blacklist_reload_->Write(request_id);
  const unsigned timeout_sec = BlacklistReloadTimeoutSec();
  const uint64_t deadline = platform_monotonic_time() + timeout_sec;
  struct pollfd watch_fd;
  watch_fd.fd = pipe_blacklist_reload_result_->GetReadFd();
  watch_fd.events = POLLIN | POLLPRI;
  while (!done) {
    const uint64_t now = platform_monotonic_time();
    int timeout_ms =
        (now >= deadline) ? 0 : static_cast<int>((deadline - now) * 1000);
    watch_fd.revents = 0;
    const int retval = poll(&watch_fd, 1, timeout_ms);
    if (retval < 0 && errno == EINTR)
      continue;
    if (retval <= 0) {
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "blacklist reload timed out after %u seconds, "
               "keeping the current catalog revision",
               timeout_sec);
      status = kBlReloadTimedOut;
      break;
    }
    BlacklistReloadResult result = {};
    pipe_blacklist_reload_result_->Read(&result);
    if (result.request_id != request_id)
      continue;  // stale result of a previous, timed-out request
    status = static_cast<BlacklistReloadStatus>(result.status);
    done = true;
  }
  pthread_mutex_unlock(&blacklist_reload_mutex_);
  return status;
}

void FuseRemounter::StartBlacklistReloader() {
  assert(!blacklist_reloader_started_.load());
  pipe_blacklist_reload_ = new Pipe<kPipeBlacklistReload>();
  pipe_blacklist_reload_result_ = new Pipe<kPipeBlacklistReloadResult>();
  pipe_blacklist_reload_terminate_ = new Pipe<kPipeThreadTerminator>();
  const int mutex_retval = pthread_mutex_init(&blacklist_reload_mutex_, NULL);
  assert(mutex_retval == 0);
  const int retval = pthread_create(&thread_blacklist_reload_, NULL,
                                    MainBlacklistReload, this);
  assert(retval == 0);
  blacklist_reloader_started_.store(true);
}

void FuseRemounter::StopBlacklistReloader() {
  if (!blacklist_reloader_started_.load())
    return;
  pipe_blacklist_reload_terminate_->Write(kPipeTerminateSignal);
  // The helper can be stuck inside the config repository mount; joining it
  // then blocks the same way joining the remount trigger thread can.
  pthread_join(thread_blacklist_reload_, NULL);
  delete pipe_blacklist_reload_;
  delete pipe_blacklist_reload_result_;
  delete pipe_blacklist_reload_terminate_;
  pthread_mutex_destroy(&blacklist_reload_mutex_);
  blacklist_reloader_started_.store(false);
}

unsigned FuseRemounter::BlacklistReloadTimeoutSec() const {
  string optarg;
  if (mountpoint_->file_system()->options_mgr()->GetValue(
          "CVMFS_BLACKLIST_RELOAD_TIMEOUT_SEC", &optarg)) {
    // String2Uint64() yields 0 on unparsable input; a zero timeout would
    // fail every check.  Clamp huge values: the wait math casts to int ms.
    const uint64_t timeout = String2Uint64(optarg);
    if (timeout > 0)
      return timeout > 3600 ? 3600 : static_cast<unsigned>(timeout);
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogWarn,
             "invalid CVMFS_BLACKLIST_RELOAD_TIMEOUT_SEC=\"%s\", "
             "using default %u seconds",
             optarg.c_str(), kDefaultBlacklistReloadTimeoutSec);
  }
  return kDefaultBlacklistReloadTimeoutSec;
}

std::string FuseRemounter::GetRemountInfo() const {
  std::string result;
  result += "trigger thread: ";
  result += HasRemountTrigger() ? "active" : "inactive";
  result += "\nblacklist reload helper: ";
  result += blacklist_reloader_started_.load() ? "active" : "inactive";
  result += "\ndrainout mode: ";
  result += IsInDrainoutMode() ? "active" : "inactive";
  result += "\nmaintenance mode: ";
  result += IsInMaintenanceMode() ? "active" : "inactive";
  result += "\noffline mode: ";
  result += offline_mode_ ? "active" : "inactive";
  if (catalogs_valid_until_ == MountPoint::kIndefiniteDeadline) {
    result += "\ncatalogs valid until: indefinite (fixed catalog)";
  } else {
    const int64_t expires_in = static_cast<int64_t>(catalogs_valid_until_)
                               - static_cast<int64_t>(time(NULL));
    result += "\ncatalogs expire in: ";
    result += StringifyInt(expires_in);
    result += " seconds";
  }
  result += "\n";
  return result;
}


void FuseRemounter::Spawn() {
  invalidator_->Spawn();
  StartBlacklistReloader();
  if (!mountpoint_->fixed_catalog()) {
    MakePipe(pipe_remount_trigger_);
    const int retval = pthread_create(&thread_remount_trigger_, NULL,
                                      MainRemountTrigger, this);
    assert(retval == 0);

    SetOfflineMode(mountpoint_->catalog_mgr()->offline_mode());
    const unsigned ttl = offline_mode_ ? MountPoint::kShortTermTTL
                                       : mountpoint_->GetEffectiveTtlSec();
    catalogs_valid_until_ = time(NULL) + ttl;
    SetAlarm(ttl);
  }
}


/**
 * Applies a previously started remount operation.  This is called from the
 * fuse callbacks or from CheckSynchronously().  Usually, the method quits
 * immediately except when a new catalog is available and the kernel caches are
 * flushed.
 */
void FuseRemounter::TryFinish(const shash::Any &root_hash) {
  const FenceGuard fence_guard(&fence_maintenance_);
  if (IsInMaintenanceMode())
    return;
  if (!EnterCriticalSection())
    return;
  if (!IsInDrainoutMode()) {
    LeaveCriticalSection();
    return;
  }

  // No one else is in this code path and we have a valid FuseInvalidator handle

  if (!invalidator_handle_.IsDone()) {
    LeaveCriticalSection();
    return;
  }
  LogCvmfs(kLogCvmfs, kLogDebug, "caches drained out, applying new catalog");

  // No new inserts into caches
  mountpoint_->inode_cache()->Pause();
  mountpoint_->path_cache()->Pause();
  mountpoint_->md5path_cache()->Pause();
  mountpoint_->inode_cache()->Drop();
  mountpoint_->path_cache()->Drop();
  mountpoint_->md5path_cache()->Drop();

  // Ensure that all Fuse callbacks left the catalog query code
  fence_->Drain();
  catalog::LoadReturn retval;
  if (root_hash.IsNull()) {
    retval = mountpoint_->catalog_mgr()->Remount();
  } else {
    retval = mountpoint_->catalog_mgr()->ChangeRoot(root_hash);
  }
  if (mountpoint_->inode_annotation()) {
    inode_generation_info_->inode_generation = mountpoint_->inode_annotation()
                                                   ->GetGeneration();
  }
  mountpoint_->ReEvaluateAuthz();
  fence_->Open();

  mountpoint_->inode_cache()->Resume();
  mountpoint_->path_cache()->Resume();
  mountpoint_->md5path_cache()->Resume();

  drainout_mode_.fetch_add(-2);  // 2 --> 0, end of drainout mode

  if ((retval == catalog::kLoadFail) || (retval == catalog::kLoadNoSpace)) {
    // Can temporarily "escape" offline mode if update came from updated
    // alien cache
    SetOfflineMode(true);
    catalogs_valid_until_ = time(NULL) + MountPoint::kShortTermTTL;
    SetAlarm(MountPoint::kShortTermTTL);
  } else {
    SetOfflineMode(false);
    LogCvmfs(kLogCvmfs, kLogSyslog, "switched to catalog revision %" PRIu64,
             mountpoint_->catalog_mgr()->GetRevision());
    catalogs_valid_until_ = time(NULL) + mountpoint_->GetEffectiveTtlSec();
    SetAlarm(mountpoint_->GetEffectiveTtlSec());
  }

  LeaveCriticalSection();
}
