/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "fuse_remount.h"

#include <errno.h>
#include <poll.h>

#include <cassert>
#include <cstdlib>
#include <cstring>

#include "backoff.h"
#include "catalog_mgr_client.h"
#include "fuse_inode_gen.h"
#include "logging.h"
#include "lru_md.h"
#include "mountpoint.h"
#include "platform.h"
#include "util/posix.h"

using namespace std;  // NOLINT


/**
 * Executed by the trigger thread, or triggered from cvmfs_talk.  Moves into
 * drainout mode if a new catalog is available online.
 */
FuseRemounter::Status FuseRemounter::Check() {
  FenceGuard fence_guard(&fence_maintenance_);
  if (IsInMaintenanceMode())
    return kStatusMaintenance;

  LogCvmfs(kLogCvmfs, kLogDebug, "catalog TTL expired, remount");
  catalog::LoadError retval = mountpoint_->catalog_mgr()->Remount(true);

  LogCvmfs(kLogCvmfs, kLogDebug, "checking revision against blacklists");
  if (mountpoint_->CheckBlacklists() &&
      mountpoint_->catalog_mgr()->IsRevisionBlacklisted()) {
    LogCvmfs(kLogCatalog, kLogDebug | kLogSyslogErr,
            "repository revision blacklisted, exiting");
    exit(1);
  }

  switch (retval) {
    case catalog::kLoadNew:
      if (atomic_cas32(&drainout_mode_, 0, 1)) {
        // As of this point, fuse callbacks return zero as cache timeout
        LogCvmfs(kLogCvmfs, kLogDebug,
                 "new catalog revision available, "
                 "draining out meta-data caches");
        invalidator_handle_.Reset();
        invalidator_->InvalidateDentries(&invalidator_handle_);
        atomic_inc32(&drainout_mode_);
        // drainout_mode_ == 2, IsInDrainoutMode is now 'true'
      } else {
        LogCvmfs(kLogCvmfs, kLogDebug, "already in drainout mode, leaving");
      }
      return kStatusDraining;
    case catalog::kLoadFail:
    case catalog::kLoadNoSpace:
      LogCvmfs(kLogCvmfs, kLogDebug,
               "reload failed (%s), applying short term TTL",
               catalog::Code2Ascii(retval));
      catalogs_valid_until_ = time(NULL) + MountPoint::kShortTermTTL;
      SetAlarm(MountPoint::kShortTermTTL);
      return (retval == catalog::kLoadFail) ?
             kStatusFailGeneral : kStatusFailNoSpace;
    case catalog::kLoadUp2Date:
      LogCvmfs(kLogCvmfs, kLogDebug,
               "catalog up to date, applying effective TTL");
      catalogs_valid_until_ = time(NULL) + mountpoint_->GetEffectiveTtlSec();
      SetAlarm(mountpoint_->GetEffectiveTtlSec());
      return kStatusUp2Date;
    default:
      abort();
  }
}


/**
 * Used from the TalkManager.  Continously calls 'check' until it returns with
 * "up to date" or a failure.
 */
FuseRemounter::Status FuseRemounter::CheckSynchronously() {
  BackoffThrottle throttle;
  while (true) {
    Status status = Check();
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
  atomic_cas32(&maintenance_mode_, 0, 1);
  fence_maintenance_.Open();

  // All running Check() and TryFinish() methods returned.  Both methods now
  // return immediately as noops.

  // Flush caches before reload of fuse module
  invalidator_handle_.Reset();
  invalidator_->InvalidateDentries(&invalidator_handle_);
  invalidator_handle_.WaitFor();
}


FuseRemounter::FuseRemounter(
  MountPoint *mountpoint,
  cvmfs::InodeGenerationInfo *inode_generation_info,
  struct fuse_chan **fuse_channel)
  : mountpoint_(mountpoint)
  , inode_generation_info_(inode_generation_info)
  , invalidator_(new FuseInvalidator(mountpoint->inode_tracker(), fuse_channel))
  , invalidator_handle_(mountpoint->kcache_timeout_sec())
  , fence_(new Fence())
  , catalogs_valid_until_(MountPoint::kIndefiniteDeadline)
{
  pipe_remount_trigger_[0] = pipe_remount_trigger_[1] = -1;
  atomic_init32(&drainout_mode_);
  atomic_init32(&maintenance_mode_);
  atomic_init32(&critical_section_);
}


FuseRemounter::~FuseRemounter() {
  if (HasRemountTrigger()) {
    char quit = 'Q';
    WritePipe(pipe_remount_trigger_[1], &quit, 1);
    pthread_join(thread_remount_trigger_, NULL);
    ClosePipe(pipe_remount_trigger_);
  }
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
    int retval = poll(&watch_ctrl, 1, timeout_ms);
    if (retval < 0) {
      if (errno == EINTR) {
        if (timeout_ms >= 0) {
          uint64_t now = platform_monotonic_time();
          timeout_ms = (now > deadline) ? 0 : (deadline - now) * 1000;
        }
        continue;
      }
      LogCvmfs(kLogCvmfs, kLogSyslogErr | kLogDebug,
               "remount trigger connection failure (%d)", errno);
      abort();
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
  assert(HasRemountTrigger());
  timeout *= 1000;  // timeout given in ms
  const unsigned buf_size = 1 + sizeof(int);
  char buf[buf_size];
  buf[0] = 'T';
  memcpy(&buf[1], &timeout, sizeof(timeout));
  WritePipe(pipe_remount_trigger_[1], buf, buf_size);
}


void FuseRemounter::Spawn() {
  invalidator_->Spawn();
  if (!mountpoint_->fixed_catalog()) {
    MakePipe(pipe_remount_trigger_);
    int retval = pthread_create(
      &thread_remount_trigger_, NULL, MainRemountTrigger, this);
    assert(retval == 0);

    unsigned ttl = mountpoint_->catalog_mgr()->offline_mode() ?
      MountPoint::kShortTermTTL : mountpoint_->GetEffectiveTtlSec();
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
void FuseRemounter::TryFinish() {
  FenceGuard fence_guard(&fence_maintenance_);
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
  catalog::LoadError retval = mountpoint_->catalog_mgr()->Remount(false);
  if (mountpoint_->inode_annotation()) {
    inode_generation_info_->inode_generation =
      mountpoint_->inode_annotation()->GetGeneration();
  }
  mountpoint_->ReEvaluateAuthz();
  fence_->Open();

  mountpoint_->inode_cache()->Resume();
  mountpoint_->path_cache()->Resume();
  mountpoint_->md5path_cache()->Resume();

  atomic_xadd32(&drainout_mode_, -2);  // 2 --> 0, end of drainout mode

  if ((retval == catalog::kLoadFail) || (retval == catalog::kLoadNoSpace) ||
      mountpoint_->catalog_mgr()->offline_mode())
  {
    LogCvmfs(kLogCvmfs, kLogDebug, "reload/finish failed, use short term TTL");
    catalogs_valid_until_ = time(NULL) + MountPoint::kShortTermTTL;
    SetAlarm(MountPoint::kShortTermTTL);
  } else {
    LogCvmfs(kLogCvmfs, kLogSyslog, "switched to catalog revision %d",
             mountpoint_->catalog_mgr()->GetRevision());
    catalogs_valid_until_ = time(NULL) + mountpoint_->GetEffectiveTtlSec();
    SetAlarm(mountpoint_->GetEffectiveTtlSec());
  }

  LeaveCriticalSection();
}
