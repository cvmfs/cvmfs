/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FUSE_REMOUNT_H_
#define CVMFS_FUSE_REMOUNT_H_

#include <pthread.h>

#include <atomic>
#include <cstdint>
#include <ctime>
#include <string>

#include "crypto/hash.h"
#include "duplex_fuse.h"
#include "fence.h"
#include "fuse_evict.h"
#include "util/pipe.h"
#include "util/single_copy.h"

namespace cvmfs {
struct InodeGenerationInfo;
}
class MountPoint;

/**
 * Orchestrates an orderly remount of a new snapshot revision in the Fuse
 * module.  Remounting always happens as a result of calling Check() followed
 * by a call to TryFinish().  The Check() method is either called from the
 * remount trigger or from the TalkManager.  The TryFinish() method is either
 * called from a fuse callback function or from CheckSynchronously().  If a new
 * root file catalog is available online and the kernel caches got flushed, the
 * actual heavy-lifting of applying the new catalog takes place in TryFinish();
 *
 * Remounting is inherently asynchronous because the kernel caches need to be
 * flushed.  We do this through the FuseInvalidator.  Once the FuseInvalidor
 * is ready (either by waiting or by active eviction), we flush all user-level
 * caches and reload a new root catalog.
 */
class FuseRemounter : SingleCopy {
 public:
  enum Status {
    kStatusUp2Date = 0,
    kStatusDraining,
    kStatusMaintenance,
    kStatusFailGeneral,
    kStatusFailNoSpace,
  };

  /**
   * Result of a bounded blacklist reload; travels through a pipe as int32_t.
   */
  enum BlacklistReloadStatus : int32_t {
    kBlReloadOk = 0,    ///< blacklist (re)loaded successfully
    kBlReloadTimedOut,  ///< reload did not finish within the timeout
    kBlReloadFailed,    ///< reload ran but failed (see syslog)
  };

  FuseRemounter(MountPoint *mountpoint,
                cvmfs::InodeGenerationInfo *inode_generation_info,
                struct fuse_session **fuse_session,
                bool fuse_notify_invalidation);
  ~FuseRemounter();
  void Spawn();

  Status Check();
  Status CheckSynchronously();
  Status ChangeRoot(const shash::Any &root_hash);
  void TryFinish(const shash::Any &root_hash = shash::Any());
  void EnterMaintenanceMode();
  bool IsCaching() {
    return (maintenance_mode_.load() == 0) && (drainout_mode_.load() == 0);
  }
  bool IsInDrainoutMode() const { return drainout_mode_.load() == 2; }
  bool IsInMaintenanceMode() const { return maintenance_mode_.load() == 1; }

  /// State of the remount/update loop, for the "remount info" command.
  std::string GetRemountInfo() const;

  Fence *fence() { return fence_; }
  time_t catalogs_valid_until() { return catalogs_valid_until_; }

  void InvalidateDentry(uint64_t parent_ino, const NameString &name) {
    invalidator_->InvalidateDentry(parent_ino, name);
  }

 private:
  static void *MainRemountTrigger(void *data);

  bool HasRemountTrigger() const { return pipe_remount_trigger_[0] >= 0; }
  void SetAlarm(int timeout);
  static const unsigned kDefaultBlacklistReloadTimeoutSec = 10;
  /// Bounded ReloadBlacklists(): runs on the helper thread, waits at most
  /// BlacklistReloadTimeoutSec(); on timeout Check() keeps the revision.
  BlacklistReloadStatus ReloadBlacklistsBounded();
  unsigned BlacklistReloadTimeoutSec() const;
  static void *MainBlacklistReload(void *data);
  void StartBlacklistReloader();
  void StopBlacklistReloader();

  bool EnterCriticalSection() {
    int32_t expected_val = 0;
    return critical_section_.compare_exchange_strong(expected_val, 1);
  }
  void LeaveCriticalSection() { critical_section_.fetch_sub(1); /* 1 -> 0 */ }

  void SetOfflineMode(bool value);

  MountPoint *mountpoint_;                             ///< Not owned
  cvmfs::InodeGenerationInfo *inode_generation_info_;  ///< Not owned
  FuseInvalidator *invalidator_;
  /**
   * Used to query whether the kernel cache invalidation is done.
   */
  FuseInvalidator::Handle invalidator_handle_;
  /**
   * Ensures that within a fuse callback all operations take place on the same
   * catalog revision.
   */
  Fence *fence_;
  /**
   * This fence makes sure that Check() and TryFinish() have been left after
   * the maintenance mode flag was set.
   */
  Fence fence_maintenance_;
  pthread_t thread_remount_trigger_;
  /**
   * The thread that triggers the reload of the root catalog is controlled
   * through this pipe.
   */
  int pipe_remount_trigger_[2];
  /**
   * Helper thread that reloads the blacklists (they live on the config
   * repository mount, which can block indefinitely).
   */
  pthread_t thread_blacklist_reload_;
  std::atomic<bool> blacklist_reloader_started_;
  /**
   * Parent -> helper: request sequence number.
   * Helper -> parent: request sequence number and result.
   */
  Pipe<kPipeBlacklistReload> *pipe_blacklist_reload_;
  Pipe<kPipeBlacklistReloadResult> *pipe_blacklist_reload_result_;
  Pipe<kPipeThreadTerminator> *pipe_blacklist_reload_terminate_;
  /** Serializes concurrent Check() callers around one bounded reload. */
  pthread_mutex_t blacklist_reload_mutex_;
  int64_t blacklist_reload_request_id_;
  /**
   * Indicates whether the last reload attempt failed. If so, the short term
   * TTL is active.
   */
  bool offline_mode_;
  /**
   * Stores the deadline after which the remount trigger will look again for
   * an updated version.  Can be MountPoint::kIndefiniteDeadline if a fixed root
   * catalog is used.  Only for information purposes ('expires' xattr).
   * TODO(jblomer): access to this field should be locked
   */
  time_t catalogs_valid_until_;
  /**
   * In drainout mode, the fuse module sets the timeout of meta data replies to
   * zero.  If supported by Fuse, the FuseInvalidator will evict all active
   * entries from the kernel cache.  Drainout mode is left only once the new
   * root catalog is active (after TryFinish()).
   *
   * Moving into drainout mode is a two-steps procedure.  Going from zero to one
   * the handle of the FuseInvalidator is prepared, from one to two is the
   * actual move into drainout mode.
   */
  std::atomic<int32_t> drainout_mode_;
  /**
   * in maintenance mode, cache timeout is 0 and catalogs are not reloaded.
   * Maintenance mode is entered when the fuse module gets reloaded.
   */
  std::atomic<int32_t> maintenance_mode_;
  /**
   * Only one thread must perform the actual remount (stopping user-level
   * caches, loading new catalog, etc.).  This is used to protect TyrFinish()
   * from concurrent execution.
   */
  std::atomic<int32_t> critical_section_;
};  // class FuseRemounter

#endif  // CVMFS_FUSE_REMOUNT_H_

