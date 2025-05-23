/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FUSE_REMOUNT_H_
#define CVMFS_FUSE_REMOUNT_H_

#include <pthread.h>

#include <ctime>

#include "crypto/hash.h"
#include "duplex_fuse.h"
#include "fence.h"
#include "fuse_evict.h"
#include "util/atomic.h"
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

  FuseRemounter(MountPoint *mountpoint,
                cvmfs::InodeGenerationInfo *inode_generation_info,
                void **fuse_channel_or_session,
                bool fuse_notify_invalidation);
  ~FuseRemounter();
  void Spawn();

  Status Check();
  Status CheckSynchronously();
  Status ChangeRoot(const shash::Any &root_hash);
  void TryFinish(const shash::Any &root_hash = shash::Any());
  void EnterMaintenanceMode();
  bool IsCaching() {
    return (atomic_read32(&maintenance_mode_) == 0)
           && (atomic_read32(&drainout_mode_) == 0);
  }
  bool IsInDrainoutMode() { return atomic_read32(&drainout_mode_) == 2; }
  bool IsInMaintenanceMode() { return atomic_read32(&maintenance_mode_) == 1; }

  Fence *fence() { return fence_; }
  time_t catalogs_valid_until() { return catalogs_valid_until_; }

  void InvalidateDentry(uint64_t parent_ino, const NameString &name) {
    invalidator_->InvalidateDentry(parent_ino, name);
  }

 private:
  static void *MainRemountTrigger(void *data);

  bool HasRemountTrigger() { return pipe_remount_trigger_[0] >= 0; }
  void SetAlarm(int timeout);

  bool EnterCriticalSection() { return atomic_cas32(&critical_section_, 0, 1); }
  void LeaveCriticalSection() { atomic_dec32(&critical_section_); /* 1 -> 0 */ }

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
  atomic_int32 drainout_mode_;
  /**
   * in maintenance mode, cache timeout is 0 and catalogs are not reloaded.
   * Maintenance mode is entered when the fuse module gets reloaded.
   */
  atomic_int32 maintenance_mode_;
  /**
   * Only one thread must perform the actual remount (stopping user-level
   * caches, loading new catalog, etc.).  This is used to protect TyrFinish()
   * from concurrent execution.
   */
  atomic_int32 critical_section_;
};  // class FuseRemounter

#endif  // CVMFS_FUSE_REMOUNT_H_
