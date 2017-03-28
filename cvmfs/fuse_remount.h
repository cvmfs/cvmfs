/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FUSE_REMOUNT_H_
#define CVMFS_FUSE_REMOUNT_H_

#include <pthread.h>

#include <ctime>

#include "atomic.h"
#include "duplex_fuse.h"
#include "fence.h"
#include "fuse_evict.h"
#include "util/single_copy.h"

namespace cvmfs {
class InodeGenerationInfo;
}
class MountPoint;

/**
 * Orchestrates an orderly remount of a new snapshot revision in the Fuse
 * module.  Remounting always happens as a result of calling Check().  The
 * Check() method is either called from a fuse callback function or from
 * the remount trigger thread as a result of an alarm signal (to remount even
 * when cvmfs is idle).
 *
 * Remounting is inherently asynchronous because the kernel caches need to be
 * flushed.  We do this through the FuseInvalidator.  Once the FuseInvalidor
 * is ready (either by waiting or by active eviction), we flush all user-level
 * caches and reload a new root catalog.
 */
class FuseRemounter : SingleCopy {
 public:
  explicit FuseRemounter(MountPoint *mountpoint,
                         cvmfs::InodeGenerationInfo *inode_generation_info,
                         struct fuse_chan **fuse_channel);
  ~FuseRemounter();
  void Spawn();

  void Check();

  bool IsInDrainoutMode() { return atomic_read32(&drainout_mode_) == 2; }
  bool IsInMaintenanceMode() { return atomic_read32(&maintenance_mode_) == 1; }
  bool IsCatalogsExpired() { return atomic_read32(&catalogs_expired_) == 1; }

  Fence *fence() { return fence_; }
  time_t catalog_valid_until() { return catalogs_valid_until_; }

 private:
  static void *MainRemountTrigger(void *data);

  void Finish();

  bool HasRemountTrigger() { return pipe_remount_trigger_[0] >= 0; }
  void SetAlarm(int timeout);
  void SetCatalogsExpired() { atomic_cas32(&catalogs_expired_, 0, 1); }
  bool EnterCriticalSection()  {
    return atomic_cas32(&critical_section_, 0, 1);
  }
  void LeaveCriticalSection() { atomic_dec32(&critical_section_); /* 1 -> 0 */ }
  void SetMaintenanceMode();

  bool spawned_;
  MountPoint *mountpoint_;  ///< Not owned
  cvmfs::InodeGenerationInfo *inode_generation_info_;  ///< Not owned
  FuseInvalidator *invalidator_;
  /**
   * Used to query when the kernel cache invalidation is done.
   */
  FuseInvalidator::Handle invalidator_handle_;
  /**
   * Ensures that within a fuse callback all operations take place on the same
   * catalog revision.
   */
  Fence *fence_;
  pthread_t thread_remount_trigger_;
  /**
   * The thread that triggers the reload of the root catalog is informed through
   * this pipe by the alarm signal handler when the TTL expires.
   */
  int pipe_remount_trigger_[2];
  /**
   * Stores when the alarm signal fires next time.  Can be
   * MountPoint::kIndefiniteDeadline if a fixed root catalog is used
   * TODO(jblomer): access to this field should be locked
   */
  time_t catalogs_valid_until_;
  /**
   * In drainout mode, the fuse module sets the timeout of meta data replys to
   * zero.  If supported by Fuse, the FuseInvalidator will evict all active
   * entries from the kernel cache.  Drainout mode is left only once the new
   * root catalog is active (after Finish()).
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
   * This flag is set by the alarm timer (or by a forced check) to indicate that
   * the root catalog TTL is expired, hence cvmfs should check upstream for an
   * updated copy.
   */
  atomic_int32 catalogs_expired_;
  /**
   * Only one thread must perform the actual remount (stopping user-level
   * caches, loading new catalog, etc.).  This is used to protect Finish() from
   * concurrent execution.
   */
  atomic_int32 critical_section_;
};  // class FuseRemounter

#endif  // CVMFS_FUSE_REMOUNT_H_
