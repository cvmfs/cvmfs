/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FUSE_EVICT_H_
#define CVMFS_FUSE_EVICT_H_

#include <pthread.h>
#include <stdint.h>

#include "bigvector.h"
#include "duplex_fuse.h"
#include "gtest/gtest_prod.h"
#include "shortstring.h"
#include "util/atomic.h"
#include "util/concurrency.h"
#include "util/single_copy.h"

namespace glue {
class InodeTracker;
class DentryTracker;
}  // namespace glue

class MountPoint;

/**
 * This class can poke all known dentries out of the kernel caches.  This allows
 * for faster remount/reload of the fuse module because caches don't need to
 * drain out by timeout.  If the fuse library doesn't provide
 * fuse_lowlevel_notify_inval_entry, it falls back to waiting for drainout.
 *
 * Evicting entries from the cache must be done from a separate thread to
 * avoid a deadlock in the fuse callbacks (see Fuse documentation).
 */
class FuseInvalidator : SingleCopy {
  friend class T_FuseInvalidator;  // for T_FuseInvalidator.SetUp()
  FRIEND_TEST(T_FuseInvalidator, StartStop);
  FRIEND_TEST(T_FuseInvalidator, InvalidateTimeout);
  FRIEND_TEST(T_FuseInvalidator, InvalidateOps);

 public:
  static bool HasFuseNotifyInval();

  /**
   * Used to track the progress of an "invalidation" request.  The invalidator
   * will evict cache entries for a duration given by the timeout.  For very
   * large caches, active eviction can take longer than the timeout.
   *
   * The caller needs to keep Handle active until IsDone() is true;
   */
  class Handle : SingleCopy {
    friend class FuseInvalidator;

   public:
    explicit Handle(unsigned timeout_s);
    ~Handle();
    bool IsDone() const { return atomic_read32(status_) == 1; }
    void Reset() { atomic_write32(status_, 0); }
    void WaitFor();

   private:
    void SetDone() { atomic_cas32(status_, 0, 1); }

    unsigned timeout_s_;
    atomic_int32 *status_;
  };

  struct Command {
    virtual ~Command() { }
  };
  struct QuitCommand : public Command { };
  struct InvalInodesCommand : public Command {
    Handle *handle;
  };
  struct InvalDentryCommand : public Command {
    uint64_t parent_ino;
    NameString name;
  };

  FuseInvalidator(MountPoint *mountpoint,
                  void **fuse_channel_or_session,
                  bool fuse_notify_invalidation);
  ~FuseInvalidator();
  void Spawn();
  void InvalidateInodes(Handle *handle);

  void InvalidateDentry(uint64_t parent_ino, const NameString &name);

 private:
  /**
   * CONSTRUCTOR ONLY FOR UNITTESTS - mountpoint will illegally be null
   * ( we do not want to construct a full mountpoint in the unittest )
   */
  FuseInvalidator(glue::InodeTracker *inode_tracker,
                  glue::DentryTracker *dentry_tracker,
                  void **fuse_channel_or_session,
                  bool fuse_notify_invalidation);
  /**
   * Add one second to the caller-provided timeout to be on the safe side.
   */
  static const unsigned kTimeoutSafetyMarginSec;  // = 1;
  /**
   * If caches are drained out by timeout, set a polling interval.
   */
  static const unsigned kCheckTimeoutFreqMs;  // = 100;
  /**
   * If caches are actively drained out, check every so many operations if the
   * caches are anyway drained out by timeout.
   */
  static const unsigned kCheckTimeoutFreqOps;  // = 256

  static void *MainInvalidator(void *data);

  MountPoint *mount_point_;

  glue::InodeTracker *inode_tracker_;
  glue::DentryTracker *dentry_tracker_;
  /**
   * libfuse2 uses struct fuse_chan, libfuse3 uses struct fuse_session
   */
  void **fuse_channel_or_session_;
  bool spawned_;
  Channel<Command> channel_;
  pthread_t thread_invalidator_;
  /**
   * An invalidation run can take some time.  Allow for early cancellation if
   * thread should be shut down.
   */
  atomic_int32 terminated_;
  BigVector<uint64_t> evict_list_;

  static bool g_fuse_notify_invalidation_;
};  // class FuseInvalidator

#endif  // CVMFS_FUSE_EVICT_H_
