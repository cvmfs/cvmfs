/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "fuse_evict.h"

#include <inttypes.h>
#include <stdint.h>

#include <cassert>
#include <cstdlib>
#include <cstring>

#include "glue_buffer.h"
#include "mountpoint.h"
#include "shortstring.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

FuseInvalidator::Handle::Handle(unsigned timeout_s)
  : timeout_s_((timeout_s == 0) ? 0 : (timeout_s + kTimeoutSafetyMarginSec))
{
  status_ = reinterpret_cast<atomic_int32 *>(smalloc(sizeof(atomic_int32)));
  atomic_init32(status_);
}


FuseInvalidator::Handle::~Handle() {
  free(status_);
}


void FuseInvalidator::Handle::WaitFor() {
  while (!IsDone()) SafeSleepMs(FuseInvalidator::kCheckTimeoutFreqMs);
}


//------------------------------------------------------------------------------


const unsigned FuseInvalidator::kTimeoutSafetyMarginSec = 1;
const unsigned FuseInvalidator::kCheckTimeoutFreqMs = 100;
const unsigned FuseInvalidator::kCheckTimeoutFreqOps = 256;

bool FuseInvalidator::g_fuse_notify_invalidation_ = true;

bool FuseInvalidator::HasFuseNotifyInval() {
  /**
   * Technically, also libfuse 2.8 has support.  Libfuse 2.8 comes with EL6,
   * which had bugs reported related to the fuse_notify_inval_...() functions.
   * Since just waiting for the timeout works perfectly fine, there is no reason
   * to optimize for forced cache eviction too aggressively.
   *
   * TODO(jblomer): could we have libfuse 2.9 or higher with a very old kernel
   * that doesn't support active invalidation?  How old does the kernel need
   * to be?  Probably that situation is never triggered in practice.
   */
  return FuseInvalidator::g_fuse_notify_invalidation_ && (FUSE_VERSION >= 29);
}


FuseInvalidator::FuseInvalidator(
  MountPoint *mount_point,
  void **fuse_channel_or_session,
  bool fuse_notify_invalidation)
  : mount_point_(mount_point)
  , inode_tracker_(mount_point->inode_tracker())
  , dentry_tracker_(mount_point->dentry_tracker())
  , fuse_channel_or_session_(fuse_channel_or_session)
  , spawned_(false)
{
  g_fuse_notify_invalidation_ = fuse_notify_invalidation;
  MakePipe(pipe_ctrl_);
  memset(&thread_invalidator_, 0, sizeof(thread_invalidator_));
  atomic_init32(&terminated_);
}

FuseInvalidator::FuseInvalidator(
  glue::InodeTracker *inode_tracker,
  glue::DentryTracker *dentry_tracker,
  void **fuse_channel_or_session,
  bool fuse_notify_invalidation)
  : mount_point_(NULL)
  , inode_tracker_(inode_tracker)
  , dentry_tracker_(dentry_tracker)
  , fuse_channel_or_session_(fuse_channel_or_session)
  , spawned_(false)
{
  g_fuse_notify_invalidation_ = fuse_notify_invalidation;
  MakePipe(pipe_ctrl_);
  memset(&thread_invalidator_, 0, sizeof(thread_invalidator_));
  atomic_init32(&terminated_);
}


FuseInvalidator::~FuseInvalidator() {
  atomic_cas32(&terminated_, 0, 1);
  if (spawned_) {
    char c = 'Q';
    WritePipe(pipe_ctrl_[1], &c, 1);
    pthread_join(thread_invalidator_, NULL);
  }
  ClosePipe(pipe_ctrl_);
}


void FuseInvalidator::InvalidateInodes(Handle *handle) {
  assert(handle != NULL);
  char c = 'I';
  WritePipe(pipe_ctrl_[1], &c, 1);
  WritePipe(pipe_ctrl_[1], &handle, sizeof(handle));
}

void FuseInvalidator::InvalidateDentry(
  uint64_t parent_ino, const NameString &name)
{
  char c = 'D';
  WritePipe(pipe_ctrl_[1], &c, 1);
  WritePipe(pipe_ctrl_[1], &parent_ino, sizeof(parent_ino));
  unsigned len = name.GetLength();
  WritePipe(pipe_ctrl_[1], &len, sizeof(len));
  WritePipe(pipe_ctrl_[1], name.GetChars(), len);
}

void *FuseInvalidator::MainInvalidator(void *data) {
  FuseInvalidator *invalidator = reinterpret_cast<FuseInvalidator *>(data);
  LogCvmfs(kLogCvmfs, kLogDebug, "starting dentry invalidator thread");

  bool reported_missing_inval_support = false;
  char c;
  Handle *handle;
  while (true) {
    ReadPipe(invalidator->pipe_ctrl_[0], &c, 1);
    if (c == 'Q')
      break;

    if (c == 'D') {
      uint64_t parent_ino;
      unsigned len;
      ReadPipe(invalidator->pipe_ctrl_[0], &parent_ino, sizeof(parent_ino));
      ReadPipe(invalidator->pipe_ctrl_[0], &len, sizeof(len));
      char *name = static_cast<char *>(smalloc(len + 1));
      ReadPipe(invalidator->pipe_ctrl_[0], name, len);
      name[len] = '\0';
      if (invalidator->fuse_channel_or_session_ == NULL) {
        if (!reported_missing_inval_support) {
          LogCvmfs(kLogCvmfs, kLogSyslogWarn,
                   "missing fuse support for dentry invalidation (%lu/%s)",
                   parent_ino, name);
          reported_missing_inval_support = true;
        }
        free(name);
        continue;
      }
      LogCvmfs(kLogCvmfs, kLogDebug, "evicting single dentry %" PRIu64 "/%s",
               parent_ino, name);
#if CVMFS_USE_LIBFUSE == 2
      fuse_lowlevel_notify_inval_entry(*reinterpret_cast<struct fuse_chan**>(
        invalidator->fuse_channel_or_session_), parent_ino, name, len);
#else
      fuse_lowlevel_notify_inval_entry(*reinterpret_cast<struct fuse_session**>(
        invalidator->fuse_channel_or_session_), parent_ino, name, len);
#endif
      free(name);
      continue;
    }

    assert(c == 'I');
    ReadPipe(invalidator->pipe_ctrl_[0], &handle, sizeof(handle));
    LogCvmfs(kLogCvmfs, kLogDebug, "invalidating kernel caches, timeout %u",
             handle->timeout_s_);

    uint64_t deadline = platform_monotonic_time() + handle->timeout_s_;

    // Fallback: drainout by timeout
    if ((invalidator->fuse_channel_or_session_ == NULL) ||
        !HasFuseNotifyInval())
    {
      while (platform_monotonic_time() < deadline) {
        SafeSleepMs(kCheckTimeoutFreqMs);
        if (atomic_read32(&invalidator->terminated_) == 1) {
          LogCvmfs(kLogCvmfs, kLogDebug,
                   "cancel cache eviction due to termination");
          break;
        }
      }
      handle->SetDone();
      continue;
    }

    // We must not hold a lock when calling fuse_lowlevel_notify_inval_entry.
    // Therefore, we first copy all the inodes into a temporary data structure.
    glue::InodeTracker::Cursor inode_cursor(
      invalidator->inode_tracker_->BeginEnumerate());
    uint64_t inode;
    while (invalidator->inode_tracker_->NextInode(&inode_cursor, &inode))
    {
      invalidator->evict_list_.PushBack(inode);
    }
    invalidator->inode_tracker_->EndEnumerate(&inode_cursor);

    unsigned i = 0;
    unsigned N = invalidator->evict_list_.size();
    while (i < N) {
      uint64_t inode = invalidator->evict_list_.At(i);
      if (inode == 0)
        inode = FUSE_ROOT_ID;
      // Can fail, e.g. the inode might be already evicted

      int dbg_retval;

#if CVMFS_USE_LIBFUSE == 2
      dbg_retval = fuse_lowlevel_notify_inval_inode(
                    *reinterpret_cast<struct fuse_chan**>(
                    invalidator->fuse_channel_or_session_), inode, 0, 0);
#else
      dbg_retval = fuse_lowlevel_notify_inval_inode(
                    *reinterpret_cast<struct fuse_session**>(
                    invalidator->fuse_channel_or_session_), inode, 0, 0);
#endif
      LogCvmfs(kLogCvmfs, kLogDebug,
                "evicting inode %" PRIu64 " with retval: %d",
                inode, dbg_retval);

      (void) dbg_retval;  // prevent compiler complaining

      if ((++i % kCheckTimeoutFreqOps) == 0) {
        if (platform_monotonic_time() >= deadline) {
          LogCvmfs(kLogCvmfs, kLogDebug,
                   "cancel cache eviction after %u entries due to timeout", i);
          break;
        }
        if (atomic_read32(&invalidator->terminated_) == 1) {
          LogCvmfs(kLogCvmfs, kLogDebug,
                   "cancel cache eviction due to termination");
          break;
        }
      }
    }

    // Do the dentry tracker last to increase the effectiveness of pruning
    invalidator->dentry_tracker_->Prune();
    // Copy and empty the dentry tracker in a single atomic operation
    glue::DentryTracker *dentries_copy = invalidator->dentry_tracker_->Move();
    glue::DentryTracker::Cursor dentry_cursor = dentries_copy->BeginEnumerate();
    uint64_t entry_parent;
    NameString entry_name;
    i = 0;

#if CVMFS_USE_LIBFUSE == 2
    int (*notify_func)(struct fuse_chan*, fuse_ino_t, const char*, size_t);
    notify_func = &fuse_lowlevel_notify_inval_entry;
#else
    int (*notify_func)(struct fuse_session*, fuse_ino_t, const char*, size_t);
    notify_func = &fuse_lowlevel_notify_inval_entry;
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 16)
    // must be libfuse >= 3.16, otherwise the signature is wrong and it
    // will fail building
    // mount_point can only be NULL for unittests
    if (invalidator->mount_point_ != NULL &&
        invalidator->mount_point_->fuse_expire_entry()) {
      notify_func = &fuse_lowlevel_notify_expire_entry;
    }
#endif
#endif

    while (dentries_copy->NextEntry(&dentry_cursor, &entry_parent, &entry_name))
    {
      LogCvmfs(kLogCvmfs, kLogDebug, "evicting dentry %lu --> %s",
               entry_parent, entry_name.c_str());
      // Can fail, e.g. the entry might be already evicted
#if CVMFS_USE_LIBFUSE == 2
      struct fuse_chan* channel_or_session =
                                    *reinterpret_cast<struct fuse_chan**>(
                                     invalidator->fuse_channel_or_session_);
#else
      struct fuse_session* channel_or_session =
                                  *reinterpret_cast<struct fuse_session**>(
                                  invalidator->fuse_channel_or_session_);
#endif

      notify_func(channel_or_session, entry_parent, entry_name.GetChars(),
                                                    entry_name.GetLength());

      if ((++i % kCheckTimeoutFreqOps) == 0) {
        if (atomic_read32(&invalidator->terminated_) == 1) {
          LogCvmfs(kLogCvmfs, kLogDebug,
                   "cancel cache eviction due to termination");
          break;
        }
      }
    }
    dentries_copy->EndEnumerate(&dentry_cursor);
    delete dentries_copy;

    handle->SetDone();
    invalidator->evict_list_.Clear();
  }

  LogCvmfs(kLogCvmfs, kLogDebug, "stopping dentry invalidator thread");
  return NULL;
}


void FuseInvalidator::Spawn() {
  int retval;
  retval = pthread_create(&thread_invalidator_, NULL, MainInvalidator, this);
  assert(retval == 0);
  spawned_ = true;
}
