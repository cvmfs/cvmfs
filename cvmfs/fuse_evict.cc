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
#include "logging.h"
#include "platform.h"
#include "shortstring.h"
#include "smalloc.h"
#include "util/posix.h"

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
  glue::InodeTracker *inode_tracker,
  glue::NentryTracker *nentry_tracker,
  void **fuse_channel_or_session,
  bool fuse_notify_invalidation)
  : inode_tracker_(inode_tracker)
  , nentry_tracker_(nentry_tracker)
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


void *FuseInvalidator::MainInvalidator(void *data) {
  FuseInvalidator *invalidator = reinterpret_cast<FuseInvalidator *>(data);
  LogCvmfs(kLogCvmfs, kLogDebug, "starting dentry invalidator thread");

  char c;
  Handle *handle;
  while (true) {
    ReadPipe(invalidator->pipe_ctrl_[0], &c, 1);
    if (c == 'Q')
      break;

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
#if CVMFS_USE_LIBFUSE == 2
      fuse_lowlevel_notify_inval_inode(*reinterpret_cast<struct fuse_chan**>(
        invalidator->fuse_channel_or_session_), inode, 0, 0);
#else
      fuse_lowlevel_notify_inval_inode(*reinterpret_cast<struct fuse_session**>(
        invalidator->fuse_channel_or_session_), inode, 0, 0);
#endif
      LogCvmfs(kLogCvmfs, kLogDebug, "evicting inode %" PRIu64, inode);

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

    // Do the nentry tracker last to increase the effectiveness of pruning
    invalidator->nentry_tracker_->Prune();
    // Copy and empty the nentry tracker in a single atomic operation
    glue::NentryTracker *nentries_copy = invalidator->nentry_tracker_->Move();
    glue::NentryTracker::Cursor nentry_cursor = nentries_copy->BeginEnumerate();
    uint64_t entry_parent;
    NameString entry_name;
    i = 0;
    while (nentries_copy->NextEntry(&nentry_cursor, &entry_parent, &entry_name))
    {
      // Can fail, e.g. the entry might be already evicted
#if CVMFS_USE_LIBFUSE == 2
      fuse_lowlevel_notify_inval_entry(*reinterpret_cast<struct fuse_chan**>(
        invalidator->fuse_channel_or_session_),
        entry_parent, entry_name.GetChars(), entry_name.GetLength());
#else
      fuse_lowlevel_notify_inval_entry(*reinterpret_cast<struct fuse_session**>(
        invalidator->fuse_channel_or_session_),
        entry_parent, entry_name.GetChars(), entry_name.GetLength());
#endif
      if ((++i % kCheckTimeoutFreqOps) == 0) {
        if (atomic_read32(&invalidator->terminated_) == 1) {
          LogCvmfs(kLogCvmfs, kLogDebug,
                   "cancel cache eviction due to termination");
          break;
        }
      }
    }
    nentries_copy->EndEnumerate(&nentry_cursor);
    delete nentries_copy;

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
