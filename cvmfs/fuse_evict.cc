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
  // Invalidation of entries can silently remove mount points on top of cvmfs.
  // Turning active evition off for the time being.
  // return FUSE_VERSION >= 29;
  return false;
}


FuseInvalidator::FuseInvalidator(
  glue::InodeTracker *inode_tracker,
  struct fuse_chan **fuse_channel)
  : inode_tracker_(inode_tracker)
  , fuse_channel_(fuse_channel)
  , spawned_(false)
{
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


void FuseInvalidator::InvalidateDentries(Handle *handle) {
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
    if ((invalidator->fuse_channel_ == NULL) || !HasFuseNotifyInval()) {
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
    EvictableObject evictable_object;
    glue::InodeTracker::Cursor cursor(
      invalidator->inode_tracker_->BeginEnumerate());
    while (invalidator->inode_tracker_->Next(
             &cursor, &evictable_object.inode, &evictable_object.name))
    {
      invalidator->evict_list_.PushBack(evictable_object);
    }
    invalidator->inode_tracker_->EndEnumerate(&cursor);

    unsigned i = 0;
    unsigned N = invalidator->evict_list_.size();
    while (i < N) {
      evictable_object = invalidator->evict_list_.At(i);
      if (evictable_object.inode == 0)
        evictable_object.inode = FUSE_ROOT_ID;
      // Can return non-zero value if parent entry was already evicted
      fuse_lowlevel_notify_inval_entry(
        *invalidator->fuse_channel_,
        evictable_object.inode,
        evictable_object.name.GetChars(),
        evictable_object.name.GetLength());
      LogCvmfs(kLogCvmfs, kLogDebug, "evicting <%" PRIu64 ">/%s",
               evictable_object.inode, evictable_object.name.c_str());

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
