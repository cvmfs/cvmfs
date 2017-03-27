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

#include "logging.h"
#include "glue_buffer.h"
#include "platform.h"
#include "shortstring.h"
#include "smalloc.h"
#include "util/posix.h"

using namespace std;  // NOLINT

FuseInvalidator::Handle::Handle(unsigned timeout_s)
  : timeout_s_(timeout_s)
{
  status_ = reinterpret_cast<atomic_int32 *>(smalloc(sizeof(atomic_int32)));
  atomic_init32(status_);
}


FuseInvalidator::Handle::~Handle() {
  free(status_);
}


void FuseInvalidator::Handle::WaitFor() {
  while (!IsDone()) SafeSleepMs(100);
}


//------------------------------------------------------------------------------


FuseInvalidator::FuseInvalidator(
  glue::InodeTracker *inode_tracker,
  struct fuse_chan **fuse_channel)
 : inode_tracker_(inode_tracker)
 , fuse_channel_(fuse_channel)
 , spawned_(false)
{
  MakePipe(pipe_ctrl_);
  atomic_init32(&terminated_);
}


FuseInvalidator::~FuseInvalidator() {
  atomic_cas32(&terminated_, 0, 1);
  if (spawned_) {
    char c = 'Q';
    WritePipe(pipe_ctrl_[1], &c, 1);
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

    uint64_t deadline =
      platform_monotonic_time() + handle->timeout_s_ + kTimeoutSafetyMarginSec;

    // Fallback: drainout by timeout
    if (invalidator->fuse_channel_ == NULL) {
      while (platform_monotonic_time() <= deadline) {
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

    uint64_t inode;
    NameString name;
    glue::InodeTracker::Cursor cursor(
      invalidator->inode_tracker_->BeginEnumerate());
    unsigned i = 0;
    while (invalidator->inode_tracker_->Next(&cursor, &inode, &name)) {
      if (inode == 0)
        inode = FUSE_ROOT_ID;
      // Can return non-zero value if parent entry was already evicted
      fuse_lowlevel_notify_inval_entry(
        *invalidator->fuse_channel_,
        inode,
        name.GetChars(),
        name.GetLength()
      );
      LogCvmfs(kLogCvmfs, kLogDebug, "evicting <%" PRIu64 ">/%s",
               inode, name.c_str());

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
    invalidator->inode_tracker_->EndEnumerate(&cursor);
    handle->SetDone();
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
