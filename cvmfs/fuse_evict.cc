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

// TODO(heretherebedragons) - we should remove this. This is set based on
// CVMFS_FUSE_NOTIFY_INVALIDATION (which we then should also remove)
// - introduced for macos. but now we have it
// hardcoded to use timeout to drain the kernel cache on timeout.
bool FuseInvalidator::g_fuse_notify_invalidation_ = true;

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


void FuseInvalidator::InvalidateInodesAndDentries(Handle *handle) {
  assert(handle != NULL);
  char c = 'B';
  WritePipe(pipe_ctrl_[1], &c, 1);
  WritePipe(pipe_ctrl_[1], &handle, sizeof(handle));
}

void FuseInvalidator::InvalidateInodesNoEvictAndDentries(Handle *handle) {
  assert(handle != NULL);
  char c = 'X';
  WritePipe(pipe_ctrl_[1], &c, 1);
  WritePipe(pipe_ctrl_[1], &handle, sizeof(handle));
}

void FuseInvalidator::InvalidateDentries(Handle *handle) {
  assert(handle != NULL);
  char c = 'D';
  WritePipe(pipe_ctrl_[1], &c, 1);
  WritePipe(pipe_ctrl_[1], &handle, sizeof(handle));
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
  char c = 'S';
  WritePipe(pipe_ctrl_[1], &c, 1);
  WritePipe(pipe_ctrl_[1], &parent_ino, sizeof(parent_ino));
  unsigned len = name.GetLength();
  WritePipe(pipe_ctrl_[1], &len, sizeof(len));
  WritePipe(pipe_ctrl_[1], name.GetChars(), len);
}

void *FuseInvalidator::MainInvalidator(void *data) {
  FuseInvalidator *invalidator = reinterpret_cast<FuseInvalidator *>(data);
  LogCvmfs(kLogCvmfs, kLogDebug, "starting dentry invalidator thread");

#ifdef __APPLE__
  bool reported_missing_inval_support = false;
#endif
  char c;
  Handle *handle;
  while (true) {
    ReadPipe(invalidator->pipe_ctrl_[0], &c, 1);
    switch (c) {
      case 'Q':  // quit
        goto quit_thread;
      break;
      case 'S':  // invalidate "S"ingle dentry
#ifdef __APPLE__
        if (!reported_missing_inval_support) {
          LogCvmfs(kLogCvmfs, kLogSyslogWarn,
                    "missing fuse support for dentry invalidation (%lu/%s)",
                    parent_ino, name);
          reported_missing_inval_support = true;
        }
#else
        invalidator->DoInvalidateDentry();
#endif
      break;
      case 'I':  // invalidate all "I"nodes
        ReadPipe(invalidator->pipe_ctrl_[0], &handle, sizeof(handle));
#ifdef __APPLE__
        ClearCacheByTimeout(handle);
#else
        invalidator->DoInvalidateInodes(handle);
        invalidator->evict_list_.Clear(); 
#endif
        handle->SetDone();
      break;
      case 'D':  // invalidate all "D"entries
        ReadPipe(invalidator->pipe_ctrl_[0], &handle, sizeof(handle));
#ifdef __APPLE__
        ClearCacheByTimeout(handle);
#else
        invalidator->DoInvalidateDentries();
#endif
        handle->SetDone();
      break;
      case 'B':  // invalidate all "B"oth: inodes and dentries
        ReadPipe(invalidator->pipe_ctrl_[0], &handle, sizeof(handle));
#ifdef __APPLE__
        ClearCacheByTimeout(handle);
#else
        invalidator->DoInvalidateDentries();
        invalidator->DoInvalidateInodes(handle);
        invalidator->evict_list_.Clear();
#endif
        handle->SetDone();
      break;
      case 'X':  // invalidate all both: inodes (do not delete evict list)
                 // and dentries
        ReadPipe(invalidator->pipe_ctrl_[0], &handle, sizeof(handle));
#ifdef __APPLE__
        ClearCacheByTimeout(handle);
#else
        invalidator->DoInvalidateDentries();
        invalidator->DoInvalidateInodes(handle);
#endif
        handle->SetDone();
      break;
      default:
      // TODO(heretherebedragons) PANIC?
      break;
    }
  }

  quit_thread:
    LogCvmfs(kLogCvmfs, kLogDebug, "stopping dentry invalidator thread");
    return NULL;
}

void FuseInvalidator::ClearCacheByTimeout(Handle *handle) {
  LogCvmfs(kLogCvmfs, kLogDebug,
          "invalidating kernel caches, timeout %u", handle->timeout_s_);
  uint64_t deadline = platform_monotonic_time() + handle->timeout_s_;
  LogCvmfs(kLogCvmfs, kLogDebug,
                "fallback: (inode) cache eviction by request not supported, "
                "using drainout by timeout");
  while (platform_monotonic_time() < deadline) {
    SafeSleepMs(kCheckTimeoutFreqMs);
    if (atomic_read32(&terminated_) == 1) {
      LogCvmfs(kLogCvmfs, kLogDebug,
                          "cancel (inode) cache eviction due to termination");
      break;
    }
  }
}

void FuseInvalidator::DoInvalidateDentry() {
  uint64_t parent_ino;
  unsigned len;
  ReadPipe(pipe_ctrl_[0], &parent_ino, sizeof(parent_ino));
  ReadPipe(pipe_ctrl_[0], &len, sizeof(len));
  char *name = static_cast<char *>(smalloc(len + 1));
  ReadPipe(pipe_ctrl_[0], name, len);
  name[len] = '\0';
  LogCvmfs(kLogCvmfs, kLogDebug, "evicting single dentry %" PRIu64 "/%s",
                                 parent_ino, name);
#if CVMFS_USE_LIBFUSE == 2
  fuse_lowlevel_notify_inval_entry(*reinterpret_cast<struct fuse_chan**>(
                      fuse_channel_or_session_), parent_ino, name, len);
#else
  fuse_lowlevel_notify_inval_entry(*reinterpret_cast<struct fuse_session**>(
                              fuse_channel_or_session_), parent_ino, name, len);
#endif
  free(name);
}

void FuseInvalidator::DoInvalidateInodes(Handle *handle) {
  LogCvmfs(kLogCvmfs, kLogDebug,
          "invalidating kernel caches: inodes, timeout %u", handle->timeout_s_);

  uint64_t deadline = platform_monotonic_time() + handle->timeout_s_;

  // We must not hold a lock when calling fuse_lowlevel_notify_inval_entry.
  // Therefore, we first copy all the inodes into a temporary data structure.
  glue::InodeTracker::Cursor inode_cursor(inode_tracker_->BeginEnumerate());
  uint64_t inode;
  while (inode_tracker_->NextInode(&inode_cursor, &inode)) {
    evict_list_.PushBack(inode);
  }
  inode_tracker_->EndEnumerate(&inode_cursor);

  unsigned i = 0;
  unsigned N = evict_list_.size();
  while (i < N) {
    uint64_t inode = evict_list_.At(i);
    if (inode == 0) {
      inode = FUSE_ROOT_ID;
    }
    // Can fail, e.g. the inode might be already evicted

    int dbg_retval;

#if CVMFS_USE_LIBFUSE == 2
    dbg_retval = fuse_lowlevel_notify_inval_inode(
                                        *reinterpret_cast<struct fuse_chan**>(
                                        fuse_channel_or_session_), inode, 0, 0);
#else
    dbg_retval = fuse_lowlevel_notify_inval_inode(
                                       *reinterpret_cast<struct fuse_session**>(
                                       fuse_channel_or_session_), inode, 0, 0);
#endif
    LogCvmfs(kLogCvmfs, kLogDebug, "evicting inode %" PRIu64 " with retval: %d",
                                   inode, dbg_retval);

    (void) dbg_retval;  // prevent compiler complaining

    if ((++i % kCheckTimeoutFreqOps) == 0) {
      if (platform_monotonic_time() >= deadline) {
        LogCvmfs(kLogCvmfs, kLogDebug,
                    "cancel cache eviction after %u entries due to timeout", i);
        break;
      }
      if (atomic_read32(&terminated_) == 1) {
        LogCvmfs(kLogCvmfs, kLogDebug,
                                    "cancel cache eviction due to termination");
        break;
      }
    }
  }
}

void FuseInvalidator::DoInvalidateDentries() {
  // Do not do any prune. Can create deadlocks and give back broken symlinks
  // during catalog reload

  // Copy and empty the dentry tracker in a single atomic operation
  glue::DentryTracker *dentries_copy = dentry_tracker_->Move();
  glue::DentryTracker::Cursor dentry_cursor = dentries_copy->BeginEnumerate();
  uint64_t entry_parent;
  NameString entry_name;
  unsigned i = 0;

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
  if (mount_point_->fuse_expire_entry() && mount_point_ != NULL) {
    notify_func = &fuse_lowlevel_notify_expire_entry;
  }
#endif
#endif

  while (dentries_copy->NextEntry(&dentry_cursor, &entry_parent, &entry_name)) {
    LogCvmfs(kLogCvmfs, kLogDebug, "evicting dentry %lu --> %s",
                                   entry_parent, entry_name.c_str());
    // Can fail, e.g. the entry might be already evicted
#if CVMFS_USE_LIBFUSE == 2
    struct fuse_chan* channel_or_session =*reinterpret_cast<struct fuse_chan**>(
                                                      fuse_channel_or_session_);
#else
    struct fuse_session* channel_or_session =
                                       *reinterpret_cast<struct fuse_session**>(
                                                      fuse_channel_or_session_);
#endif

    notify_func(channel_or_session, entry_parent, entry_name.GetChars(),
                                                  entry_name.GetLength());

    if ((++i % kCheckTimeoutFreqOps) == 0) {
      if (atomic_read32(&terminated_) == 1) {
        LogCvmfs(kLogCvmfs, kLogDebug,
                                    "cancel cache eviction due to termination");
        break;
      }
    }
  }
  dentries_copy->EndEnumerate(&dentry_cursor);
  delete dentries_copy;
}


void FuseInvalidator::Spawn() {
  int retval;
  retval = pthread_create(&thread_invalidator_, NULL, MainInvalidator, this);
  assert(retval == 0);
  spawned_ = true;
}
