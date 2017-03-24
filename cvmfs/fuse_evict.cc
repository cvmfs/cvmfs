/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "fuse_evict.h"

#include <cassert>

#include "logging.h"
#include "glue_buffer.h"
#include "util/posix.h"

using namespace std;  // NOLINT

FuseInvalidator::FuseInvalidator(glue::InodeTracker *inode_tracker)
 : inode_tracker_(inode_tracker)
 , spawned_(false)
{
  MakePipe(pipe_ctrl_);
}


FuseInvalidator::~FuseInvalidator() {
  if (spawned_) {
    char c = 'Q';
    WritePipe(pipe_ctrl_[1], &c, 1);
  }
  ClosePipe(pipe_ctrl_);
}


void FuseInvalidator::Invalidate() {
  char c = 'I';
  WritePipe(pipe_ctrl_[1], &c, 1);
}


void *FuseInvalidator::MainInvalidator(void *data) {
  FuseInvalidator *invalidator = reinterpret_cast<FuseInvalidator *>(data);
  LogCvmfs(kLogCvmfs, kLogDebug, "starting dentry invalidator thread");

  char c;
  while (true) {
    ReadPipe(invalidator->pipe_ctrl_[0], &c, 1);
    if (c == 'Q')
      break;
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
