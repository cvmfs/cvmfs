/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FUSE_EVICT_H_
#define CVMFS_FUSE_EVICT_H_

#include <pthread.h>

#include "util/single_copy.h"

namespace glue {
class InodeTracker;
}

/**
 * This class can poke all known dentries out of the kernel caches.  This allows
 * for faster remount/reload of the fuse module because caches don't need to
 * drain out by timeout.
 */
class FuseInvalidator : SingleCopy {
 public:
  explicit FuseInvalidator(glue::InodeTracker *inode_tracker);
  ~FuseInvalidator();
  void Spawn();
  void Invalidate();

 private:
  static void *MainInvalidator(void *data);

  glue::InodeTracker *inode_tracker_;
  bool spawned_;
  int pipe_ctrl_[2];
  pthread_t thread_invalidator_;
};  // class FuseInvalidator

#endif  // CVMFS_FUSE_EVICT_H_
