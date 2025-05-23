/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_SHRINKWRAP_POSIX_GARBAGE_COLLECTOR_H_
#define CVMFS_SHRINKWRAP_POSIX_GARBAGE_COLLECTOR_H_

#include "statistics.h"

struct posix_gc_thread {
  struct fs_traversal_context *ctx;
  perf::Statistics *stat;
  unsigned thread_total;
  unsigned thread_num;
};

void InitializeGarbageCollection(struct fs_traversal_context *ctx);
void FinalizeGarbageCollection(struct fs_traversal_context *ctx);
int RunGarbageCollection(struct fs_traversal_context *ctx);

#endif  // CVMFS_SHRINKWRAP_POSIX_GARBAGE_COLLECTOR_H_
