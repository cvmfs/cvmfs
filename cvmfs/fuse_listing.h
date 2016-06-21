/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FUSE_LISTING_H_
#define CVMFS_FUSE_LISTING_H_

#include <inttypes.h>
#include <pthread.h>
#include <sys/types.h>

#include <cassert>
#include <cstdlib>

#include "google/dense_hash_map"
#include "util/algorithm.h"
#include "util/pointer.h"

struct FuseListing {
  char *buffer;  /**< Filled by fuse_add_direntry */

  // Not really used anymore.  But directory listing needs to be migrated during
  // hotpatch. If buffer is allocated by smmap, capacity is zero.
  size_t size;
  size_t capacity;

  FuseListing() : buffer(NULL), size(0), capacity(0) { }
};

typedef google::dense_hash_map<uint64_t, FuseListing,
                               hash_murmur<uint64_t> >
        FuseDirectoryHandles;


struct FuseDirectoryMap : SingleCopy {
  FuseDirectoryMap() : next_directory_handle_(0) {
    int retval = pthread_mutex_init(&lock_handles, NULL);
    assert(retval == 0);
  }

  ~FuseDirectoryMap() {
    pthread_mutex_destroy(&lock_handles);
  }

  FuseDirectoryHandles handles;
  pthread_mutex_t lock_handles;
  uint64_t next_directory_handle_ = 0;
};

#endif  // CVMFS_FUSE_LISTING_H_
