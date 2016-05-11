/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FUSE_LISTING_H_
#define CVMFS_FUSE_LISTING_H_

#include <inttypes.h>
#include <sys/types.h>

#include "google/dense_hash_map"
#include "util/algorithm.h"

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

#endif  // CVMFS_FUSE_LISTING_H_
