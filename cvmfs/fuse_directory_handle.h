/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FUSE_DIRECTORY_HANDLE_H_
#define CVMFS_FUSE_DIRECTORY_HANDLE_H_

#include "util/algorithm.h"

#include <cstddef>
#include <cstdint>

#include <google/dense_hash_map>

namespace cvmfs {

/**
 * For cvmfs_opendir / cvmfs_readdir
 */
struct DirectoryListing {
  char *buffer;  /**< Filled by fuse_add_direntry */

  // Not really used anymore.  But directory listing needs to be migrated during
  // hotpatch. If buffer is allocated by smmap, capacity is zero.
  size_t size;
  size_t capacity;

  DirectoryListing() : buffer(NULL), size(0), capacity(0) { }
};

class DirectoryHandles : public google::dense_hash_map<uint64_t,
                                                       DirectoryListing,
                                                       hash_murmur<uint64_t> >
{
 public:
  DirectoryHandles() {
    set_empty_key((uint64_t)(-1));
    set_deleted_key((uint64_t)(-2));
  }
};

}  // namespace cvmfs

#endif  // CVMFS_FUSE_DIRECTORY_HANDLE_H_
