/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FUSE_INODE_GEN_H_
#define CVMFS_FUSE_INODE_GEN_H_

#include <stdint.h>

namespace cvmfs {

/**
 * Stores the initial catalog revision (in order to detect overflows) and
 * the incarnation (number of reloads) of the Fuse module
 */
struct InodeGenerationInfo {
  InodeGenerationInfo() {
    version = 2;
    initial_revision = 0;
    incarnation = 0;
    overflow_counter = 0;
    inode_generation = 0;
  }
  unsigned version;
  uint64_t initial_revision;
  uint32_t incarnation;
  uint32_t overflow_counter;  // not used any more
  uint64_t inode_generation;
};

}  // namespace cvmfs

#endif  // CVMFS_FUSE_INODE_GEN_H_
