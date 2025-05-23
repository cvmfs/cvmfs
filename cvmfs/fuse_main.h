/**
 * This file is part of the CernVM File System.
 */
#include <cstddef>

#ifndef CVMFS_FUSE_MAIN_H_
#define CVMFS_FUSE_MAIN_H_

// The entry point in libcvmfs_stub to handover from cvmfs2
struct CvmfsStubExports {
  CvmfsStubExports()
      : version(0), size(sizeof(CvmfsStubExports)), fn_main(NULL) { }

  int version;
  int size;
  int (*fn_main)(int, char **);
};

#endif  // CVMFS_FUSE_MAIN_H_
