/**
 * This file is part of the CernVM File System.
 */

// The entry point in libcvmfs_stub to handover from cvmfs2
struct CvmfsStubExports {
  CvmfsStubExports()
    : version(0)
    , size(sizeof(CvmfsStubExports))
    , fn_main(NULL)
  {}

  int version;
  int size;
  int (*fn_main)(int, char**);
};
