/**
 * This file is part of the CernVM File System.
 */

#include "duplex_fuse.h"

#ifdef CVMFS_LIBCVMFS

extern "C" {
unsigned fuse_lowlevel_notify_inval_inode_cnt = 0;
unsigned fuse_lowlevel_notify_inval_entry_cnt = 0;
}

#endif  // CVMFS_LIBCVMFS
