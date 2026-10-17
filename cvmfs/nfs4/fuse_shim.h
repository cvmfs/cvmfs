/**
 * This file is part of the CernVM File System.
 *
 * Internals of libcvmfs_nfsfuse, a replacement for the parts of libfuse3 that
 * cvmfs uses.  Instead of talking to /dev/fuse, the replacement serves the
 * registered fuse_lowlevel_ops over NFSv4 and mounts the file system with the
 * kernel's NFS client, which needs no kernel extension on macOS.
 */

#ifndef CVMFS_NFS4_FUSE_SHIM_H_
#define CVMFS_NFS4_FUSE_SHIM_H_

#include <sys/stat.h>
#include <sys/statvfs.h>

#include <string>

#include "duplex_fuse.h"

/**
 * libfuse's request handle.  cvmfs answers every request before its callback
 * returns, so the reply is simply stored here for the caller.
 */
struct fuse_req {
  enum Reply {
    kNone = 0,
    kErr,
    kEntry,
    kAttr,
    kOpen,
    kBuf,
    kReadlink,
    kStatfs,
    kXattr,
  };

  fuse_req();

  Reply reply;
  int err;
  struct fuse_entry_param entry;
  struct stat attr;
  struct fuse_file_info fi;
  struct statvfs statfs;
  size_t xattr_size;
  std::string buf;
  struct fuse_ctx ctx;
};

#endif  // CVMFS_NFS4_FUSE_SHIM_H_
