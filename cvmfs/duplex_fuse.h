/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_DUPLEX_FUSE_H_
#define CVMFS_DUPLEX_FUSE_H_

#ifdef CVMFS_LIBCVMFS
// Unit tests
#include <sys/types.h>
#define FUSE_MAKE_VERSION(maj, min) ((maj) * 100 + (min))
#define FUSE_VERSION FUSE_MAKE_VERSION(3, 3)
#define FUSE_ROOT_ID 1
extern "C" {
typedef unsigned long fuse_ino_t;  // NOLINT
// Empty structs have different sizes in C and C++, hence the dummy int
struct fuse_session {
  int dummy;
};
struct fuse_lowlevel_ops {
  int dummy;
};  // for loader.h
enum fuse_expire_flags {
  FUSE_LL_EXPIRE_ONLY = (1 << 0),
};


// Defined in duplex_fuse.cc
extern unsigned fuse_lowlevel_notify_inval_inode_cnt;
extern unsigned fuse_lowlevel_notify_inval_entry_cnt;
static int __attribute__((used)) fuse_lowlevel_notify_inval_inode(
    struct fuse_session *, fuse_ino_t, off_t, off_t)  // NOLINT
{
  fuse_lowlevel_notify_inval_inode_cnt++;
  return -1;
}
static int __attribute__((used)) fuse_lowlevel_notify_inval_entry(
    struct fuse_session *, fuse_ino_t, const char *, size_t)  // NOLINT
{
  fuse_lowlevel_notify_inval_entry_cnt++;
  return -1;
}
}
#else  // CVMFS_LIBCVMFS

#ifdef CVMFS_ENABLE_FUSE3_LOOP_CONFIG
#define FUSE_USE_VERSION 312
#else
#define FUSE_USE_VERSION 31
#endif
#include <fuse3/fuse.h>
#include <fuse3/fuse_lowlevel.h>
#include <fuse3/fuse_opt.h>
#endif  // CVMFS_LIBCVMFS

#endif  // CVMFS_DUPLEX_FUSE_H_
