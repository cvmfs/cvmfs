/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_DUPLEX_FUSE_H_
#define CVMFS_DUPLEX_FUSE_H_

#ifdef CVMFS_LIBCVMFS
// Unit tests
#define FUSE_VERSION 29
#define FUSE_ROOT_ID 1
extern "C" {
// Empty structs have different sizes in C and C++, hence the dummy int
struct fuse_chan { int dummy; };
// Defined in t_fuse_evict.cc
extern unsigned fuse_lowlevel_notify_inval_inode_cnt;
static int __attribute__((used)) fuse_lowlevel_notify_inval_inode(
  void *, fuse_ino_t, off_t, off_t)  // NOLINT (ulong from fuse)
{
  fuse_lowlevel_notify_inval_inode_cnt++;
  return -1;
}
}
#else
#define FUSE_USE_VERSION 26
#include <fuse/fuse_lowlevel.h>
#if(FUSE_VERSION < 28)
#include <cstdlib>
extern "C" {
static int __attribute__((used)) fuse_lowlevel_notify_inval_entry(
  void *, unsigned long, const char *, size_t)  // NOLINT
{
  abort();
}
}
#endif
#endif

#endif  // CVMFS_DUPLEX_FUSE_H_
