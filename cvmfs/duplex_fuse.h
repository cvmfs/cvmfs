/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_DUPLEX_FUSE_H_
#define CVMFS_DUPLEX_FUSE_H_

#ifdef CVMFS_LIBCVMFS
  // Unit tests
  #include <sys/types.h>
  #define FUSE_VERSION 29
  #define FUSE_ROOT_ID 1
  extern "C" {
    typedef unsigned long fuse_ino_t;  // NOLINT
    // Empty structs have different sizes in C and C++, hence the dummy int
    struct fuse_chan { int dummy; };
    struct fuse_lowlevel_ops { int dummy; };  // for loader.h
    // Defined in t_fuse_evict.cc
    extern unsigned fuse_lowlevel_notify_inval_inode_cnt;
    static int __attribute__((used)) fuse_lowlevel_notify_inval_inode(
      void *, unsigned /*fuse_ino_t*/, off_t, off_t)  // NOLINT (ulong from fuse)
    {
      fuse_lowlevel_notify_inval_inode_cnt++;
      return -1;
    }
  }
#else  // CVMFS_LIBCVMFS
  #ifndef CVMFS_FUSE_LIBVERSION
    #ifdef HAS_FUSE3
      #define CVMFS_FUSE_LIBVERSION 3
    #else
      #define CVMFS_FUSE_LIBVERSION 2
    #endif
  #endif

  #if CVMFS_FUSE_LIBVERSION == 2
    #define FUSE_USE_VERSION 26
    #include <fuse/fuse_lowlevel.h>
    #include <fuse/fuse_opt.h>
    #if(FUSE_VERSION < 28)
      #include <cstdlib>
      extern "C" {
        static int __attribute__((used)) fuse_lowlevel_notify_inval_entry(
          void *, unsigned long, const char *, size_t)  // NOLINT
        {
          abort();
        }
        static int __attribute__((used)) fuse_lowlevel_notify_inval_inode(
          void *, fuse_ino_t, off_t, off_t)  // NOLINT
        {
          abort();
        }
      }
    #endif  // FUSE_VERSION < 28
  #else
    // CVMFS_FUSE_LIBVERSION == 3
    #define FUSE_USE_VERSION 31
    #include <fuse3/fuse.h>
    #include <fuse3/fuse_lowlevel.h>
    #include <fuse3/fuse_opt.h>
  #endif
#endif

#endif  // CVMFS_DUPLEX_FUSE_H_
