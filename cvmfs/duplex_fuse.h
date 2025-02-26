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
    enum fuse_expire_flags {
        FUSE_LL_EXPIRE_ONLY     = (1 << 0),
    };


    // Defined in t_fuse_evict.cc
    extern unsigned fuse_lowlevel_notify_inval_inode_cnt;
    extern unsigned fuse_lowlevel_notify_inval_entry_cnt;
    static int __attribute__((used)) fuse_lowlevel_notify_inval_inode(
      void *, unsigned /*fuse_ino_t*/, off_t, off_t)  // NOLINT (ulong from fuse)
    {
      fuse_lowlevel_notify_inval_inode_cnt++;
      return -1;
    }
    static int __attribute__((used)) fuse_lowlevel_notify_inval_entry(
      struct fuse_chan *, fuse_ino_t, const char *, size_t)  // NOLINT
    {
      fuse_lowlevel_notify_inval_entry_cnt++;
      return -1;
    }
  }
  #define CVMFS_USE_LIBFUSE 2
#else  // CVMFS_LIBCVMFS
  #ifndef CVMFS_USE_LIBFUSE
    #error "Build system error: CVMFS_USE_LIBFUSE unset"
  #endif

  #if CVMFS_USE_LIBFUSE == 2
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
    // CVMFS_USE_LIBFUSE == 3
#ifdef CVMFS_ENABLE_FUSE3_LOOP_CONFIG
    #define FUSE_USE_VERSION 312
#else
    #define FUSE_USE_VERSION 31
#endif
    #include <fuse3/fuse.h>
    #include <fuse3/fuse_lowlevel.h>
    #include <fuse3/fuse_opt.h>
  #endif
#endif

#endif  // CVMFS_DUPLEX_FUSE_H_
