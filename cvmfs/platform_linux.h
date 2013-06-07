/**
 * This file is part of the CernVM File System.
 *
 * Linux specific system/library calls.
 */

#ifndef CVMFS_PLATFORM_LINUX_H_
#define CVMFS_PLATFORM_LINUX_H_

#include <pthread.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <signal.h>

#include <attr/xattr.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>

#include <string>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * Spinlocks are not necessarily part of pthread on all platforms.
 */
typedef pthread_spinlock_t platform_spinlock;

inline int platform_spinlock_init(platform_spinlock *lock, int pshared) {
  return pthread_spin_init(lock, pshared);
}

inline int platform_spinlock_destroy(platform_spinlock *lock) {
  return pthread_spin_destroy(lock);
}

inline int platform_spinlock_trylock(platform_spinlock *lock) {
  return pthread_spin_trylock(lock);
}


/**
 * pthread_self() is not necessarily an unsigned long.
 */
inline unsigned long platform_gettid() {
  return pthread_self();
}


inline int platform_sigwait(const int signum) {
  sigset_t sigset;
  int retval = sigemptyset(&sigset);
  assert(retval == 0);
  retval = sigaddset(&sigset, signum);
  assert(retval == 0);
  retval = sigwaitinfo(&sigset, NULL);
  return retval;
}


/**
 * File system functions, ensure 64bit versions.
 */
typedef struct dirent64 platform_dirent64;

inline platform_dirent64 *platform_readdir(DIR *dirp) {
  return readdir64(dirp);
}

typedef struct stat64 platform_stat64;

inline int platform_stat(const char *path, platform_stat64 *buf) {
  return stat64(path, buf);
}

inline int platform_lstat(const char *path, platform_stat64 *buf) {
  return lstat64(path, buf);
}

inline int platform_fstat(int filedes, platform_stat64 *buf) {
  return fstat64(filedes, buf);
}

inline void platform_disable_kcache(int filedes) {
  posix_fadvise(filedes, 0, 0, POSIX_FADV_RANDOM | POSIX_FADV_NOREUSE);
}

inline int platform_readahead(int filedes) {
  return readahead(filedes, 0, static_cast<size_t>(-1));
}

inline std::string platform_readlink32(std::string const& path) {
  char buf[32];
  ssize_t len = ::readlink(path.c_str(), buf, sizeof(buf)-1);
  if (len != -1) {
    buf[len] = '\0';
    return std::string(buf);
  } else {
    // error
    return std::string();
  }
}

inline std::string platform_lgetxattr32(std::string const& path, std::string const& name) {
  char buf[32];
  ssize_t len = ::getxattr(path.c_str(), name.c_str(), buf, sizeof(buf)-1);
  if (len != -1) {
    buf[len] = '\0';
    return std::string(buf);
  } else {
    // error
    return std::string();
  }
}

inline std::string platform_libname(const std::string &base_name) {
  return "lib" + base_name + ".so";
}

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#endif  // CVMFS_PLATFORM_LINUX_H_
