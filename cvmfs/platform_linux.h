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
#include <sys/types.h>
#include <sys/prctl.h>
#include <attr/xattr.h>
#include <signal.h>
#include <limits.h>
#include <unistd.h>

#include <cassert>

#include <cstring>
#include <string>
#include <cstdlib>

#include "smalloc.h"

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
 * Grants a PID capabilites for ptrace() usage
 *
 * @param PID  the PID of the process to be granted ptrace()-access
 *             (may be ignored)
 * @return     true when successful
 */
inline bool platform_allow_ptrace(const pid_t pid) {
#ifdef PR_SET_PTRACER
  // On Ubuntu, yama prevents all processes from ptracing other processes, even
  // when they are owned by the same user. Therefore the watchdog would not be
  // able to create a stacktrace, without this extra permission:
  const int retval = prctl(PR_SET_PTRACER, pid, 0, 0, 0);
  return (retval == 0);
#else
  // On other platforms this is currently a no-op
  return true;
#endif
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

inline bool platform_getxattr(const std::string &path, const std::string &name,
                              std::string *value)
{
  int size = 0;
  void *buffer = NULL;
  int retval;
  retval = getxattr(path.c_str(), name.c_str(), buffer, size);
  if (retval > 1) {
    size = retval;
    buffer = smalloc(size);
    retval = getxattr(path.c_str(), name.c_str(), buffer, size);
  }
  if ((retval < 0) || (retval > size)) {
    free(buffer);
    return false;
  }
  value->assign(static_cast<const char *>(buffer), size);
  free(buffer);
  return true;
}

inline void platform_disable_kcache(int filedes) {
  posix_fadvise(filedes, 0, 0, POSIX_FADV_RANDOM | POSIX_FADV_NOREUSE);
}

inline int platform_readahead(int filedes) {
  return readahead(filedes, 0, static_cast<size_t>(-1));
}


inline std::string platform_libname(const std::string &base_name) {
  return "lib" + base_name + ".so";
}


inline const char* platform_getexepath() {
  static char buf[PATH_MAX] = {0};
  if (strlen(buf) == 0) {
    int ret = readlink("/proc/self/exe", buf, PATH_MAX);
    if (ret > 0 && ret < (int)PATH_MAX) {
       buf[ret] = 0;
    }
  }
  return buf;
}

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#endif  // CVMFS_PLATFORM_LINUX_H_
