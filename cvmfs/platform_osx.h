/**
 * This file is part of the CernVM File System.
 *
 * Mac OS X specific system/library calls.
 */

#ifndef CVMFS_PLATFORM_OSX_H_
#define CVMFS_PLATFORM_OSX_H_

#include <libkern/OSAtomic.h>
#include <mach/mach.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <alloca.h>
#include <signal.h>
#include <mach-o/dyld.h>

#include <cstring>
#include <cassert>
#include <cstdlib>

#include <string>

#include "smalloc.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * UNIX domain sockets:
 * MSG_NOSIGNAL prevents send() from sending SIGPIPE
 * and EPIPE is return instead, where supported.
 * MSG_NOSIGNAL is Linux specific, SO_NOSIGPIPE is the Mac OS X equivalent.
 */
#define MSG_NOSIGNAL SO_NOSIGPIPE

/**
 * HOST_NAME_MAX does on exist on OS X
 */
#define HOST_NAME_MAX _POSIX_HOST_NAME_MAX


/**
 * Spinlocks on OS X are not in pthread but in OS X specific APIs.
 */
typedef OSSpinLock platform_spinlock;

inline int platform_spinlock_init(platform_spinlock *lock, int pshared) {
  *lock = 0;
  return 0;
}

inline int platform_spinlock_destroy(platform_spinlock *lock) { return 0; }

inline int platform_spinlock_trylock(platform_spinlock *lock) {
  return (OSSpinLockTry(lock)) ? 0 : -1;
}


/**
 * pthread_self() is not necessarily an unsigned long.
 */
inline unsigned long platform_gettid() {
  return mach_thread_self();
}


inline int platform_sigwait(const int signum) {
  sigset_t sigset;
  int retval = sigemptyset(&sigset);
  assert(retval == 0);
  retval = sigaddset(&sigset, signum);
  assert(retval == 0);
  int result;
  retval = sigwait(&sigset, &result);
  assert(retval == 0);
  return result;
}


/**
 * File system functions, Mac OS X has 64bit functions by default.
 */
typedef struct dirent platform_dirent64;

inline platform_dirent64 *platform_readdir(DIR *dirp) { return readdir(dirp); }

typedef struct stat platform_stat64;

inline int platform_stat(const char *path, platform_stat64 *buf) {
  return stat(path, buf);
}

inline int platform_lstat(const char *path, platform_stat64 *buf) {
  return lstat(path, buf);
}

inline int platform_fstat(int filedes, platform_stat64 *buf) {
  return fstat(filedes, buf);
}

inline bool platform_getxattr(const std::string &path, const std::string &name,
                              std::string *value)
{
  int size = 0;
  void *buffer = NULL;
  int retval;
  retval = getxattr(path.c_str(), name.c_str(), buffer, size, 0, 0);
  if (retval >= 1) {
    size = retval;
    buffer = smalloc(size);
    retval = getxattr(path.c_str(), name.c_str(), buffer, size, 0, 0);
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
  fcntl(filedes, F_RDAHEAD, 0);
  fcntl(filedes, F_NOCACHE, 1);
}

inline int platform_readahead(int filedes) {
  // TODO: is there a readahead equivalent?
  return 0;
}

/**
 * strdupa does not exist on OSX
 */
#define strdupa(s) strcpy(reinterpret_cast<char *> \
  (alloca(strlen((s)) + 1)), (s))


inline std::string platform_libname(const std::string &base_name) {
  return "lib" + base_name + ".dylib";
}

inline const char* platform_getexepath() {
  static const char* path = _dyld_get_image_name(0);
  return path;
}

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#endif  // CVMFS_PLATFORM_OSX_H_
