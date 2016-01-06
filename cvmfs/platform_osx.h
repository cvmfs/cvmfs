/**
 * This file is part of the CernVM File System.
 *
 * Mac OS X specific system/library calls.
 */

#ifndef CVMFS_PLATFORM_OSX_H_
#define CVMFS_PLATFORM_OSX_H_

#include <alloca.h>
#include <dirent.h>
#include <fcntl.h>
#include <libkern/OSAtomic.h>
#include <mach/mach.h>
#include <mach/mach_time.h>
#include <mach-o/dyld.h>
#include <signal.h>
#include <sys/mount.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/ucred.h>
#include <sys/xattr.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <string>
#include <vector>

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


inline std::vector<std::string> platform_mountlist() {
  std::vector<std::string> result;
  struct statfs *mntbufp;
  int num_elems = getmntinfo(&mntbufp, MNT_NOWAIT);  // modifies static memory
  for (int i = 0; i < num_elems; ++i) {
    result.push_back(mntbufp[i].f_mntonname);
  }
  return result;
}


inline bool platform_umount(const char *mountpoint, const bool lazy) {
  const int flags = lazy ? MNT_FORCE : 0;
  int retval = unmount(mountpoint, flags);
  return retval == 0;
}


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
inline thread_port_t platform_gettid() {
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
 * See platform_linux.h
 */
inline bool platform_allow_ptrace(const pid_t pid) {
  // No-op on Mac OS X
  return true;
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
  if (retval > 0) {
    value->assign(static_cast<const char *>(buffer), size);
    free(buffer);
  } else {
    value->assign("");
  }
  return true;
}

inline bool platform_setxattr(
  const std::string &path,
  const std::string &name,
  const std::string &value)
{
  int retval = setxattr(
    path.c_str(), name.c_str(), value.c_str(), value.size(), 0, 0);
  return retval == 0;
}


inline ssize_t platform_lgetxattr(
  const char *path,
  const char *name,
  void *value,
  size_t size
) {
  return getxattr(path, name, value, size, 0 /* position */, XATTR_NOFOLLOW);
}


inline ssize_t platform_llistxattr(const char *path, char *list, size_t size) {
  return listxattr(path, list, size, XATTR_NOFOLLOW);
}


inline void platform_disable_kcache(int filedes) {
  fcntl(filedes, F_RDAHEAD, 0);
  fcntl(filedes, F_NOCACHE, 1);
}

inline int platform_readahead(int filedes) {
  // TODO(jblomer): is there a readahead equivalent?
  return 0;
}

inline bool read_line(FILE *f, std::string *line) {
  char   *buffer_line = NULL;
  size_t  buffer_size = 0;
  const int res = getline(&buffer_line, &buffer_size, f);
  if (res < 0) {
    free(buffer_line);
    return false;
  }

  line->clear();
  line->assign(buffer_line);
  free(buffer_line);
  return true;
}

inline void platform_get_os_version(int32_t *major,
                                    int32_t *minor,
                                    int32_t *patch) {
  const std::string plist = "/System/Library/CoreServices/SystemVersion.plist";
  const std::string plist_key = "ProductVersion";

  FILE *plist_file = fopen(plist.c_str(), "r");
  assert(plist_file != NULL && "couldn't open SystemVersion.plist");

  std::string line;
  bool found_key = false;
  while (read_line(plist_file, &line) && !found_key) {
    if (line.find(plist_key) != std::string::npos) {
      found_key = true;
      break;
    }
  }
  assert(found_key && "didn't find key in SystemVersion.plist");

  const std::string start_tag = "<string>";
  const std::string end_tag   = "</string>";
  size_t start, end;
  bool found_value = false;
  while (read_line(plist_file, &line) && !found_value) {
    start = line.find(start_tag);
    end   = line.find(end_tag);
    if (start != std::string::npos && end != std::string::npos) {
      found_value = true;
      break;
    }
  }
  assert(found_value && "didn't find value in SystemVersion.plist");
  fclose(plist_file);

  start = start + start_tag.length();
  const std::string version = line.substr(start, end - start);
  const int matches = sscanf(version.c_str(), "%u.%u.%u", major, minor, patch);
  assert(matches == 3 && "failed to read OS X version string");
}


inline uint64_t platform_monotonic_time() {
  uint64_t val_abs = mach_absolute_time();
  // Doing the conversion every time is slow but thread-safe
  mach_timebase_info_data_t info;
  mach_timebase_info(&info);
  uint64_t val_ns = val_abs * (info.numer / info.denom);
  return val_ns * 1e-9;
}


/**
 * strdupa does not exist on OSX
 */
#define strdupa(s) strcpy(/* NOLINT(runtime/printf) */\
  reinterpret_cast<char *>(alloca(strlen((s)) + 1)), (s))


inline std::string platform_libname(const std::string &base_name) {
  return "lib" + base_name + ".dylib";
}

inline const char* platform_getexepath() {
  static const char* path = _dyld_get_image_name(0);
  return path;
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_PLATFORM_OSX_H_
