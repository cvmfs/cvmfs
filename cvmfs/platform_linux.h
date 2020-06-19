/**
 * This file is part of the CernVM File System.
 *
 * Linux specific system/library calls.
 */

#ifndef CVMFS_PLATFORM_LINUX_H_
#define CVMFS_PLATFORM_LINUX_H_

#include <sys/types.h>  // contains ssize_t needed inside <attr/xattr.h>
#include <sys/xattr.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <mntent.h>
#include <pthread.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <vector>

#ifdef CVMFS_ENABLE_INOTIFY
#include "file_watcher_inotify.h"
#else  // CVMFS_ENABLE_INOTIFY
#include "file_watcher.h"
#endif  // CVMFS_ENABLE_INOTIFY
#include "smalloc.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

#define platform_sighandler_t sighandler_t

inline std::vector<std::string> platform_mountlist() {
  std::vector<std::string> result;
  FILE *fmnt = setmntent("/proc/mounts", "r");
  struct mntent *mntbuf;  // Static buffer managed by libc!
  while ((mntbuf = getmntent(fmnt)) != NULL) {
    result.push_back(mntbuf->mnt_dir);
  }
  endmntent(fmnt);
  return result;
}

// glibc < 2.11
#ifndef MNT_DETACH
#define MNT_DETACH 0x00000002
#endif
inline bool platform_umount(const char *mountpoint, const bool lazy) {
  struct stat64 mtab_info;
  int retval = lstat64(_PATH_MOUNTED, &mtab_info);
  // If /etc/mtab exists and is not a symlink to /proc/mounts
  if ((retval == 0) && S_ISREG(mtab_info.st_mode)) {
    // Lock the modification on /etc/mtab against concurrent
    // crash unmount handlers (removing the lock file would result in a race)
    std::string lockfile = std::string(_PATH_MOUNTED) + ".cvmfslock";
    const int fd_lockfile = open(lockfile.c_str(), O_RDONLY | O_CREAT, 0600);
    if (fd_lockfile < 0) return false;
    int timeout = 10;
    while ((flock(fd_lockfile, LOCK_EX | LOCK_NB) != 0) && (timeout > 0)) {
      if (errno != EWOULDBLOCK) {
        close(fd_lockfile);
        return false;
      }
      struct timeval wait_for;
      wait_for.tv_sec = 1;
      wait_for.tv_usec = 0;
      select(0, NULL, NULL, NULL, &wait_for);
      timeout--;
    }
    if (timeout <= 0) {
      close(fd_lockfile);
      return false;
    }

    // Remove entry from /etc/mtab (create new file without entry)
    std::string mntnew = std::string(_PATH_MOUNTED) + ".cvmfstmp";
    FILE *fmntold = setmntent(_PATH_MOUNTED, "r");
    if (!fmntold) {
      flock(fd_lockfile, LOCK_UN);
      close(fd_lockfile);
      return false;
    }
    FILE *fmntnew = setmntent(mntnew.c_str(), "w+");
    if (!fmntnew && (chmod(mntnew.c_str(), mtab_info.st_mode) != 0) &&
        (chown(mntnew.c_str(), mtab_info.st_uid, mtab_info.st_gid) != 0)) {
      endmntent(fmntold);
      flock(fd_lockfile, LOCK_UN);
      close(fd_lockfile);
      return false;
    }
    struct mntent *mntbuf;  // Static buffer managed by libc!
    while ((mntbuf = getmntent(fmntold)) != NULL) {
      if (strcmp(mntbuf->mnt_dir, mountpoint) != 0) {
        retval = addmntent(fmntnew, mntbuf);
        if (retval != 0) {
          endmntent(fmntold);
          endmntent(fmntnew);
          unlink(mntnew.c_str());
          flock(fd_lockfile, LOCK_UN);
          close(fd_lockfile);
          return false;
        }
      }
    }
    endmntent(fmntold);
    endmntent(fmntnew);
    retval = rename(mntnew.c_str(), _PATH_MOUNTED);
    flock(fd_lockfile, LOCK_UN);
    close(fd_lockfile);
    if (retval != 0) return false;
    // Best effort
    retval = chmod(_PATH_MOUNTED, mtab_info.st_mode);
    retval = chown(_PATH_MOUNTED, mtab_info.st_uid, mtab_info.st_gid);
    // We pickup these values only to silent warnings
  }

  int flags = lazy ? MNT_DETACH : 0;
  retval = umount2(mountpoint, flags);
  return retval == 0;
}

inline bool platform_umount_lazy(const char *mountpoint) {
  int retval = umount2(mountpoint, MNT_DETACH);
  return retval == 0;
}

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

inline void platform_spinlock_unlock(platform_spinlock *lock) {
  pthread_spin_unlock(lock);
}

/**
 * pthread_self() is not necessarily an unsigned long.
 */
inline pthread_t platform_gettid() { return pthread_self(); }

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
  // able to create a stacktrace, without this extra permission.
  const int retval = prctl(PR_SET_PTRACER, pid, 0, 0, 0);
  // On some platforms (e.g. CentOS7), PR_SET_PTRACER is defined but not
  // supported by the kernel.  That's fine and we don't have to care about it
  // when it happens.
  return (retval == 0) || (errno == EINVAL);
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

// TODO(jblomer): the translation from C to C++ should be done elsewhere
inline bool platform_getxattr(const std::string &path, const std::string &name,
                              std::string *value) {
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
  if (retval > 0) {
    value->assign(static_cast<const char *>(buffer), size);
    free(buffer);
  } else {
    value->assign("");
  }
  return true;
}

// TODO(jblomer): the translation from C to C++ should be done elsewhere
inline bool platform_setxattr(const std::string &path, const std::string &name,
                              const std::string &value) {
  int retval =
      setxattr(path.c_str(), name.c_str(), value.c_str(), value.size(), 0);
  return retval == 0;
}

inline bool platform_lsetxattr(const std::string &path, const std::string &name,
                              const std::string &value) {
  int retval =
      lsetxattr(path.c_str(), name.c_str(), value.c_str(), value.size(), 0);
  return retval == 0;
}

inline ssize_t platform_lgetxattr(const char *path, const char *name,
                                  void *value, size_t size) {
  return lgetxattr(path, name, value, size);
}

inline ssize_t platform_llistxattr(const char *path, char *list, size_t size) {
  return llistxattr(path, list, size);
}

inline void platform_disable_kcache(int filedes) {
  (void)posix_fadvise(filedes, 0, 0, POSIX_FADV_RANDOM | POSIX_FADV_NOREUSE);
}

inline int platform_readahead(int filedes) {
  return readahead(filedes, 0, static_cast<size_t>(-1));
}

/**
 * Advises the kernel to evict the given file region from the page cache.
 *
 * Note: Pages containing the data at `offset` and `offset + length` are NOT
 *       evicted by the kernel. This means that a few pages are not purged when
 *       offset and length are not exactly on page boundaries. See below:
 *
 *                offset                                  length
 *                  |                                        |
 *   +---------+----|----+---------+---------+---------+-----|---+---------+
 *   |         |    |    | xxxxxxx | xxxxxxx | xxxxxxx |     |   |         |
 *   |         |    |    | xxxxxxx | xxxxxxx | xxxxxxx |     |   |         |
 *   +---------+----|----+---------+---------+---------+-----|---+---------+
 *   0       4096   |  8192      12288     16384     20480   | 24576     28672
 *
 * git.kernel.org/cgit/linux/kernel/git/stable/linux-stable.git/tree/mm/fadvise.c#n115
 *
 * TODO(rmeusel): figure out a clever way how to align `offset` and `length`
 *
 * @param fd      file descriptor whose page cache should be (partially) evicted
 * @param offset  start offset of the pages to be evicted
 * @param length  number of bytes to be evicted
 */
inline int platform_invalidate_kcache(const int fd, const off_t offset,
                                      const size_t length) {
  return posix_fadvise(fd, offset, length, POSIX_FADV_DONTNEED);
}

inline std::string platform_libname(const std::string &base_name) {
  return "lib" + base_name + ".so";
}

inline std::string platform_getexepath() {
  char buf[PATH_MAX + 1];
  int ret = readlink("/proc/self/exe", buf, PATH_MAX);
  if (ret > 0) {
    buf[ret] = '\0';
    return std::string(buf);
  }
  return "";
}

inline struct timespec _time_with_clock(int clock) {
  struct timespec tp;
  int retval = clock_gettime(clock, &tp);
  assert(retval == 0);
  return tp;
}

inline uint64_t platform_monotonic_time() {
#ifdef CLOCK_MONOTONIC_COARSE
  struct timespec tp = _time_with_clock(CLOCK_MONOTONIC_COARSE);
#else
  struct timespec tp = _time_with_clock(CLOCK_MONOTONIC);
#endif
  return tp.tv_sec + (tp.tv_nsec >= 500000000);
}

inline uint64_t platform_monotonic_time_ns() {
  struct timespec tp = _time_with_clock(CLOCK_MONOTONIC);
  return static_cast<uint64_t>(tp.tv_sec * 1e9 + tp.tv_nsec);
}

inline uint64_t platform_realtime_ns() {
  struct timespec tp = _time_with_clock(CLOCK_REALTIME);
  return static_cast<uint64_t>(tp.tv_sec * 1e9 + tp.tv_nsec);
}

inline uint64_t platform_memsize() {
  return static_cast<uint64_t>(sysconf(_SC_PHYS_PAGES)) *
         static_cast<uint64_t>(sysconf(_SC_PAGE_SIZE));
}

inline file_watcher::FileWatcher* platform_file_watcher() {
#ifdef CVMFS_ENABLE_INOTIFY
  return new file_watcher::FileWatcherInotify();
#else  // CVMFS_ENABLE_INOTIFY
  return NULL;
#endif  // CVMFS_ENABLE_INOTIFY
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_PLATFORM_LINUX_H_
