#ifndef CVMFS_COMPAT_LINUX_H_
#define CVMFS_COMPAT_LINUX_H_

#include <pthread.h>
typedef pthread_spinlock_t PortableSpinlock;
inline int portableSpinlockInit(PortableSpinlock *lock, int pshared) { return pthread_spin_init(lock, pshared); }
inline int portableSpinlockDestroy(PortableSpinlock *lock) { return pthread_spin_destroy(lock); }
inline int portableSpinlockTrylock(PortableSpinlock *lock) { return pthread_spin_trylock(lock); }

#include <dirent.h>
typedef struct dirent64 PortableDirent;
inline PortableDirent *portableReaddir(DIR *dirp) { return readdir64(dirp); }

#include <sys/stat.h>
typedef struct stat64 PortableStat64;
inline int portableFileStat64(const char *path, PortableStat64 *buf) { return stat64(path, buf); }
inline int portableLinkStat64(const char *path, PortableStat64 *buf) { return lstat64(path, buf); }
inline int portableFileDescriptorStat64(int filedes, PortableStat64 *buf) { return fstat64(filedes, buf); }

#endif  // CVMFS_COMPAT_LINUX_H_
