#ifndef COMPAT_MACOSX_H
#define COMPAT_MACOSX_H

#include <string.h>
#include <stdlib.h>

/* -------------------------------------------- 
 *
 *  socket stuff
 *
 * -------------------------------------------- */

/** this is just a quick workaround atm.
 *  MSG_NOSIGNAL prevents send() from sending SIGPIPE and EPIPE is return instead,
 *  at least on systems which support this.
 *  MSG_NOSIGNAL is not POSIX compliant, SO_NOSIGPIPE is the Mac OS X equivalent
 */
#define MSG_NOSIGNAL SO_NOSIGPIPE

/* -------------------------------------------- 
 *
 *  spinlocks
 *
 * -------------------------------------------- */

#include <libkern/OSAtomic.h>
typedef OSSpinLock PortableSpinlock;
inline int portableSpinlockInit(PortableSpinlock *lock, int pshared) { *lock = 0; return 0; }
inline int portableSpinlockDestroy(PortableSpinlock *lock) { return 0; }
inline int portableSpinlockTrylock(PortableSpinlock *lock) { return (OSSpinLockTry(lock)) ? 0 : -1; }

/* -------------------------------------------- 
 *
 *  directory and file handling
 *
 * -------------------------------------------- */

#include <dirent.h>
typedef struct dirent PortableDirent;
inline PortableDirent *portableReaddir(DIR *dirp) { return readdir(dirp); }

#include <sys/stat.h>
typedef struct stat PortableStat64;
inline int portableFileStat64(const char *path, PortableStat64 *buf) { return stat(path, buf); }
inline int portableLinkStat64(const char *path, PortableStat64 *buf) { return lstat(path, buf); }
inline int portableFileDescriptorStat64(int filedes, PortableStat64 *buf) { return fstat(filedes, buf); }

/* -------------------------------------------- 
 *
 *  string handling
 *
 * -------------------------------------------- */
#define strdupa(s) strcpy((char *)alloca(strlen((s)) + 1), (s))

#endif