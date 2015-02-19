/**
 * This file is part of the CernVM File System.
 *
 * An optimized virtual file system layer for the client only.  It expects to
 * operate on immutable, valid SQlite files.  Hence it can do a few
 * optimiziations.  Most notably it doesn't need to know about the path of
 * the SQlite file once opened.  It works purely on the file descriptor.
 */

#include "cvmfs_config.h"
#include "sqlitevfs.h"

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cassert>
#include <climits>
#include <cstring>
#include <ctime>

#include "duplex_sqlite3.h"
#include "platform.h"
#include "smalloc.h"
#include "statistics.h"

using namespace std;  // NOLINT

namespace sqlite {

namespace {

const string kVfsName = "cvmfs-readonly";

/**
 * The private user data attached to the sqlite_vfs object.
 */
struct VfsRdOnly {
  VfsRdOnly()
    : n_access(NULL)
    , no_open(NULL)
    , n_rand(NULL)
    , sz_rand(NULL)
    , n_read(NULL)
    , sz_read(NULL)
    , n_sleep(NULL)
    , sz_sleep(NULL)
    , n_time(NULL)
  { }
  perf::Counter *n_access;
  perf::Counter *no_open;
  perf::Counter *n_rand;
  perf::Counter *sz_rand;
  perf::Counter *n_read;
  perf::Counter *sz_read;
  perf::Counter *n_sleep;
  perf::Counter *sz_sleep;
  perf::Counter *n_time;
};

/**
 * This is passed to all operations once a file is opened.
 */
struct VfsRdOnlyFile {
  sqlite3_file base;  // Base class. Must be first.
  VfsRdOnly *vfs_rdonly;
  int fd;
  uint64_t size;
};

}  // anonymous namespace


static int VfsRdOnlyClose(sqlite3_file *pFile) {
  VfsRdOnlyFile *p = reinterpret_cast<VfsRdOnlyFile *>(pFile);
  int retval = close(p->fd);
  if (retval == 0) {
    perf::Dec(p->vfs_rdonly->no_open);
    return SQLITE_OK;
  }
  return SQLITE_IOERR_CLOSE;
}


/**
 * On a short read, the remaining bytes must be zero'ed.
 * TODO(jblomer): the reads seem to be rather small.  Investigate buffered read.
 */
static int VfsRdOnlyRead(
  sqlite3_file *pFile,
  void *zBuf,
  int iAmt,
  sqlite_int64 iOfst
) {
  VfsRdOnlyFile *p = reinterpret_cast<VfsRdOnlyFile *>(pFile);
  ssize_t got = pread(p->fd, zBuf, iAmt, iOfst);
  perf::Inc(p->vfs_rdonly->n_read);
  if (got == iAmt) {
    perf::Xadd(p->vfs_rdonly->sz_read, iAmt);
    return SQLITE_OK;
  } else if (got < 0) {
    return SQLITE_IOERR_READ;
  } else {
    perf::Xadd(p->vfs_rdonly->sz_read, got);
    memset(reinterpret_cast<char *>(zBuf) + got, 0, iAmt - got);
    return SQLITE_IOERR_SHORT_READ;
  }
}


static int VfsRdOnlyWrite(
  sqlite3_file *pFile __attribute__((unused)),
  const void *zBuf __attribute__((unused)),
  int iAmt __attribute__((unused)),
  sqlite_int64 iOfst __attribute__((unused))
) {
   return SQLITE_READONLY;
}


static int VfsRdOnlyTruncate(
  sqlite3_file *pFile __attribute__((unused)),
  sqlite_int64 size __attribute__((unused)))
{
  return SQLITE_READONLY;
}


static int VfsRdOnlySync(
  sqlite3_file *pFile __attribute__((unused)),
  int flags __attribute__((unused)))
{
  return SQLITE_OK;
}


static int VfsRdOnlyFileSize(sqlite3_file *pFile, sqlite_int64 *pSize) {
  VfsRdOnlyFile *p = reinterpret_cast<VfsRdOnlyFile *>(pFile);
  *pSize = p->size;
  return SQLITE_OK;
}


static int VfsRdOnlyLock (
  sqlite3_file *p __attribute__((unused)),
  int level __attribute__((unused))
) {
  return SQLITE_OK;
}


static int VfsRdOnlyUnlock (
  sqlite3_file *p __attribute__((unused)),
  int level __attribute__((unused))
) {
  return SQLITE_OK;
}


static int VfsRdOnlyCheckReservedLock(
  sqlite3_file *p __attribute__((unused)),
  int *pResOut
) {
  *pResOut = 0;
  return SQLITE_OK;
}


/**
 *  No xFileControl() verbs are implemented by this VFS.
 */
static int VfsRdOnlyFileControl(
  sqlite3_file *p __attribute__((unused)),
  int op __attribute__((unused)),
  void *pArg __attribute__((unused))
) {
  return SQLITE_NOTFOUND;
}


/**
 * A good unit of bytes to read at once.  But probably only used for writes.
 */
static int VfsRdOnlySectorSize(sqlite3_file *p __attribute__ ((unused))) {
  return 4096;
}


/**
 * Only relevant for writing.
 */
static int VfsRdOnlyDeviceCharacteristics(
  sqlite3_file *p __attribute__ ((unused)))
{
  return 0;
}


/**
 * Supports only read-only opens.
 */
static int VfsRdOnlyOpen(
  sqlite3_vfs *vfs,
  const char *zName,
  sqlite3_file *pFile,
  int flags,
  int *pOutFlags)
{
  static const sqlite3_io_methods io_methods = {
    1, // iVersion
    VfsRdOnlyClose,
    VfsRdOnlyRead,
    VfsRdOnlyWrite,
    VfsRdOnlyTruncate,
    VfsRdOnlySync,
    VfsRdOnlyFileSize,
    VfsRdOnlyLock,
    VfsRdOnlyUnlock,
    VfsRdOnlyCheckReservedLock,
    VfsRdOnlyFileControl,
    VfsRdOnlySectorSize,
    VfsRdOnlyDeviceCharacteristics
  };

  VfsRdOnlyFile *p = reinterpret_cast<VfsRdOnlyFile *>(pFile);
  // Prevent xClose from being called in case of errors
  p->base.pMethods = NULL;

  if (flags & SQLITE_OPEN_READWRITE)
    return SQLITE_IOERR;
  if (flags & SQLITE_OPEN_DELETEONCLOSE)
    return SQLITE_IOERR;
  if (flags & SQLITE_OPEN_EXCLUSIVE)
    return SQLITE_IOERR;

  p->fd = open(zName, O_RDONLY);
  if (p->fd < 0)
    return SQLITE_IOERR;
  platform_stat64 info;
  int retval = platform_fstat(p->fd, &info);
  if (retval != 0)
    return SQLITE_IOERR_FSTAT;
  p->size = info.st_size;
  if (pOutFlags)
    *pOutFlags = flags;
  p->vfs_rdonly = reinterpret_cast<VfsRdOnly *>(vfs->pAppData);
  p->base.pMethods = &io_methods;
  perf::Inc(p->vfs_rdonly->no_open);
  return SQLITE_OK;
}


static int VfsRdOnlyDelete(
  sqlite3_vfs* __attribute__((unused)),
  const char *zName __attribute__((unused)),
  int syncDir __attribute__((unused)))
{
  return SQLITE_IOERR_DELETE;
}


/**
 * Cvmfs r/o file catalogs cannot have a write-ahead log or a journal.
 */
static int VfsRdOnlyAccess(
  sqlite3_vfs *vfs,
  const char *zPath,
  int flags,
  int *pResOut)
{
  if (flags == SQLITE_ACCESS_READWRITE) {
    *pResOut = 0;
    return SQLITE_OK;
  }
  if (HasSuffix(zPath, "-wal", false) || HasSuffix(zPath, "-journal", false)) {
    *pResOut = 0;
    return SQLITE_OK;
  }

  int amode = 0;
  switch( flags ){
    case SQLITE_ACCESS_EXISTS:
      amode = F_OK;
      break;
    case SQLITE_ACCESS_READ:
      amode = R_OK;
      break;
    default:
      assert(false);
  }
  *pResOut = (access(zPath, amode) == 0);
  perf::Inc(reinterpret_cast<VfsRdOnly *>(vfs->pAppData)->n_access);
  return SQLITE_OK;
}


/**
 * Since the path is never stored, there is no need to produce a full path.
 */
int VfsRdOnlyFullPathname(
  sqlite3_vfs *vfs __attribute__((unused)),
  const char *zPath,
  int nOut,
  char *zOut)
{
  zOut[nOut-1] = '\0';
  sqlite3_snprintf(nOut, zOut, "%s", zPath);
  return SQLITE_OK;
}


/**
 * Taken from unixRandomness
 */
static int VfsRdOnlyRandomness(
  sqlite3_vfs *vfs,
  int nBuf,
  char *zBuf)
{
  assert((size_t)nBuf >= (sizeof(time_t) + sizeof(int)));
  perf::Inc(reinterpret_cast<VfsRdOnly *>(vfs->pAppData)->n_rand);
  memset(zBuf, 0, nBuf);
  pid_t randomnessPid = getpid();
  int fd, got;
  fd = open("/dev/urandom", O_RDONLY, 0);
  if (fd < 0) {
    time_t t;
    time(&t);
    memcpy(zBuf, &t, sizeof(t));
    memcpy(&zBuf[sizeof(t)], &randomnessPid, sizeof(randomnessPid));
    assert(sizeof(t) + sizeof(randomnessPid) <= (size_t)nBuf);
    nBuf = sizeof(t) + sizeof(randomnessPid);
  } else {
    do {
      got = read(fd, zBuf, nBuf);
    } while (got < 0 && errno == EINTR);
    close(fd);
  }
  perf::Xadd(reinterpret_cast<VfsRdOnly *>(vfs->pAppData)->sz_rand, nBuf);
  return nBuf;
}


/**
 * Like SafeSleepMs, avoid conflict with the ALARM signal.
 */
static int VfsRdOnlySleep(
  sqlite3_vfs *vfs,
  int microseconds)
{
  struct timeval wait_for;
  wait_for.tv_sec = microseconds / (1000*1000);
  wait_for.tv_usec = microseconds % (1000 * 1000);
  select(0, NULL, NULL, NULL, &wait_for);
  perf::Inc(reinterpret_cast<VfsRdOnly *>(vfs->pAppData)->n_sleep);
  perf::Xadd(reinterpret_cast<VfsRdOnly *>(vfs->pAppData)->sz_sleep,
             microseconds);
  return microseconds;
}


/**
 * Taken from unixCurrentTimeInt64()
 */
static int VfsRdOnlyCurrentTimeInt64(
  sqlite3_vfs *vfs,
  sqlite3_int64 *piNow)
{
  static const sqlite3_int64 unixEpoch = 24405875*(sqlite3_int64)8640000;
  int rc = SQLITE_OK;
  struct timeval sNow;
  if (gettimeofday(&sNow, 0) == 0) {
    *piNow = unixEpoch + 1000*(sqlite3_int64)sNow.tv_sec + sNow.tv_usec/1000;
    perf::Inc(reinterpret_cast<VfsRdOnly *>(vfs->pAppData)->n_time);
  } else {
    rc = SQLITE_ERROR;
  }
  return rc;
}


/**
 * Taken from unixCurrentTime
 */
static int VfsRdOnlyCurrentTime(
  sqlite3_vfs *vfs,
  double *prNow)
{
  sqlite3_int64 i = 0;
  int rc = VfsRdOnlyCurrentTimeInt64(vfs, &i);
  *prNow = i/86400000.0;
  return rc;
}


/**
 * So far unused by sqlite.
 */
static int VfsRdOnlyGetLastError(
  sqlite3_vfs *vfs __attribute__((unused)),
  int not_used1 __attribute__((unused)),
  char *not_used2 __attribute__((unused)))
{
  return 0;
}


/**
 * Can only be registered once.
 */
bool RegisterVfsRdOnly(
  perf::Statistics *statistics,
  const VfsOptions options)
{
  sqlite3_vfs *vfs = reinterpret_cast<sqlite3_vfs *>(
    smalloc(sizeof(sqlite3_vfs)));
  memset(vfs, 0, sizeof(sqlite3_vfs));
  VfsRdOnly *vfs_rdonly = new VfsRdOnly();

  vfs->iVersion = 2;
  vfs->szOsFile = sizeof(VfsRdOnlyFile);
  vfs->mxPathname = PATH_MAX;
  vfs->zName = kVfsName.c_str();
  vfs->pAppData = vfs_rdonly;
  vfs->xOpen = VfsRdOnlyOpen;
  vfs->xDelete = VfsRdOnlyDelete;
  vfs->xAccess = VfsRdOnlyAccess;
  vfs->xFullPathname = VfsRdOnlyFullPathname;
  vfs->xDlOpen = NULL;
  vfs->xDlError = NULL;
  vfs->xDlSym = NULL;
  vfs->xDlClose = NULL;
  vfs->xRandomness = VfsRdOnlyRandomness;
  vfs->xSleep = VfsRdOnlySleep;
  vfs->xCurrentTime = VfsRdOnlyCurrentTime;
  vfs->xGetLastError = VfsRdOnlyGetLastError;
  vfs->xCurrentTimeInt64 = VfsRdOnlyCurrentTimeInt64;
  assert(vfs->zName);

  int retval = sqlite3_vfs_register(vfs, options == kVfsOptDefault);
  if (retval != SQLITE_OK) {
    free(const_cast<char *>(vfs->zName));
    delete vfs_rdonly;
    free(vfs);
    return false;
  }

  vfs_rdonly->n_access =
    statistics->Register("sqlite.n_access", "overall number of access() calls");
  assert(vfs_rdonly->n_access);
  vfs_rdonly->no_open =
    statistics->Register("sqlite.no_open", "currently open sqlite files");
  assert(vfs_rdonly->no_open);
  vfs_rdonly->n_rand =
    statistics->Register("sqlite.n_rand", "overall number of random() calls");
  assert(vfs_rdonly->n_rand);
  vfs_rdonly->sz_rand =
    statistics->Register("sqlite.sz_rand", "overall number of random bytes");
  assert(vfs_rdonly->sz_rand);
  vfs_rdonly->n_read =
    statistics->Register("sqlite.n_read", "overall number of read() calls");
  assert(vfs_rdonly->n_read);
  vfs_rdonly->sz_read =
    statistics->Register("sqlite.sz_read", "overall bytes read()");
  assert(vfs_rdonly->sz_read);
  vfs_rdonly->n_sleep =
    statistics->Register("sqlite.n_sleep", "overall number of sleep() calls");
  assert(vfs_rdonly->n_sleep);
  vfs_rdonly->sz_sleep =
    statistics->Register("sqlite.sz_sleep", "overall microseconds slept");
  assert(vfs_rdonly->sz_sleep);
  vfs_rdonly->n_time =
    statistics->Register("sqlite.n_time", "overall number of time() calls");
  assert(vfs_rdonly->n_time);

  return true;
}


/**
 * If the file system was the default VFS, another default VFS is selected by
 * SQlite randomly.
 */
bool UnregisterVfsRdOnly() {
  sqlite3_vfs *vfs = sqlite3_vfs_find(kVfsName.c_str());
  if (vfs == NULL)
    return false;
  int retval = sqlite3_vfs_unregister(vfs);
  if (retval != SQLITE_OK)
    return false;

  delete reinterpret_cast<VfsRdOnly *>(vfs->pAppData);
  free(vfs);
  return true;
}

}  // namespace sqlite
