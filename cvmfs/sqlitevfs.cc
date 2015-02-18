/**
 * This file is part of the CernVM File System.
 */

#include "sqlitevfs.h"

#include <dlfcn.h>
#include <fcntl.h>
#include <sys/select.h>
#include <sys/time.h>
#include <unistd.h>

#include <cassert>
#include <climits>
#include <cstring>
#include <ctime>

#include "duplex_sqlite3.h"
#include "smalloc.h"
#include "statistics.h"

using namespace std;  // NOLINT

namespace sqlite {

namespace {

struct VfsRdOnly {
  explicit VfsRdOnly(perf::Statistics *statistics) : statistics(statistics) { }
  perf::Statistics *statistics;
};

}  // anonymous namespace


static int VfsRdOnlyOpen(
  sqlite3_vfs *vfs,
  const char *zName,
  sqlite3_file *file,
  int flags,
  int *pOutFlags)
{
  return 0;
}


static int VfsRdOnlyDelete(sqlite3_vfs*, const char *zName, int syncDir) {
  return 0;
}


static int VfsRdOnlyAccess(
  sqlite3_vfs *vfs,
  const char *zName,
  int flags,
  int *pResOut)
{
  return 0;
}


/**
 * Taken from unixFullPathname
 */
int VfsRdOnlyFullPathname(
  sqlite3_vfs *vfs __attribute__((unused)),
  const char *zPath,
  int nOut,
  char *zOut)
{
  zOut[nOut-1] = '\0';
  if (zPath[0] == '/') {
    sqlite3_snprintf(nOut, zOut, "%s", zPath);
  } else {
    int nCwd;
    if (getcwd(zOut, nOut-1) == NULL) {
      return SQLITE_ERROR;
    }
    nCwd = (int)strlen(zOut);
    sqlite3_snprintf(nOut-nCwd, &zOut[nCwd], "/%s", zPath);
  }
  return SQLITE_OK;
}


/**
 * Taken from unixRandomness
 */
static int VfsRdOnlyRandomness(
  sqlite3_vfs *vfs __attribute__((unused)),
  int nBuf,
  char *zBuf)
{
  assert((size_t)nBuf >= (sizeof(time_t) + sizeof(int)));
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
  return nBuf;
}


static int VfsRdOnlySleep(sqlite3_vfs *vfs, int microseconds) {
  struct timeval wait_for;
  wait_for.tv_sec = microseconds / (1000*1000);
  wait_for.tv_usec = microseconds % (1000 * 1000);
  select(0, NULL, NULL, NULL, &wait_for);
  return microseconds;
}


/**
 * Taken from unixCurrentTimeInt64()
 */
static int VfsRdOnlyCurrentTimeInt64(
  sqlite3_vfs *vfs __attribute__((unused)),
  sqlite3_int64 *piNow)
{
  static const sqlite3_int64 unixEpoch = 24405875*(sqlite3_int64)8640000;
  int rc = SQLITE_OK;
  struct timeval sNow;
  if (gettimeofday(&sNow, 0) == 0) {
    *piNow = unixEpoch + 1000*(sqlite3_int64)sNow.tv_sec + sNow.tv_usec/1000;
  } else {
    rc = SQLITE_ERROR;
  }
  return rc;
}


/**
 * Taken from unixCurrentTime
 */
static int VfsRdOnlyCurrentTime(
  sqlite3_vfs *vfs __attribute__((unused)),
  double *prNow)
{
  sqlite3_int64 i = 0;
  int rc = VfsRdOnlyCurrentTimeInt64(0, &i);
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


bool RegisterVfsReadOnly(
  const string &vfs_name,
  perf::Statistics *statistics)
{
  sqlite3_vfs *vfs = reinterpret_cast<sqlite3_vfs *>(
    smalloc(sizeof(sqlite3_vfs)));
  memset(vfs, 0, sizeof(sqlite3_vfs));
  VfsRdOnly *vfs_rdonly = new VfsRdOnly(statistics);

  vfs->iVersion = 2;
  vfs->szOsFile = 0;
  vfs->mxPathname = PATH_MAX;
  vfs->zName = strdup(vfs_name.c_str());
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

  int retval = sqlite3_vfs_register(vfs, 0);
  if (retval != SQLITE_OK) {
    free(const_cast<char *>(vfs->zName));
    delete vfs_rdonly;
    free(vfs);
    return false;
  }
  return true;
}


bool UnregisterVfsRdOnly(const string &vfs_name) {
  sqlite3_vfs *vfs = sqlite3_vfs_find(vfs_name.c_str());
  if (vfs == NULL)
    return false;
  int retval = sqlite3_vfs_unregister(vfs);
  if (retval != SQLITE_OK)
    return false;

  free(const_cast<char *>(vfs->zName));
  delete reinterpret_cast<VfsRdOnly *>(vfs->pAppData);
  free(vfs);
  return true;
}

}  // namespace sqlite
