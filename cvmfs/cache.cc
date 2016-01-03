/**
 * This file is part of the CernVM File System.
 *
 * The cache module maintains the local file cache.  Files are
 * staged into the cache by Fetch().  The cache stores files with a name
 * according to their content hash.
 *
 * The procedure is
 *   -# Look in the catalog for content hash
 *   -# If it is in local cache: return file descriptor
 *   -# Otherwise download, store in cache and return fd
 *
 * Each running CVMFS instance has to have a separate cache directory.
 * The local cache directory (directories 00..ff) can be accessed
 * in parallel to a running CVMFS, i.e. files can be deleted for instance
 * anytime.  However, this will confuse the cache database managed by the lru
 * module.
 *
 * Files are created in txn directory first.  At the very latest
 * point they are renamed into their "real" content hash names atomically by
 * rename().  This concept is taken over from GROW-FS.
 *
 * Identical URLs won't be concurrently downloaded.  The first thread performs
 * the download and informs the other, waiting threads on pipes.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "cache.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#ifndef __APPLE__
#include <sys/statfs.h>
#endif
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <vector>

#include "atomic.h"
#include "compression.h"
#include "cvmfs.h"
#include "directory_entry.h"
#include "download.h"
#include "hash.h"
#include "logging.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "platform.h"
#include "quota.h"
#include "shortstring.h"
#include "signature.h"
#include "smalloc.h"
#include "statistics.h"
#include "util.h"

#ifndef NFS_SUPER_MAGIC
#define NFS_SUPER_MAGIC 0x6969
#endif

using namespace std;  // NOLINT

namespace cache {

uint64_t kBigFile = 25*1024*1024;  // As of 25M, a file is considered "big file"


/**
 * A CallGuard object can be placed at the beginning of a function.  It counts
 * the number of so-annotated functions that are in flight.  The Drainout() call
 * will wait until all functions that have been called so far are finished.
 *
 * The class is used in order to wait for remaining calls when switching into
 * the read-only cache mode.
 */
class CallGuard {
 public:
  CallGuard() {
    int32_t global_drainout = atomic_read32(&global_drainout_);
    drainout_ = (global_drainout != 0);
    if (!drainout_)
      atomic_inc32(&num_inflight_calls_);
  }
  ~CallGuard() {
    if (!drainout_)
      atomic_dec32(&num_inflight_calls_);
  }
  static void Drainout() {
    atomic_cas32(&global_drainout_, 0, 1);
    while (atomic_read32(&num_inflight_calls_) != 0)
      SafeSleepMs(50);
  }
 private:
  bool drainout_;
  static atomic_int32 global_drainout_;
  static atomic_int32 num_inflight_calls_;
};
atomic_int32 CallGuard::num_inflight_calls_ = 0;
atomic_int32 CallGuard::global_drainout_ = 0;


//------------------------------------------------------------------------------

const uint64_t CacheManager::kSizeUnknown = uint64_t(-1);


CacheManager::CacheManager() : quota_mgr_(new NoopQuotaManager()) { }


CacheManager::~CacheManager() {
  delete quota_mgr_;
}


/**
 * Compresses and checksums the file pointed to by fd.  The hash algorithm needs
 * to be set in id.
 */
int CacheManager::ChecksumFd(int fd, shash::Any *id) {
  shash::ContextPtr hash_context(id->algorithm);
  hash_context.buffer = alloca(hash_context.size);
  shash::Init(hash_context);

  z_stream strm;
  zlib::CompressInit(&strm);
  zlib::StreamStates retval;

  unsigned char buf[4096];
  uint64_t pos = 0;
  bool eof;

  do {
    int64_t nbytes = Pread(fd, buf, 4096, pos);
    if (nbytes < 0) {
      zlib::CompressFini(&strm);
      return nbytes;
    }
    pos += nbytes;
    eof = nbytes < 4096;
    retval = zlib::CompressZStream2Null(buf, nbytes, eof, &strm, &hash_context);
    if (retval == zlib::kStreamDataError) {
      zlib::CompressFini(&strm);
      return -EINVAL;
    }
  } while (!eof);

  zlib::CompressFini(&strm);
  if (retval != zlib::kStreamEnd)
    return -EINVAL;
  shash::Final(hash_context, id);
  return 0;
}


/**
 * Commits the memory blob buffer to the given chunk id.  No checking!
 * The hash and the memory blob need to match.
 */
bool CacheManager::CommitFromMem(
  const shash::Any &id,
  const unsigned char *buffer,
  const uint64_t size,
  const string &description)
{
  void *txn = alloca(this->SizeOfTxn());
  int fd = this->StartTxn(id, size, txn);
  if (fd < 0)
    return false;
  this->CtrlTxn(description, kTypeRegular, 0, txn);
  int64_t retval = this->Write(buffer, size, txn);
  if ((retval < 0) || (static_cast<uint64_t>(retval) != size)) {
    this->AbortTxn(txn);
    return false;
  }
  retval = this->CommitTxn(txn);
  return retval == 0;
}


/**
 * Tries to open a file and copies its contents into a newly malloc'd
 * memory area.  User of the function has to free buffer (if successful).
 *
 * @param[in] id content hash of the catalog entry.
 * @param[out] buffer Contents of the file
 * @param[out] size Size of the file
 * \return True if successful, false otherwise.
 */
bool CacheManager::Open2Mem(
  const shash::Any &id,
  unsigned char **buffer,
  uint64_t *size)
{
  *size = 0;
  *buffer = NULL;

  int fd = this->Open(id);
  if (fd < 0)
    return false;

  int64_t s = this->GetSize(fd);
  assert(s >= 0);
  *size = static_cast<uint64_t>(s);

  int64_t retval = 0;
  if (*size > 0) {
    *buffer = static_cast<unsigned char *>(smalloc(*size));
    retval = this->Pread(fd, *buffer, *size, 0);
  } else {
    *buffer = NULL;
  }

  this->Close(fd);
  if ((retval < 0) || (static_cast<uint64_t>(retval) != *size)) {
    free(*buffer);
    *buffer = NULL;
    *size = 0;
    return false;
  }
  return true;
}


/**
 * Uses the regular open and, if the file exists in the cache, pins it.  There
 * is a race condition: the file can be removed between the open and the Pin.
 * This is fixed by the quota manager's unpin method that removes files which
 * do not exist anymore in the cache.  (The quota manager also translates double
 * pins into a no-op, so that the accounting does not get out of sync.)
 */
int CacheManager::OpenPinned(
  const shash::Any &id,
  const string &description,
  bool is_catalog)
{
  int fd = this->Open(id);
  if (fd >= 0) {
    int64_t size = this->GetSize(fd);
    if (size < 0) {
      this->Close(fd);
      return size;
    }
    bool retval =
      quota_mgr_->Pin(id, static_cast<uint64_t>(size), description, is_catalog);
    if (!retval) {
      this->Close(fd);
      return -ENOSPC;
    }
  }
  return fd;
}


//------------------------------------------------------------------------------

const uint64_t PosixCacheManager::kBigFile = 25*1024*1024;  // 25M


int PosixCacheManager::AbortTxn(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  LogCvmfs(kLogCache, kLogDebug, "abort %s", transaction->tmp_path.c_str());
  close(transaction->fd);
  int result = unlink(transaction->tmp_path.c_str());
  transaction->~Transaction();
  atomic_dec32(&no_inflight_txns_);
  if (result == -1)
    return -errno;
  return 0;
}


/**
 * This should only be used to replace the default NoopQuotaManager by a
 * PosixQuotaManager.  The cache manager takes the ownership of the passed
 * quota manager.
 */
bool PosixCacheManager::AcquireQuotaManager(QuotaManager *quota_mgr) {
  if (quota_mgr == NULL)
    return false;
  delete quota_mgr_;
  quota_mgr_ = quota_mgr;
  return true;
}


int PosixCacheManager::Close(int fd) {
  int retval = close(fd);
  if (retval != 0)
    return -errno;
  return 0;
}


int PosixCacheManager::CommitTxn(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  int result;
  LogCvmfs(kLogCache, kLogDebug, "commit %s %s",
           transaction->final_path.c_str(), transaction->tmp_path.c_str());

  result = Flush(transaction);
  close(transaction->fd);
  if (result < 0) {
    unlink(transaction->tmp_path.c_str());
    transaction->~Transaction();
    atomic_dec32(&no_inflight_txns_);
    return result;
  }

  // To support debugging, move files into quarantine on file size mismatch
  if (transaction->size != transaction->expected_size) {
    // Allow size to be zero if alien cache, because hadoop-fuse-dfs returns
    // size zero for a while
    if ( (transaction->expected_size != kSizeUnknown) &&
         (reports_correct_filesize_ || (transaction->size != 0)) )
    {
      LogCvmfs(kLogCache, kLogDebug | kLogSyslogErr,
               "size check failure for %s, expected %lu, got %lu",
               transaction->id.ToString().c_str(),
               transaction->expected_size, transaction->size);
      CopyPath2Path(transaction->tmp_path,
                    cache_path_ + "/quarantaine/" + transaction->id.ToString());
      unlink(transaction->tmp_path.c_str());
      transaction->~Transaction();
      atomic_dec32(&no_inflight_txns_);
      return -EIO;
    }
  }

  if ((transaction->type == kTypePinned) || (transaction->type == kTypeCatalog))
  {
    bool retval = quota_mgr_->Pin(
      transaction->id, transaction->size, transaction->description,
      (transaction->type == kTypeCatalog));
    if (!retval) {
      LogCvmfs(kLogCache, kLogDebug, "commit failed: cannot pin %s",
               transaction->id.ToString().c_str());
      unlink(transaction->tmp_path.c_str());
      transaction->~Transaction();
      atomic_dec32(&no_inflight_txns_);
      return -ENOSPC;
    }
  }

  // Move the temporary file into its final location
  if (alien_cache_) {
    int retval = chmod(transaction->tmp_path.c_str(), 0660);
    assert(retval == 0);
  }
  result =
    Rename(transaction->tmp_path.c_str(), transaction->final_path.c_str());
  if (result < 0) {
    LogCvmfs(kLogCache, kLogDebug, "commit failed: %s", strerror(errno));
    unlink(transaction->tmp_path.c_str());
    if ((transaction->type == kTypePinned) ||
        (transaction->type == kTypeCatalog))
    {
      quota_mgr_->Remove(transaction->id);
    }
  } else {
    // Success, inform quota manager
    if (transaction->type == kTypeVolatile) {
      quota_mgr_->InsertVolatile(transaction->id, transaction->size,
                                 transaction->description);
    } else if (transaction->type == kTypeRegular) {
      quota_mgr_->Insert(transaction->id, transaction->size,
                         transaction->description);
    }
  }
  transaction->~Transaction();
  atomic_dec32(&no_inflight_txns_);
  return result;
}


PosixCacheManager *PosixCacheManager::Create(
  const string &cache_path,
  const bool alien_cache,
  const bool workaround_rename)
{
  UniquePtr<PosixCacheManager> cache_manager(
    new PosixCacheManager(cache_path, alien_cache));
  assert(cache_manager.IsValid());

  cache_manager->workaround_rename_ = workaround_rename;
  if (cache_manager->alien_cache_) {
    if (!MakeCacheDirectories(cache_path, 0770)) {
      return NULL;
    }
    LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
             "Cache directory structure created.");
    struct statfs cache_buf;
    if ((statfs(cache_path.c_str(), &cache_buf) == 0) &&
        (cache_buf.f_type == NFS_SUPER_MAGIC))
    {
      cache_manager->workaround_rename_ = true;
      LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
             "Alien cache is on NFS.");
    }
  } else {
    if (!MakeCacheDirectories(cache_path, 0700))
      return NULL;
  }

  if (FileExists(cache_path + "/cvmfscatalog.cache")) {
    LogCvmfs(kLogCache, kLogDebug | kLogSyslogErr,
             "Not mounting on cvmfs 2.0.X cache");
    return NULL;
  }

  return cache_manager.Release();
}


void PosixCacheManager::CtrlTxn(
  const std::string &description,
  const ObjectType type,
  const int flags,
  void *txn)
{
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  transaction->description = description;
  transaction->type = type;
}


int PosixCacheManager::Dup(int fd) {
  int new_fd = dup(fd);
  if (new_fd < 0)
    return -errno;
  return new_fd;
}


int PosixCacheManager::Flush(Transaction *transaction) {
  if (transaction->buf_pos == 0)
    return 0;
  int written =
    write(transaction->fd, transaction->buffer, transaction->buf_pos);
  if (written < 0)
    return -errno;
  if (static_cast<unsigned>(written) != transaction->buf_pos) {
    transaction->buf_pos -= written;
    return -EIO;
  }
  transaction->buf_pos = 0;
  return 0;
}


inline string PosixCacheManager::GetPathInCache(const shash::Any &id) {
  return cache_path_ + "/" + id.MakePathWithoutSuffix();
}


int64_t PosixCacheManager::GetSize(int fd) {
  platform_stat64 info;
  int retval = platform_fstat(fd, &info);
  if (retval != 0)
    return -errno;
  return info.st_size;
}


int PosixCacheManager::Open(const shash::Any &id) {
  const string path = GetPathInCache(id);
  int result = open(path.c_str(), O_RDONLY);

  if (result >= 0) {
    LogCvmfs(kLogCache, kLogDebug, "hit %s", path.c_str());
    // platform_disable_kcache(result);
    quota_mgr_->Touch(id);
  } else {
    result = -errno;
    LogCvmfs(kLogCache, kLogDebug, "miss %s (%d)", path.c_str(), result);
  }
  return result;
}


int PosixCacheManager::OpenFromTxn(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  int retval = Flush(transaction);
  if (retval < 0)
    return retval;
  int fd_rdonly = open(transaction->tmp_path.c_str(), O_RDONLY);
  if (fd_rdonly == -1)
    return -errno;
  return fd_rdonly;
}


int64_t PosixCacheManager::Pread(
  int fd,
  void *buf,
  uint64_t size,
  uint64_t offset)
{
  int64_t result;
  do {
    errno = 0;
    result = pread(fd, buf, size, offset);
  } while ((result == -1) && (errno == EINTR));
  if (result < 0)
    return -errno;
  return result;
}


int PosixCacheManager::Rename(const char *oldpath, const char *newpath) {
  int result;
  if (workaround_rename_ == false) {
    result = rename(oldpath, newpath);
    if (result < 0)
      return -errno;
    return 0;
  }

  result = link(oldpath, newpath);
  if (result < 0) {
    if (errno == EEXIST)
      LogCvmfs(kLogCache, kLogDebug, "%s already existed, ignoring", newpath);
    else
      return -errno;
  }
  result = unlink(oldpath);
  if (result < 0)
    return -errno;
  return 0;
}


/**
 * Used by the sqlite vfs in order to preload file catalogs into the file system
 * buffers.
 */
int PosixCacheManager::Readahead(int fd) {
  unsigned char *buf[4096];
  int nbytes;
  uint64_t pos = 0;
  do {
    nbytes = Pread(fd, buf, 4096, pos);
    pos += nbytes;
  } while (nbytes == 4096);
  LogCvmfs(kLogCache, kLogDebug, "read-ahead %d, %"PRIu64, fd, pos);
  if (nbytes < 0)
    return nbytes;
  return 0;
}


int PosixCacheManager::Reset(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  transaction->buf_pos = 0;
  transaction->size = 0;
  int retval = lseek(transaction->fd, 0, SEEK_SET);
  if (retval < 0)
    return -errno;
  retval = ftruncate(transaction->fd, 0);
  if (retval < 0)
    return -errno;
  return 0;
}


int PosixCacheManager::StartTxn(
  const shash::Any &id,
  uint64_t size,
  void *txn)
{
  atomic_inc32(&no_inflight_txns_);
  if (cache_mode_ == kCacheReadOnly) {
    atomic_dec32(&no_inflight_txns_);
    return -EROFS;
  }

  if (size != kSizeUnknown) {
    if (size > quota_mgr_->GetMaxFileSize()) {
      LogCvmfs(kLogCache, kLogDebug, "file too big for lru cache (%"PRIu64" "
                                     "requested but only %"PRIu64" bytes free)",
               size, quota_mgr_->GetMaxFileSize());
      atomic_dec32(&no_inflight_txns_);
      return -ENOSPC;
    }

    // Opportunistically clean up cache for large files
    if (size > kBigFile) {
      assert(quota_mgr_->GetCapacity() >= size);
      quota_mgr_->Cleanup(quota_mgr_->GetCapacity() - size);
    }
  }

  Transaction *transaction = new (txn) Transaction(id, GetPathInCache(id));
  const unsigned temp_path_len = txn_template_path_.length();

  char template_path[temp_path_len + 1];
  memcpy(template_path, &txn_template_path_[0], temp_path_len);
  template_path[temp_path_len] = '\0';
  transaction->fd = mkstemp(template_path);
  if (transaction->fd == -1) {
    transaction->~Transaction();
    atomic_dec32(&no_inflight_txns_);
    return -errno;
  }

  LogCvmfs(kLogCache, kLogDebug, "start transaction on %s has result %d",
           template_path, transaction->fd);
  transaction->tmp_path = template_path;
  transaction->expected_size = size;
  return transaction->fd;
}


void PosixCacheManager::TearDown2ReadOnly() {
  cache_mode_ = kCacheReadOnly;
  while (atomic_read32(&no_inflight_txns_) != 0)
    SafeSleepMs(50);

  QuotaManager *old_manager = quota_mgr_;
  quota_mgr_ = new NoopQuotaManager();
  delete old_manager;

  // TODO(jblomer): Hacks, should be handled elsewhere
  unlink(("running." + *cvmfs::repository_name_).c_str());
  LogCvmfs(kLogCache, kLogSyslog, "switch to read-only cache mode");
  SetLogMicroSyslog("");
}


int64_t PosixCacheManager::Write(const void *buf, uint64_t size, void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);

  if (transaction->expected_size != kSizeUnknown) {
    if (transaction->size + size > transaction->expected_size) {
      LogCvmfs(kLogCache, kLogDebug,
               "Transaction size (%"PRIu64") > expected size (%"PRIu64")",
               transaction->size + size, transaction->expected_size);
      return -ENOSPC;
    }
  }

  uint64_t written = 0;
  const unsigned char *read_pos = reinterpret_cast<const unsigned char *>(buf);
  while (written < size) {
    if (transaction->buf_pos == sizeof(transaction->buffer)) {
      int retval = Flush(transaction);
      if (retval != 0) {
        transaction->size += written;
        return retval;
      }
    }
    uint64_t remaining = size - written;
    uint64_t space_in_buffer =
      sizeof(transaction->buffer) - transaction->buf_pos;
    uint64_t batch_size = std::min(remaining, space_in_buffer);
    memcpy(transaction->buffer + transaction->buf_pos, read_pos, batch_size);
    transaction->buf_pos += batch_size;
    written += batch_size;
    read_pos += batch_size;
  }
  transaction->size += written;
  return written;
}

}  // namespace cache
