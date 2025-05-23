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


#include "cache_posix.h"

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

#include "crypto/hash.h"
#include "crypto/signature.h"
#include "directory_entry.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "network/download.h"
#include "quota.h"
#include "shortstring.h"
#include "statistics.h"
#include "util/atomic.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

namespace {

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

}  // anonymous namespace


//------------------------------------------------------------------------------


const uint64_t PosixCacheManager::kBigFile = 25 * 1024 * 1024;  // 25M


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
  int retval = do_refcount_ ? fd_mgr_->Close(fd) : close(fd);
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
    if ((transaction->expected_size != kSizeUnknown)
        && (reports_correct_filesize_ || (transaction->size != 0))) {
      LogCvmfs(kLogCache, kLogDebug | kLogSyslogErr,
               "size check failure for %s, expected %lu, got %lu",
               transaction->id.ToString().c_str(), transaction->expected_size,
               transaction->size);
      CopyPath2Path(transaction->tmp_path,
                    cache_path_ + "/quarantaine/" + transaction->id.ToString());
      unlink(transaction->tmp_path.c_str());
      transaction->~Transaction();
      atomic_dec32(&no_inflight_txns_);
      return -EIO;
    }
  }

  if ((transaction->label.flags & kLabelPinned)
      || (transaction->label.flags & kLabelCatalog)) {
    bool retval = quota_mgr_->Pin(transaction->id, transaction->size,
                                  transaction->label.GetDescription(),
                                  (transaction->label.flags & kLabelCatalog));
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
  result = Rename(transaction->tmp_path.c_str(),
                  transaction->final_path.c_str());
  if (result < 0) {
    LogCvmfs(kLogCache, kLogDebug, "commit failed: %s", strerror(errno));
    unlink(transaction->tmp_path.c_str());
    if ((transaction->label.flags & kLabelPinned)
        || (transaction->label.flags & kLabelCatalog)) {
      quota_mgr_->Remove(transaction->id);
    }
  } else {
    // Success, inform quota manager
    if (transaction->label.flags & kLabelVolatile) {
      quota_mgr_->InsertVolatile(transaction->id, transaction->size,
                                 transaction->label.GetDescription());
    } else if (!transaction->label.IsCatalog()
               && !transaction->label.IsPinned()) {
      quota_mgr_->Insert(transaction->id, transaction->size,
                         transaction->label.GetDescription());
    }
  }
  transaction->~Transaction();
  atomic_dec32(&no_inflight_txns_);
  return result;
}

bool PosixCacheManager::InitCacheDirectory(const string &cache_path) {
  FileSystemInfo fs_info = GetFileSystemInfo(cache_path);

  if (fs_info.type == kFsTypeTmpfs) {
    is_tmpfs_ = true;
  }

  if (alien_cache_) {
    if (!MakeCacheDirectories(cache_path, 0770)) {
      return false;
    }
    LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
             "Cache directory structure created.");
    switch (fs_info.type) {
      case kFsTypeNFS:
        rename_workaround_ = kRenameLink;
        LogCvmfs(kLogCache, kLogDebug | kLogSyslog, "Alien cache is on NFS.");
        break;
      case kFsTypeBeeGFS:
        rename_workaround_ = kRenameSamedir;
        LogCvmfs(kLogCache, kLogDebug | kLogSyslog,
                 "Alien cache is on BeeGFS.");
        break;
      default:
        break;
    }
  } else {
    if (!MakeCacheDirectories(cache_path, 0700))
      return false;
  }

  // TODO(jblomer): we might not need to look anymore for cvmfs 2.0 relicts
  if (FileExists(cache_path + "/cvmfscatalog.cache")) {
    LogCvmfs(kLogCache, kLogDebug | kLogSyslogErr,
             "Not mounting on cvmfs 2.0.X cache");
    return false;
  }
  return true;
}

PosixCacheManager *PosixCacheManager::Create(
    const string &cache_path,
    const bool alien_cache,
    const RenameWorkarounds rename_workaround,
    const bool do_refcount) {
  UniquePtr<PosixCacheManager> cache_manager(
      new PosixCacheManager(cache_path, alien_cache, do_refcount));
  assert(cache_manager.IsValid());

  cache_manager->rename_workaround_ = rename_workaround;

  bool result_ = cache_manager->InitCacheDirectory(cache_path);
  if (!result_) {
    return NULL;
  }

  return cache_manager.Release();
}


void PosixCacheManager::CtrlTxn(const Label &label,
                                const int flags,
                                void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  transaction->label = label;
}


string PosixCacheManager::Describe() {
  string msg;
  if (do_refcount_) {
    msg = "Refcounting Posix cache manager"
          "(cache directory: "
          + cache_path_ + ")\n";
  } else {
    msg = "Posix cache manager (cache directory: " + cache_path_ + ")\n";
  }
  return msg;
}


/**
 * If not refcounting, nothing to do, the kernel keeps the state
 * of open file descriptors.  Return a dummy memory location.
 */
void *PosixCacheManager::DoSaveState() {
  if (do_refcount_) {
    SavedState *state = new SavedState();
    state->fd_mgr = fd_mgr_->Clone();
    return state;
  }
  char *c = reinterpret_cast<char *>(smalloc(1));
  *c = kMagicNoRefcount;
  return c;
}


int PosixCacheManager::DoRestoreState(void *data) {
  assert(data);
  if (do_refcount_) {
    SavedState *state = reinterpret_cast<SavedState *>(data);
    if (state->magic_number == kMagicRefcount) {
      LogCvmfs(kLogCache, kLogDebug,
               "Restoring refcount cache manager from "
               "refcounted posix cache manager");

      fd_mgr_->AssignFrom(state->fd_mgr.weak_ref());
    } else {
      LogCvmfs(kLogCache, kLogDebug,
               "Restoring refcount cache manager from "
               "non-refcounted posix cache manager");
    }
    return -1;
  }

  char *c = reinterpret_cast<char *>(data);
  assert(*c == kMagicNoRefcount || *c == kMagicRefcount);
  if (*c == kMagicRefcount) {
    SavedState *state = reinterpret_cast<SavedState *>(data);
    LogCvmfs(kLogCache, kLogDebug,
             "Restoring non-refcount cache manager from "
             "refcounted posix cache manager - this "
             " is not possible, keep refcounting.");
    fd_mgr_->AssignFrom(state->fd_mgr.weak_ref());
    do_refcount_ = true;
  }
  return -1;
}


bool PosixCacheManager::DoFreeState(void *data) {
  assert(data);
  SavedState *state = reinterpret_cast<SavedState *>(data);
  if (state->magic_number == kMagicRefcount) {
    delete state;
  } else {
    // If not refcounted, the state is the dummy SavedState
    // of the regular posix cache manager
    free(data);
  }
  return true;
}


int PosixCacheManager::Dup(int fd) {
  int new_fd = do_refcount_ ? fd_mgr_->Dup(fd) : dup(fd);
  if (new_fd < 0)
    return -errno;
  return new_fd;
}


int PosixCacheManager::Flush(Transaction *transaction) {
  if (transaction->buf_pos == 0)
    return 0;
  int written = write(transaction->fd, transaction->buffer,
                      transaction->buf_pos);
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


int PosixCacheManager::Open(const LabeledObject &object) {
  const string path = GetPathInCache(object.id);
  int result;
  if (do_refcount_) {
    result = fd_mgr_->Open(object.id, path);
  } else {
    result = open(path.c_str(), O_RDONLY);
  }
  if (result >= 0) {
    LogCvmfs(kLogCache, kLogDebug, "hit %s", path.c_str());
    // platform_disable_kcache(result);
    quota_mgr_->Touch(object.id);
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
  int fd_rdonly;

  if (do_refcount_) {
    fd_rdonly = fd_mgr_->Open(transaction->id, transaction->tmp_path.c_str());
  } else {
    fd_rdonly = open(transaction->tmp_path.c_str(), O_RDONLY);
  }
  if (fd_rdonly == -1)
    return -errno;
  return fd_rdonly;
}


int64_t PosixCacheManager::Pread(int fd,
                                 void *buf,
                                 uint64_t size,
                                 uint64_t offset) {
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
  if (rename_workaround_ != kRenameLink) {
    result = rename(oldpath, newpath);
    if (result < 0)
      return -errno;
    return 0;
  }

  result = link(oldpath, newpath);
  if (result < 0) {
    if (errno == EEXIST) {
      LogCvmfs(kLogCache, kLogDebug, "%s already existed, ignoring", newpath);
    } else {
      return -errno;
    }
  }
  result = unlink(oldpath);
  if (result < 0)
    return -errno;
  return 0;
}


/**
 * Used by the sqlite vfs in order to preload file catalogs into the file system
 * buffers.
 *
 * No-op if the fd is to a file that is on a tmpfs, and so already in page cache
 */
int PosixCacheManager::Readahead(int fd) {
  unsigned char *buf[4096];
  int nbytes;
  uint64_t pos = 0;
  if (is_tmpfs()) {
    return 0;
  }
  do {
    nbytes = Pread(fd, buf, 4096, pos);
    pos += nbytes;
  } while (nbytes == 4096);
  LogCvmfs(kLogCache, kLogDebug, "read-ahead %d, %" PRIu64, fd, pos);
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


int PosixCacheManager::StartTxn(const shash::Any &id,
                                uint64_t size,
                                void *txn) {
  atomic_inc32(&no_inflight_txns_);
  if (cache_mode_ == kCacheReadOnly) {
    atomic_dec32(&no_inflight_txns_);
    return -EROFS;
  }

  if (size != kSizeUnknown) {
    if (size > quota_mgr_->GetMaxFileSize()) {
      LogCvmfs(kLogCache, kLogDebug,
               "file too big for lru cache (%" PRIu64 " "
               "requested but only %" PRIu64 " bytes free)",
               size, quota_mgr_->GetMaxFileSize());
      atomic_dec32(&no_inflight_txns_);
      return -ENOSPC;
    }

    // For large files, ensure enough free cache space before writing the chunk
    if (size > kBigFile) {
      uint64_t cache_size = quota_mgr_->GetSize();
      uint64_t cache_capacity = quota_mgr_->GetCapacity();
      assert(cache_capacity >= size);
      if ((cache_size + size) > cache_capacity) {
        uint64_t leave_size = std::min(cache_capacity / 2,
                                       cache_capacity - size);
        quota_mgr_->Cleanup(leave_size);
      }
    }
  }

  string path_in_cache = GetPathInCache(id);
  Transaction *transaction = new (txn) Transaction(id, path_in_cache);

  char *template_path = NULL;
  unsigned temp_path_len = 0;
  if (rename_workaround_ == kRenameSamedir) {
    temp_path_len = path_in_cache.length() + 6;
    template_path = reinterpret_cast<char *>(alloca(temp_path_len + 1));
    memcpy(template_path, path_in_cache.data(), path_in_cache.length());
    memset(template_path + path_in_cache.length(), 'X', 6);
  } else {
    temp_path_len = txn_template_path_.length();
    template_path = reinterpret_cast<char *>(alloca(temp_path_len + 1));
    memcpy(template_path, &txn_template_path_[0], temp_path_len);
  }
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


manifest::Breadcrumb PosixCacheManager::LoadBreadcrumb(
    const std::string &fqrn) {
  return manifest::Manifest::ReadBreadcrumb(fqrn, cache_path_);
}


bool PosixCacheManager::StoreBreadcrumb(const manifest::Manifest &manifest) {
  return manifest.ExportBreadcrumb(cache_path_, 0600);
}


bool PosixCacheManager::StoreBreadcrumb(std::string fqrn,
                                        manifest::Breadcrumb breadcrumb) {
  return breadcrumb.Export(fqrn, cache_path_, 0600);
}


void PosixCacheManager::TearDown2ReadOnly() {
  cache_mode_ = kCacheReadOnly;
  while (atomic_read32(&no_inflight_txns_) != 0)
    SafeSleepMs(50);

  QuotaManager *old_manager = quota_mgr_;
  quota_mgr_ = new NoopQuotaManager();
  delete old_manager;
}


int64_t PosixCacheManager::Write(const void *buf, uint64_t size, void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);

  if (transaction->expected_size != kSizeUnknown) {
    if (transaction->size + size > transaction->expected_size) {
      LogCvmfs(kLogCache, kLogDebug,
               "Transaction size (%" PRIu64 ") > expected size (%" PRIu64 ")",
               transaction->size + size, transaction->expected_size);
      return -EFBIG;
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
    uint64_t space_in_buffer = sizeof(transaction->buffer)
                               - transaction->buf_pos;
    uint64_t batch_size = std::min(remaining, space_in_buffer);
    memcpy(transaction->buffer + transaction->buf_pos, read_pos, batch_size);
    transaction->buf_pos += batch_size;
    written += batch_size;
    read_pos += batch_size;
  }
  transaction->size += written;
  return written;
}
