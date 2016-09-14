/**
 * This file is part of the CernVM File System.
 */
#include "cvmfs_config.h"
#include "cache_ram.h"

#include <errno.h>
#include <algorithm>
#include <cassert>
#include <cstring>
#include <new>

#include "kvstore.h"
#include "logging.h"
#include "util/posix.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace cache {


int RamCacheManager::AddFd(const ReadOnlyFd &fd) {
  if (fd_pivot_ >= fd_index_.size()) {
    LogCvmfs(kLogCache, kLogDebug, "too many open files (%u)", fd_pivot_);
    perf::Inc(counters_.n_enfile);
    return -ENFILE;
  }
  size_t next_fd = fd_index_[fd_pivot_];
  assert(next_fd < open_fds_.size());
  assert(open_fds_[next_fd].handle == kInvalidHandle);
  open_fds_[next_fd] = fd;
  open_fds_[next_fd].index = fd_pivot_;
  ++fd_pivot_;
  return next_fd;
}

bool RamCacheManager::AcquireQuotaManager(QuotaManager *quota_mgr) {
  assert(quota_mgr != NULL);
  quota_mgr_ = quota_mgr;
  LogCvmfs(kLogCache, kLogDebug, "set quota manager");
  return true;
}


int RamCacheManager::Open(const shash::Any &id) {
  WriteLockGuard guard(rwlock_);
  return DoOpen(id);
}


int RamCacheManager::DoOpen(const shash::Any &id) {
  bool ok;
  bool is_volatile;
  MemoryBuffer buf;

  if (regular_entries_.Contains(id)) {
    is_volatile = false;
  } else if (volatile_entries_.Contains(id)) {
    is_volatile = true;
  } else {
    LogCvmfs(kLogCache, kLogDebug, "miss for %s",
             id.ToString().c_str());
    perf::Inc(counters_.n_openmiss);
    return -ENOENT;
  }
  int fd = AddFd(ReadOnlyFd(id));
  if (fd < 0) {
    LogCvmfs(kLogCache, kLogDebug, "error while opening %s: %s",
             id.ToString().c_str(), strerror(-fd));
    return fd;
  }
  open_fds_[fd].is_volatile = is_volatile;
  if (is_volatile) {
    LogCvmfs(kLogCache, kLogDebug, "hit in volatile entries for %s",
             id.ToString().c_str());
    perf::Inc(counters_.n_openvolatile);
  } else {
    LogCvmfs(kLogCache, kLogDebug, "hit in regular entries for %s",
             id.ToString().c_str());
    perf::Inc(counters_.n_openregular);
  }
  ok = GetStore(open_fds_[fd])->IncRef(id);
  assert(ok);
  return fd;
}


int64_t RamCacheManager::GetSize(int fd) {
  ReadLockGuard guard(rwlock_);
  if (!IsValid(fd)) {
    LogCvmfs(kLogCache, kLogDebug, "bad fd %d on GetSize", fd);
    return -EBADF;
  }
  perf::Inc(counters_.n_getsize);
  return GetStore(open_fds_[fd])->GetSize(open_fds_[fd].handle);
}


int RamCacheManager::Close(int fd) {
  bool rc;

  WriteLockGuard guard(rwlock_);
  if (!IsValid(fd)) {
    LogCvmfs(kLogCache, kLogDebug, "bad fd %d on Close", fd);
    return -EBADF;
  }
  rc = GetStore(open_fds_[fd])->Unref(open_fds_[fd].handle);
  assert(rc);
  open_fds_[fd].handle = kInvalidHandle;

  size_t index = open_fds_[fd].index;
  assert(index < fd_index_.size());
  assert(fd_pivot_ <= fd_index_.size());
  assert(fd_pivot_ > 0);
  --fd_pivot_;
  if (index < fd_pivot_) {
    size_t other = fd_index_[fd_pivot_];
    assert(other < open_fds_.size());
    assert(open_fds_[other].handle != kInvalidHandle);
    open_fds_[other].index = index;
    fd_index_[index] = other;
    fd_index_[fd_pivot_] = fd;
  }
  LogCvmfs(kLogCache, kLogDebug, "closed fd %d", fd);
  perf::Inc(counters_.n_close);
  return 0;
}


int64_t RamCacheManager::Pread(
  int fd,
  void *buf,
  uint64_t size,
  uint64_t offset)
{
  ReadLockGuard guard(rwlock_);
  if (!IsValid(fd)) {
    LogCvmfs(kLogCache, kLogDebug, "bad fd %d on Pread", fd);
    return -EBADF;
  }
  perf::Inc(counters_.n_pread);
  return GetStore(open_fds_[fd])->Read(open_fds_[fd].handle, buf, size, offset);
}


int RamCacheManager::Dup(int fd) {
  bool ok;
  int rc;
  WriteLockGuard guard(rwlock_);
  if (!IsValid(fd)) {
    LogCvmfs(kLogCache, kLogDebug, "bad fd %d on Dup", fd);
    return -EBADF;
  }
  rc = AddFd(open_fds_[fd]);
  if (rc < 0) return rc;
  ok = GetStore(open_fds_[fd])->IncRef(open_fds_[fd].handle);
  assert(ok);
  LogCvmfs(kLogCache, kLogDebug, "dup fd %d", fd);
  perf::Inc(counters_.n_dup);
  return rc;
}


/**
 * For a RAM cache, read-ahead is a no-op.
 */
int RamCacheManager::Readahead(int fd) {
  ReadLockGuard guard(rwlock_);
  if (!IsValid(fd)) {
    LogCvmfs(kLogCache, kLogDebug, "bad fd %d on Readahead", fd);
    return -EBADF;
  }
  LogCvmfs(kLogCache, kLogDebug, "readahead (no-op) on %d", fd);
  perf::Inc(counters_.n_readahead);
  return 0;
}


int RamCacheManager::StartTxn(const shash::Any &id, uint64_t size, void *txn) {
  LogCvmfs(kLogCache, kLogDebug, "new transaction with id %s",
           id.ToString().c_str());
  Transaction *transaction = new (txn) Transaction();
  transaction->buffer.id = id;
  transaction->pos = 0;
  transaction->expected_size = size;
  transaction->buffer.size = (size == kSizeUnknown) ? kPageSize : size;
  transaction->buffer.address = malloc(transaction->buffer.size);
  if (!transaction->buffer.address && size > 0) {
    LogCvmfs(kLogCache, kLogDebug,
             "failed to allocate %lu B for %s",
             size, id.ToString().c_str());
    return -errno;
  }
  perf::Inc(counters_.n_starttxn);
  return 0;
}


void RamCacheManager::CtrlTxn(
  const string &description,
  const ObjectType type,
  const int flags,
  void *txn)
{
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  transaction->description = description;
  transaction->buffer.object_type = type;
  LogCvmfs(kLogCache, kLogDebug, "modified transaction %s",
           transaction->buffer.id.ToString().c_str());
}


int64_t RamCacheManager::Write(const void *buf, uint64_t size, void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);

  assert(transaction->pos <= transaction->buffer.size);
  if (transaction->pos + size > transaction->buffer.size) {
    if (transaction->expected_size == kSizeUnknown) {
      perf::Inc(counters_.n_realloc);
      size_t new_size = max(2*transaction->buffer.size,
        (size_t) (size + transaction->pos));
      LogCvmfs(kLogCache, kLogDebug, "reallocate transaction for %s to %u B",
               transaction->buffer.id.ToString().c_str(),
               transaction->buffer.size);
      void *new_ptr = realloc(transaction->buffer.address, new_size);
      if (!new_ptr) {
        LogCvmfs(kLogCache, kLogDebug,
                 "failed to allocate %lu B for %s",
                 new_size, transaction->buffer.id.ToString().c_str());
        return -EIO;
      }
      transaction->buffer.address = new_ptr;
      transaction->buffer.size = new_size;
    } else {
      LogCvmfs(kLogCache, kLogDebug,
               "attempted to write more than requested (%u>%u)",
               size, transaction->buffer.size);
      return -EIO;
    }
  }

  if (transaction->buffer.address && buf) {
    // LogCvmfs(kLogCache, kLogDebug, "copy %u bytes of transaction %s",
    //          size, transaction->id.ToString().c_str());
    memcpy(static_cast<char *>(transaction->buffer.address) + transaction->pos,
           buf, size);
  }
  transaction->pos += size;
  perf::Inc(counters_.n_write);
  return size;
}


int RamCacheManager::Reset(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  transaction->pos = 0;
  LogCvmfs(kLogCache, kLogDebug, "reset transaction %s",
           transaction->buffer.id.ToString().c_str());
  perf::Inc(counters_.n_reset);
  return 0;
}


int RamCacheManager::OpenFromTxn(void *txn) {
  WriteLockGuard guard(rwlock_);
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  int64_t retval = CommitToKvStore(transaction);
  if (retval < 0) {
    LogCvmfs(kLogCache, kLogDebug,
             "error while commiting transaction on %s: %s",
             transaction->buffer.id.ToString().c_str(), strerror(-retval));
    return retval;
  }
  LogCvmfs(kLogCache, kLogDebug, "open pending transaction for %s",
           transaction->buffer.id.ToString().c_str());
  perf::Inc(counters_.n_committxn);
  return DoOpen(transaction->buffer.id);
}


int RamCacheManager::AbortTxn(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  free(transaction->buffer.address);
  LogCvmfs(kLogCache, kLogDebug, "abort transaction %s",
           transaction->buffer.id.ToString().c_str());
  perf::Inc(counters_.n_aborttxn);
  return 0;
}


int RamCacheManager::CommitTxn(void *txn) {
  WriteLockGuard guard(rwlock_);
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  perf::Inc(counters_.n_committxn);
  int64_t rc = CommitToKvStore(transaction);
  if (rc < 0) return rc;
  free(transaction->buffer.address);
  return rc;
}


int64_t RamCacheManager::CommitToKvStore(Transaction *transaction) {
  MemoryKvStore *store;

  if (transaction->buffer.object_type == cache::CacheManager::kTypeVolatile) {
    store = &volatile_entries_;
  } else {
    store = &regular_entries_;
  }
  if (transaction->buffer.object_type == cache::CacheManager::kTypePinned ||
      transaction->buffer.object_type == cache::CacheManager::kTypeCatalog) {
    transaction->buffer.refcount = 1;
  } else {
    transaction->buffer.refcount = 0;
  }

  int64_t regular_size = regular_entries_.GetUsed();
  int64_t volatile_size = volatile_entries_.GetUsed();
  int64_t overrun = regular_size + volatile_size +
    transaction->buffer.size - max_size_;

  if (overrun > 0) {
    // if we're going to clean the cache, try to remove at least 25%
    overrun = max(overrun, (int64_t) max_size_>>2);
    perf::Inc(counters_.n_overrun);
    volatile_entries_.ShrinkTo(max((int64_t) 0, volatile_size - overrun));
  }
  overrun -= volatile_size - volatile_entries_.GetUsed();
  if (overrun > 0) {
    regular_entries_.ShrinkTo(max((int64_t) 0, regular_size - overrun));
  }
  overrun -= regular_size -regular_entries_.GetUsed();
  if (overrun > 0) {
    LogCvmfs(kLogCache, kLogDebug,
             "transaction for %s would overrun the cache limit by %d",
             transaction->buffer.id.ToString().c_str(), overrun);
    perf::Inc(counters_.n_full);
    return -ENOSPC;
  }

  if (store->Commit(transaction->buffer)) {
    LogCvmfs(kLogCache, kLogDebug, "committed %s to cache",
             transaction->buffer.id.ToString().c_str());
    return 0;
  } else {
    LogCvmfs(kLogCache, kLogDebug,
             "commit on %s failed",
             transaction->buffer.id.ToString().c_str());
    return -EIO;
  }
}

}  // namespace cache
