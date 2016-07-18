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
#include "smalloc.h"
#include "util/posix.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace cache {


int RamCacheManager::AddFd(const ReadOnlyFd &fd) {
  unsigned i = 0;
  perf::Inc(counters_.n_addfd);
  for ( ; i < open_fds_.size(); ++i) {
    if (open_fds_[i].handle == kInvalidHandle) {
      open_fds_[i] = fd;
      LogCvmfs(kLogCache, kLogDebug, "found free fd %u", i);
      perf::Inc(counters_.n_reusefd);
      return i;
    }
  }
  if (open_fds_.size() < kMaxHandles) {
    open_fds_.push_back(fd);
    LogCvmfs(kLogCache, kLogDebug, "adding fd %u", i);
    perf::Inc(counters_.n_appendfd);
    return i;
  } else {
    LogCvmfs(kLogCache, kLogDebug, "too many open files (%u)", kMaxHandles);
    perf::Inc(counters_.n_enfile);
    return -ENFILE;
  }
}

bool RamCacheManager::AcquireQuotaManager(QuotaManager *quota_mgr) {
  perf::Inc(counters_.n_acquire);
  if (quota_mgr == NULL) {
    LogCvmfs(kLogCache, kLogDebug, "null quota manager");
    return false;
  }
  quota_mgr_ = quota_mgr;
  LogCvmfs(kLogCache, kLogDebug, "set quota manager");
  return true;
}


int RamCacheManager::Open(const shash::Any &id) {
  perf::Inc(counters_.n_open);
  WriteLockGuard guard(rwlock_);
  return DoOpen(id);
}

int RamCacheManager::DoOpen(const shash::Any &id) {
  bool rc;
  kvstore::MemoryBuffer buf;
  int fd = AddFd(ReadOnlyFd(id, 0));
  if (fd < 0) {
    LogCvmfs(kLogCache, kLogDebug, "error while opening %s: %s",
             id.ToString().c_str(), strerror(-fd));
    return fd;
  }

  if (regular_entries_.GetBuffer(id, &buf)) {
    open_fds_[fd].store = &regular_entries_;
    rc = regular_entries_.IncRef(id);
    assert(rc);
    LogCvmfs(kLogCache, kLogDebug, "hit in regular entries for %s",
             id.ToString().c_str());
    perf::Inc(counters_.n_openregular);
    return fd;
  } else if (volatile_entries_.GetBuffer(id, &buf)) {
    open_fds_[fd].store = &volatile_entries_;
    rc = volatile_entries_.IncRef(id);
    assert(rc);
    LogCvmfs(kLogCache, kLogDebug, "hit in volatile entries for %s",
             id.ToString().c_str());
    perf::Inc(counters_.n_openvolatile);
    return fd;
  } else {
    open_fds_[fd].handle = kInvalidHandle;
    LogCvmfs(kLogCache, kLogDebug, "miss for %s",
             id.ToString().c_str());
    perf::Inc(counters_.n_openmiss);
    return -ENOENT;
  }
}


int64_t RamCacheManager::GetSize(int fd) {
  ReadLockGuard guard(rwlock_);
  if (!IsValid(fd)) {
    LogCvmfs(kLogCache, kLogDebug, "bad fd %d on GetSize", fd);
    return -EBADF;
  }
  assert(open_fds_[fd].store);
  perf::Inc(counters_.n_getsize);
  return open_fds_[fd].store->GetSize(open_fds_[fd].handle);
}


int RamCacheManager::Close(int fd) {
  bool rc;
  bool sweep_tail = false;

  WriteLockGuard guard(rwlock_);
  if (!IsValid(fd)) {
    LogCvmfs(kLogCache, kLogDebug, "bad fd %d on Close", fd);
    return -EBADF;
  }
  assert(open_fds_[fd].store);
  rc = open_fds_[fd].store->Unref(open_fds_[fd].handle);
  assert(rc);
  open_fds_[fd].handle = kInvalidHandle;
  LogCvmfs(kLogCache, kLogDebug, "closed fd %d", fd);
  sweep_tail = (static_cast<unsigned>(fd) == (open_fds_.size() - 1));

  if (sweep_tail) {
    perf::Inc(counters_.n_closesweep);
    unsigned last_good_idx = open_fds_.size() - 1;
    while (open_fds_[last_good_idx].handle != kInvalidHandle)
      last_good_idx--;
    open_fds_.resize(last_good_idx + 1);
    LogCvmfs(kLogCache, kLogDebug, "resized fd vector to %u",
             last_good_idx + 1);
  }

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
  assert(open_fds_[fd].store);
  perf::Inc(counters_.n_pread);
  return open_fds_[fd].store->Read(open_fds_[fd].handle, buf, size, offset);
}


int RamCacheManager::Dup(int fd) {
  bool rc;
  WriteLockGuard guard(rwlock_);
  if (!IsValid(fd)) {
    LogCvmfs(kLogCache, kLogDebug, "bad fd %d on Dup", fd);
    return -EBADF;
  }
  assert(open_fds_[fd].store);
  rc = open_fds_[fd].store->IncRef(open_fds_[fd].handle);
  assert(rc);
  LogCvmfs(kLogCache, kLogDebug, "dup fd %d", fd);
  perf::Inc(counters_.n_dup);
  return AddFd(open_fds_[fd]);
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
  transaction->id = id;
  transaction->pos = 0;
  transaction->expected_size = size;
  transaction->size = (size == kSizeUnknown) ? kPageSize : size;
  if (transaction->size) {
    transaction->buffer = scalloc(1, transaction->size);
    perf::Xadd(counters_.sz_alloc, transaction->size);
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
  transaction->object_type = type;
  LogCvmfs(kLogCache, kLogDebug, "modified transaction %s",
           transaction->id.ToString().c_str());
  perf::Inc(counters_.n_ctrltxn);
}


int64_t RamCacheManager::Write(const void *buf, uint64_t size, void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);

  if (transaction->pos + size > transaction->size) {
    if (transaction->expected_size == kSizeUnknown) {
      perf::Inc(counters_.n_realloc);
      transaction->size = max(2*transaction->size, size + transaction->pos);
      LogCvmfs(kLogCache, kLogDebug, "reallocate transaction for %s to %u B",
               transaction->id.ToString().c_str(), transaction->size);
      transaction->buffer = realloc(transaction->buffer, transaction->size);
      if (!transaction->buffer) {
        LogCvmfs(kLogCache, kLogDebug, "realloc failed: %s", strerror(errno));
        return -errno;
      }
    } else {
      LogCvmfs(kLogCache, kLogDebug,
               "attempted to write more than requested (%u>%u)",
               size, transaction->size);
      return -ENOSPC;
    }
  }


  uint64_t copy_size = min(size, transaction->size - transaction->pos);
  LogCvmfs(kLogCache, kLogDebug, "copy %u bytes of transaction %s",
           copy_size, transaction->id.ToString().c_str());
  memcpy(static_cast<char *>(transaction->buffer) + transaction->pos,
         buf, copy_size);
  transaction->pos += copy_size;
  perf::Inc(counters_.n_write);
  return copy_size;
}


int RamCacheManager::Reset(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  transaction->pos = 0;
  LogCvmfs(kLogCache, kLogDebug, "reset transaction %s",
           transaction->id.ToString().c_str());
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
             transaction->id.ToString().c_str(), strerror(-retval));
    return retval;
  }
  LogCvmfs(kLogCache, kLogDebug, "open pending transaction for %s",
           transaction->id.ToString().c_str());
  perf::Inc(counters_.n_openfromtxn);
  return DoOpen(transaction->id);
}


int RamCacheManager::AbortTxn(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  if (transaction->buffer)
    free(transaction->buffer);
  LogCvmfs(kLogCache, kLogDebug, "abort transaction %s",
           transaction->id.ToString().c_str());
  perf::Inc(counters_.n_aborttxn);
  return 0;
}


int RamCacheManager::CommitTxn(void *txn) {
  WriteLockGuard guard(rwlock_);
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  perf::Inc(counters_.n_committxn);
  return CommitToKvStore(transaction);
}


int64_t RamCacheManager::CommitToKvStore(Transaction *transaction) {
  kvstore::MemoryBuffer buf;
  buf.address = transaction->buffer;
  kvstore::MemoryKvStore *store;
  if (transaction->expected_size == kSizeUnknown) {
    buf.size = transaction->pos;
    buf.address = realloc(buf.address, buf.size);
    LogCvmfs(kLogCache, kLogDebug, "reallocating transaction on %s to %u B",
             transaction->id.ToString().c_str(), buf.size);
    if (!buf.address) {
      LogCvmfs(kLogCache, kLogDebug, "realloc failed: %s", strerror(errno));
      return -errno;
    }
  } else {
    buf.size = transaction->size;
  }
  buf.object_type = transaction->object_type;

  if (buf.object_type == cache::CacheManager::kTypeVolatile) {
    store = &volatile_entries_;
  } else {
    store = &regular_entries_;
  }
  if (buf.object_type == cache::CacheManager::kTypePinned ||
      buf.object_type == cache::CacheManager::kTypePinned) {
    buf.refcount = 1;
  } else {
    buf.refcount = 0;
  }

  int64_t regular_size = regular_entries_.GetUsed();
  int64_t volatile_size = volatile_entries_.GetUsed();
  int64_t overrun = regular_size + volatile_size + buf.size - max_size_;

  if (overrun > 0) {
    perf::Inc(counters_.n_overrun);
    volatile_entries_.ShrinkTo(max((int64_t) 0, volatile_size - overrun));
  }
  overrun -= volatile_size - volatile_entries_.GetUsed();
  if (overrun > 0) {
    regular_entries_.ShrinkTo(max((int64_t) 0, regular_size - overrun));
  }
  overrun -= regular_size - regular_entries_.GetUsed();
  if (overrun > 0) {
    LogCvmfs(kLogCache, kLogDebug,
             "transaction for %s would overrun the cache limit by %d",
             transaction->id.ToString().c_str(), -overrun);
    perf::Inc(counters_.n_full);
    return -ENOSPC;
  }

  store->Commit(transaction->id, buf);
  perf::Xadd(counters_.sz_committed, buf.size);
  LogCvmfs(kLogCache, kLogDebug, "committed %s to cache",
           transaction->id.ToString().c_str());
  perf::Inc(counters_.n_committokvstore);
  return 0;
}

}  // namespace cache
