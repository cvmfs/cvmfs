/**
 * This file is part of the CernVM File System.
 */

#include "cache_ram.h"

#include <errno.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <new>

#include "kvstore.h"
#include "util/concurrency.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

const shash::Any RamCacheManager::kInvalidHandle;

string RamCacheManager::Describe() {
  return "Internal in-memory cache manager (size "
         + StringifyInt(max_size_ / (1024 * 1024)) + "MB)\n";
}


RamCacheManager::RamCacheManager(uint64_t max_size,
                                 unsigned max_entries,
                                 MemoryKvStore::MemoryAllocator alloc,
                                 perf::StatisticsTemplate statistics)
    : max_size_(max_size)
    , fd_table_(max_entries, ReadOnlyHandle())
    // TODO(jblomer): the number of slots in the kv-stores should _not_ be the
    // number of open files.
    , regular_entries_(max_entries,
                       alloc,
                       max_size,
                       perf::StatisticsTemplate("kv.regular", statistics))
    , volatile_entries_(max_entries,
                        alloc,
                        max_size,
                        perf::StatisticsTemplate("kv.volatile", statistics))
    , counters_(statistics) {
  int retval = pthread_rwlock_init(&rwlock_, NULL);
  assert(retval == 0);
  LogCvmfs(kLogCache, kLogDebug, "max %lu B, %u entries", max_size,
           max_entries);
  LogCvmfs(kLogCache, kLogDebug | kLogSyslogWarn,
           "DEPRECATION WARNING: The RAM cache manager is depcreated and "
           "will be removed from future releases.");
}


RamCacheManager::~RamCacheManager() { pthread_rwlock_destroy(&rwlock_); }


int RamCacheManager::AddFd(const ReadOnlyHandle &handle) {
  int result = fd_table_.OpenFd(handle);
  if (result == -ENFILE) {
    LogCvmfs(kLogCache, kLogDebug, "too many open files");
    perf::Inc(counters_.n_enfile);
  }
  return result;
}


bool RamCacheManager::AcquireQuotaManager(QuotaManager *quota_mgr) {
  assert(quota_mgr != NULL);
  quota_mgr_ = quota_mgr;
  LogCvmfs(kLogCache, kLogDebug, "set quota manager");
  return true;
}


int RamCacheManager::Open(const LabeledObject &object) {
  WriteLockGuard guard(rwlock_);
  return DoOpen(object.id);
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
    LogCvmfs(kLogCache, kLogDebug, "miss for %s", id.ToString().c_str());
    perf::Inc(counters_.n_openmiss);
    return -ENOENT;
  }
  ReadOnlyHandle generic_handle(id, is_volatile);
  int fd = AddFd(generic_handle);
  if (fd < 0) {
    LogCvmfs(kLogCache, kLogDebug, "error while opening %s: %s",
             id.ToString().c_str(), strerror(-fd));
    return fd;
  }
  if (is_volatile) {
    LogCvmfs(kLogCache, kLogDebug, "hit in volatile entries for %s",
             id.ToString().c_str());
    perf::Inc(counters_.n_openvolatile);
  } else {
    LogCvmfs(kLogCache, kLogDebug, "hit in regular entries for %s",
             id.ToString().c_str());
    perf::Inc(counters_.n_openregular);
  }
  ok = GetStore(generic_handle)->IncRef(id);
  assert(ok);
  return fd;
}


int64_t RamCacheManager::GetSize(int fd) {
  ReadLockGuard guard(rwlock_);
  ReadOnlyHandle generic_handle = fd_table_.GetHandle(fd);
  if (generic_handle.handle == kInvalidHandle) {
    LogCvmfs(kLogCache, kLogDebug, "bad fd %d on GetSize", fd);
    return -EBADF;
  }
  perf::Inc(counters_.n_getsize);
  return GetStore(generic_handle)->GetSize(generic_handle.handle);
}


int RamCacheManager::Close(int fd) {
  bool rc;

  WriteLockGuard guard(rwlock_);
  ReadOnlyHandle generic_handle = fd_table_.GetHandle(fd);
  if (generic_handle.handle == kInvalidHandle) {
    LogCvmfs(kLogCache, kLogDebug, "bad fd %d on Close", fd);
    return -EBADF;
  }
  rc = GetStore(generic_handle)->Unref(generic_handle.handle);
  assert(rc);

  int rc_int = fd_table_.CloseFd(fd);
  assert(rc_int == 0);
  LogCvmfs(kLogCache, kLogDebug, "closed fd %d", fd);
  perf::Inc(counters_.n_close);
  return 0;
}


int64_t RamCacheManager::Pread(int fd,
                               void *buf,
                               uint64_t size,
                               uint64_t offset) {
  ReadLockGuard guard(rwlock_);
  ReadOnlyHandle generic_handle = fd_table_.GetHandle(fd);
  if (generic_handle.handle == kInvalidHandle) {
    LogCvmfs(kLogCache, kLogDebug, "bad fd %d on Pread", fd);
    return -EBADF;
  }
  perf::Inc(counters_.n_pread);
  return GetStore(generic_handle)
      ->Read(generic_handle.handle, buf, size, offset);
}


int RamCacheManager::Dup(int fd) {
  bool ok;
  int rc;
  WriteLockGuard guard(rwlock_);
  ReadOnlyHandle generic_handle = fd_table_.GetHandle(fd);
  if (generic_handle.handle == kInvalidHandle) {
    LogCvmfs(kLogCache, kLogDebug, "bad fd %d on Dup", fd);
    return -EBADF;
  }
  rc = AddFd(generic_handle);
  if (rc < 0)
    return rc;
  ok = GetStore(generic_handle)->IncRef(generic_handle.handle);
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
  ReadOnlyHandle generic_handle = fd_table_.GetHandle(fd);
  if (generic_handle.handle == kInvalidHandle) {
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
    LogCvmfs(kLogCache, kLogDebug, "failed to allocate %lu B for %s", size,
             id.ToString().c_str());
    return -errno;
  }
  perf::Inc(counters_.n_starttxn);
  return 0;
}


void RamCacheManager::CtrlTxn(const Label &label, const int /* flags */,
                              void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  transaction->description = label.GetDescription();
  transaction->buffer.object_flags = label.flags;
  LogCvmfs(kLogCache, kLogDebug, "modified transaction %s",
           transaction->buffer.id.ToString().c_str());
}


int64_t RamCacheManager::Write(const void *buf, uint64_t size, void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);

  assert(transaction->pos <= transaction->buffer.size);
  if (transaction->pos + size > transaction->buffer.size) {
    if (transaction->expected_size == kSizeUnknown) {
      perf::Inc(counters_.n_realloc);
      size_t new_size = max(2 * transaction->buffer.size,
                            static_cast<size_t>(size + transaction->pos));
      LogCvmfs(kLogCache, kLogDebug, "reallocate transaction for %s to %lu B",
               transaction->buffer.id.ToString().c_str(),
               transaction->buffer.size);
      void *new_ptr = realloc(transaction->buffer.address, new_size);
      if (!new_ptr) {
        LogCvmfs(kLogCache, kLogDebug, "failed to allocate %lu B for %s",
                 new_size, transaction->buffer.id.ToString().c_str());
        return -EIO;
      }
      transaction->buffer.address = new_ptr;
      transaction->buffer.size = new_size;
    } else {
      LogCvmfs(kLogCache, kLogDebug,
               "attempted to write more than requested (%lu>%zu)", size,
               transaction->buffer.size);
      return -EFBIG;
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
             "error while committing transaction on %s: %s",
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
  if (rc < 0)
    return rc;
  free(transaction->buffer.address);
  return rc;
}


int64_t RamCacheManager::CommitToKvStore(Transaction *transaction) {
  MemoryKvStore *store;

  if (transaction->buffer.object_flags & CacheManager::kLabelVolatile) {
    store = &volatile_entries_;
  } else {
    store = &regular_entries_;
  }
  if ((transaction->buffer.object_flags & CacheManager::kLabelPinned)
      || (transaction->buffer.object_flags & CacheManager::kLabelCatalog)) {
    transaction->buffer.refcount = 1;
  } else {
    transaction->buffer.refcount = 0;
  }

  int64_t regular_size = regular_entries_.GetUsed();
  int64_t volatile_size = volatile_entries_.GetUsed();
  int64_t overrun = regular_size + volatile_size + transaction->buffer.size
                    - max_size_;

  if (overrun > 0) {
    // if we're going to clean the cache, try to remove at least 25%
    overrun = max(overrun, (int64_t)max_size_ >> 2);
    perf::Inc(counters_.n_overrun);
    volatile_entries_.ShrinkTo(max((int64_t)0, volatile_size - overrun));
  }
  overrun -= volatile_size - volatile_entries_.GetUsed();
  if (overrun > 0) {
    regular_entries_.ShrinkTo(max((int64_t)0, regular_size - overrun));
  }
  overrun -= regular_size - regular_entries_.GetUsed();
  if (overrun > 0) {
    LogCvmfs(kLogCache, kLogDebug,
             "transaction for %s would overrun the cache limit by %ld",
             transaction->buffer.id.ToString().c_str(), overrun);
    perf::Inc(counters_.n_full);
    return -ENOSPC;
  }

  int rc = store->Commit(transaction->buffer);
  if (rc < 0) {
    LogCvmfs(kLogCache, kLogDebug, "commit on %s failed",
             transaction->buffer.id.ToString().c_str());
    return rc;
  }
  LogCvmfs(kLogCache, kLogDebug, "committed %s to cache",
           transaction->buffer.id.ToString().c_str());
  return 0;
}
