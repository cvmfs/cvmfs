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
#include "smalloc.h"
#include "util/posix.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace cache {


int RamCacheManager::AddFd(const ReadOnlyFd &fd) {
  unsigned i = 0;
  for ( ; i < open_fds_.size(); ++i) {
    if (open_fds_[i].handle == kInvalidHandle) {
      open_fds_[i] = fd;
      return i;
    }
  }
  if (open_fds_.size() < kMaxHandles) {
    open_fds_.push_back(fd);
    return i;
  } else {
    return -ENFILE;
  }
}

bool RamCacheManager::AcquireQuotaManager(QuotaManager *quota_mgr) {
  return false;
}


int RamCacheManager::Open(const shash::Any &id) {
  WriteLockGuard guard(rwlock_);
  return DoOpen(id);
}

int RamCacheManager::DoOpen(const shash::Any &id) {
  bool rc;
  kvstore::MemoryBuffer buf;
  int fd = AddFd(ReadOnlyFd(id, 0));
  if (fd < 0) return fd;

  if (regular_entries_.GetBuffer(id, &buf)) {
    open_fds_[fd].store = &regular_entries_;
    rc = regular_entries_.IncRef(id);
    assert(rc);
    return fd;
  } else if (volatile_entries_.GetBuffer(id, &buf)) {
    open_fds_[fd].store = &volatile_entries_;
    rc = volatile_entries_.IncRef(id);
    assert(rc);
    return fd;
  } else {
    open_fds_[fd].handle = kInvalidHandle;
    return -ENOENT;
  }
}


int64_t RamCacheManager::GetSize(int fd) {
  ReadLockGuard guard(rwlock_);
  if (!IsValid(fd)) return -EBADFD;
  assert(open_fds_[fd].store);
  return open_fds_[fd].store->GetSize(open_fds_[fd].handle);
}


int RamCacheManager::Close(int fd) {
  bool rc;
  bool sweep_tail = false;

  WriteLockGuard guard(rwlock_);
  if (!IsValid(fd)) return -EBADFD;
  assert(open_fds_[fd].store);
  rc = open_fds_[fd].store->Unref(open_fds_[fd].handle);
  assert(rc);
  open_fds_[fd].handle = kInvalidHandle;
  sweep_tail = (static_cast<unsigned>(fd) == (open_fds_.size() - 1));

  if (sweep_tail) {
    unsigned last_good_idx = open_fds_.size() - 1;
    while (open_fds_[last_good_idx].handle != kInvalidHandle)
      last_good_idx--;
    open_fds_.resize(last_good_idx + 1);
  }

  return 0;
}


int64_t RamCacheManager::Pread(
  int fd,
  void *buf,
  uint64_t size,
  uint64_t offset)
{
  ReadLockGuard guard(rwlock_);
  if (!IsValid(fd)) return -EBADFD;
  assert(open_fds_[fd].store);
  return open_fds_[fd].store->Read(open_fds_[fd].handle, buf, size, offset);
}


int RamCacheManager::Dup(int fd) {
  bool rc;
  WriteLockGuard guard(rwlock_);
  if (!IsValid(fd)) return -EBADFD;
  assert(open_fds_[fd].store);
  rc = open_fds_[fd].store->IncRef(open_fds_[fd].handle);
  assert(rc);
  return AddFd(open_fds_[fd]);
}


/**
 * For a RAM cache, read-ahead is a no-op.
 */
int RamCacheManager::Readahead(int fd) {
  ReadLockGuard guard(rwlock_);
  if (!IsValid(fd))
    return -EBADF;
  return 0;
}


int RamCacheManager::StartTxn(const shash::Any &id, uint64_t size, void *txn) {
  Transaction *transaction = new (txn) Transaction();
  transaction->id = id;
  transaction->pos = 0;
  transaction->expected_size = size;
  transaction->size = (size == kSizeUnknown) ? kPageSize : size;
  if (transaction->size) {
    transaction->buffer = scalloc(1, transaction->size);
  }
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
}


int64_t RamCacheManager::Write(const void *buf, uint64_t size, void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);

  if (transaction->pos + size > transaction->size) {
    if (transaction->expected_size == kSizeUnknown) {
      transaction->size = max(2*transaction->size, size + transaction->pos);
      transaction->buffer = realloc(transaction->buffer, transaction->size);
      if (!transaction->buffer) return -errno;
    } else {
      return -ENOSPC;
    }
  }


  uint64_t copy_size = min(size, transaction->size - transaction->pos);
  memcpy(static_cast<char *>(transaction->buffer) + transaction->pos,
         buf, copy_size);
  transaction->pos += copy_size;
  return copy_size;
}


int RamCacheManager::Reset(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  transaction->pos = 0;
  return 0;
}


int RamCacheManager::OpenFromTxn(void *txn) {
  WriteLockGuard guard(rwlock_);
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  int64_t retval = CommitToKvStore(transaction);
  if (retval < 0) return retval;
  return DoOpen(transaction->id);
}


int RamCacheManager::AbortTxn(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  if (transaction->buffer)
    free(transaction->buffer);
  return 0;
}


int RamCacheManager::CommitTxn(void *txn) {
  WriteLockGuard guard(rwlock_);
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  return CommitToKvStore(transaction);
}


int64_t RamCacheManager::CommitToKvStore(Transaction *transaction) {
  kvstore::MemoryBuffer buf;
  buf.address = transaction->buffer;
  kvstore::MemoryKvStore *store;
  if (transaction->expected_size == kSizeUnknown) {
    buf.size = transaction->pos;
    buf.address = realloc(buf.address, buf.size);
    if (!buf.address) return -errno;
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
    volatile_entries_.ShrinkTo(max((int64_t) 0, volatile_size - overrun));
  }
  overrun -= volatile_size - volatile_entries_.GetUsed();
  if (overrun > 0) {
    regular_entries_.ShrinkTo(max((int64_t) 0, regular_size - overrun));
  }
  overrun -= regular_size - regular_entries_.GetUsed();
  if (overrun > 0) return -ENOSPC;

  if (!store->Commit(transaction->id, buf)) return -EEXIST;
  return 0;
}

}  // namespace cache
