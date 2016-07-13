/**
 * This file is part of the CernVM File System.
 */
#include "cvmfs_config.h"
#include "cache_ram.h"

#include <errno.h>

#include <new>

#include "smalloc.h"
#include "util/posix.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace cache {


int RamCacheManager::AddFd(const ReadOnlyFd &fd) {
  unsigned i = 0;
  for ( ; i < open_fds_.size(); ++i) {
    if (open_fds_[i].handle == -1) {
      open_fds_[i] = fd;
      return i;
    }
  }
  open_fds_.push_back(fd);
  return i;
}


RamCacheManager::RamCacheManager() {
  int retval = pthread_rwlock_init(&rwlock_, NULL);
  assert(retval == 0);
}


RamCacheManager::~RamCacheManager() {
  pthread_rwlock_destroy(&rwlock_);
}


bool RamCacheManager::AcquireQuotaManager(QuotaManager *quota_mgr) {
  return false;
}


int RamCacheManager::Open(const shash::Any &id) {
  // TODO(jblomer): get handle, size from kv store
  ReadOnlyFd fd(0, 0);

  WriteLockGuard guard(rwlock_);
  return AddFd(fd);
}


int64_t RamCacheManager::GetSize(int fd) {
  ReadLockGuard guard(rwlock_);
  if (!IsValid(fd))
    return -EBADF;
  return open_fds_[fd].size;
}


int RamCacheManager::Close(int fd) {
  bool sweep_tail = false;
  {
    ReadLockGuard guard(rwlock_);
    if (!IsValid(fd))
      return -EBADF;
    // TODO(jblomer): KvStore
    open_fds_[fd].handle = -1;
    sweep_tail = (static_cast<unsigned>(fd) == (open_fds_.size() - 1));
  }

  if (sweep_tail) {
    WriteLockGuard guard(rwlock_);
    unsigned last_good_idx = open_fds_.size() - 1;
    while (open_fds_[last_good_idx].handle < 0)
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
  return -EIO;
}


int RamCacheManager::Dup(int fd) {
  ReadOnlyFd file_descriptor;
  {
    ReadLockGuard guard(rwlock_);
    if (!IsValid(fd))
      return -EBADF;
    file_descriptor = open_fds_[fd];
  }

  // TODO(jblomer): increase link count in kv store
  WriteLockGuard guard(rwlock_);
  return AddFd(file_descriptor);
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
    transaction->buffer = smmap(transaction->size);
  }
  return 0;
}


void RamCacheManager::CtrlTxn(
  const string &description,
  const ObjectType type,
  const int flags,
  void *txn)
{
}


int64_t RamCacheManager::Write(const void *buf, uint64_t size, void *txn) {
  return -EIO;
}


int RamCacheManager::Reset(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  transaction->pos = 0;
  return 0;
}


int RamCacheManager::OpenFromTxn(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  int64_t handle = CommitToKvStore(transaction);
  if (handle < 0)
    return handle;
  ReadOnlyFd file_descriptor(handle, transaction->pos);

  WriteLockGuard guard(rwlock_);
  return AddFd(file_descriptor);
}


int RamCacheManager::AbortTxn(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  if (transaction->buffer)
    free(transaction->buffer);
  return 0;
}


int RamCacheManager::CommitTxn(void *txn) {
  Transaction *transaction = reinterpret_cast<Transaction *>(txn);
  int retval = CommitToKvStore(transaction);
  return retval;
}


int64_t RamCacheManager::CommitToKvStore(Transaction *transaction) {
  if (transaction->handle >= 0)
    return transaction->handle;

  // TODO(jblomer): commit transaction in kv store
  transaction->handle = 0;
  if (transaction->buffer) {
    free(transaction->buffer);
    transaction->buffer = NULL;
  }
  return transaction->handle;
}

}  // namespace cache
