/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_CACHE_RAM_H_
#define CVMFS_CACHE_RAM_H_

#include <pthread.h>

#include <cassert>
#include <cstdlib>
#include <string>
#include <vector>

#include "statistics.h"
#include "cache.h"
#include "hash.h"
#include "util/pointer.h"
#include "kvstore.h"

namespace cache {

/**
 * ...
 * TODO(jblomer): save open file table for hotpatch
 */
class RamCacheManager : public CacheManager {
 public:
  virtual CacheManagerIds id() { return kRamCacheManager; }

  RamCacheManager(uint64_t max_size, unsigned max_entries, perf::Statistics *statistics)
    : max_size(max_size)
    , invalid_fd_(shash::Any())
    , pinned_entries_(max_entries/3, statistics)
    , regular_entries_(max_entries/3, statistics)
    , volatile_entries_(max_entries/3, statistics) {
    int retval = pthread_rwlock_init(&rwlock_, NULL);
    assert(retval == 0);
  }
  virtual ~RamCacheManager() {
    pthread_rwlock_destroy(&rwlock_);
  }
  virtual bool AcquireQuotaManager(QuotaManager *quota_mgr);

  virtual int Open(const shash::Any &id);
  virtual int64_t GetSize(int fd);
  virtual int Close(int fd);
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset);
  virtual int Dup(int fd);
  virtual int Readahead(int fd);

  virtual uint16_t SizeOfTxn() { return sizeof(Transaction); }
  virtual int StartTxn(const shash::Any &id, uint64_t size, void *txn);
  virtual void CtrlTxn(const std::string &description,
                       const ObjectType type,
                       const int flags,
                       void *txn);
  virtual int64_t Write(const void *buf, uint64_t size, void *txn);
  virtual int Reset(void *txn);
  virtual int OpenFromTxn(void *txn);
  virtual int AbortTxn(void *txn);
  virtual int CommitTxn(void *txn);

 private:
  struct ReadOnlyFd {
    ReadOnlyFd() : handle(shash::Any()), pos(0) { }
    ReadOnlyFd(const shash::Any &h, uint64_t pos) : handle(h), pos(pos) { }
    shash::Any handle;
    uint64_t pos;
  };

  struct Transaction {
    Transaction() :
      buffer(NULL), size(0), expected_size(0), pos(0), object_type(kTypeRegular) { }
    shash::Any id;
    void *buffer;
    uint64_t size;
    uint64_t expected_size;
    uint64_t pos;
    ObjectType object_type;
    std::string description;
  };

  inline bool IsValid(int fd) {
    if ((fd < 0) || (static_cast<unsigned>(fd) >= open_fds_.size()))
      return false;
    return open_fds_[fd].handle != invalid_fd_;
  }

  int AddFd(const ReadOnlyFd &fd);
  int64_t CommitToKvStore(Transaction *transaction);
  virtual int DoOpen(const shash::Any &id);

  uint64_t max_size;
  shash::Any invalid_fd_;
  std::vector<ReadOnlyFd> open_fds_;
  pthread_rwlock_t rwlock_;
  kvstore::MemoryKvStore pinned_entries_;
  kvstore::MemoryKvStore regular_entries_;
  kvstore::MemoryKvStore volatile_entries_;
};  // class RamCacheManager

}  // namespace cache

#endif  // CVMFS_CACHE_RAM_H_
