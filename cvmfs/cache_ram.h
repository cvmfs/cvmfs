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

#include "cache.h"
#include "hash.h"
#include "util/pointer.h"

namespace cache {

class KvStore : SingleCopy {
 public:

};


/**
 * ...
 * TODO(jblomer): save open file table for hotpatch
 */
class RamCacheManager : public CacheManager {
 public:
  virtual CacheManagerIds id() { return kRamCacheManager; }

  RamCacheManager();
  virtual ~RamCacheManager();
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
    ReadOnlyFd() : handle(-1), size(0) { }
    ReadOnlyFd(int64_t h, uint64_t s) : handle(h), size(s) { }
    /**
     * Negative handle: invalid FD
     */
    int64_t handle;
    uint64_t size;
  };

  struct Transaction {
    Transaction() :
      buffer(NULL), size(0), expected_size(0), pos(0), handle(-1) { }
    shash::Any id;
    void *buffer;
    uint64_t size;
    uint64_t expected_size;
    uint64_t pos;
    int64_t handle;
  };

  inline bool IsValid(int fd) {
    if ((fd < 0) || (static_cast<unsigned>(fd) >= open_fds_.size()))
      return false;
    return open_fds_[fd].handle >= 0;
  }

  int AddFd(const ReadOnlyFd &fd);
  int64_t CommitToKvStore(Transaction *transaction);


  std::vector<ReadOnlyFd> open_fds_;
  pthread_rwlock_t rwlock_;
};  // class RamCacheManager

}  // namespace cache

#endif  // CVMFS_CACHE_RAM_H_
