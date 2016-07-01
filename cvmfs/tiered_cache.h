/**
 * This file is part of the CernVM File System.
 *
 * This provides a framework for a tiered cache implementation.
 */

#ifndef CVMFS_TIERED_CACHE_H_
#define CVMFS_TIERED_CACHE_H_

#include "cache.h"

namespace cache {

/**
 * Cache manager implementation that provides a hierarchical cache.
 * Given an "upper" and "lower" cache manager object:
 * - Reads are done first from the upper cache
 * - On upper cache miss, then this tries the lower cache.
 *   If there's a lower cache hit, then the file is written
 *   to the upper cache..
 * - Writes are done to both caches simultaneously.
 *
 * The quota manager is only applied to the upper cache.
 */
class TieredCacheManager : public CacheManager {

 public:

  virtual CacheManagerIds id() { return kTieredCacheManager; }

  static CacheManager *Create(CacheManager *upper_cache,
                              CacheManager *lower_cache)
  {return new TieredCacheManager(upper_cache, lower_cache);}

  virtual ~TieredCacheManager() {delete upper_; delete lower_;}
  virtual bool AcquireQuotaManager(QuotaManager *quota_mgr)
  {return upper_->AcquireQuotaManager(quota_mgr);}

  virtual int Open(const shash::Any &id);
  virtual int64_t GetSize(int fd) {return upper_->GetSize(fd);}
  virtual int Close(int fd) {return upper_->Close(fd);}
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset)
  {return upper_->Pread(fd, buf, size, offset);}
  virtual int Dup(int fd) {return upper_->Dup(fd);}
  virtual int Readahead(int fd) {return upper_->Readahead(fd);}

  virtual uint16_t SizeOfTxn() { return upper_->SizeOfTxn() + lower_->SizeOfTxn(); }
  virtual int StartTxn(const shash::Any &id, uint64_t size, void *txn);
  virtual void CtrlTxn(const std::string &description,
                       const ObjectType type,
                       const int flags,
                       void *txn);
  virtual int64_t Write(const void *buf, uint64_t size, void *txn);
  virtual int Reset(void *txn);
  virtual int OpenFromTxn(void *txn) {return  upper_->OpenFromTxn(txn);}
  virtual int AbortTxn(void *txn);
  virtual int CommitTxn(void *txn);

 private:

  // NOTE: TieredCacheManager takes ownership of both caches passed.
  TieredCacheManager(CacheManager *upper_cache,
                     CacheManager *lower_cache)
    : upper_(upper_cache),
      lower_(lower_cache)
  {}

  CacheManager *upper_;
  CacheManager *lower_;
};  // class TieredCacheManager

}  // namespace cache

#endif  // CVMFS_TIERED_CACHE_H_
