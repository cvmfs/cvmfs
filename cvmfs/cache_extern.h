/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_CACHE_EXTERN_H_
#define CVMFS_CACHE_EXTERN_H_

#include "cache.h"

namespace cache {

class ExternalCacheManager : public CacheManager {
 public:
  ExternalCacheManager();
  virtual ~ExternalCacheManager();

  virtual CacheManagerIds id() { return kExternalCacheManager; }
  virtual bool AcquireQuotaManager(QuotaManager *quota_mgr);

  virtual int Open(const shash::Any &id);
  virtual int64_t GetSize(int fd);
  virtual int Close(int fd);
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset);
  virtual int Dup(int fd);
  virtual int Readahead(int fd);

  virtual uint16_t SizeOfTxn();
  virtual int StartTxn(const shash::Any &id, uint64_t size, void *txn);
  virtual int ControlTxn(const std::string &description,
                         const ObjectType type,
                         const int flags,
                         void *txn);
  virtual int64_t Write(const void *buf, uint64_t sz, void *txn);
  virtual int Reset(void *txn);
  virtual int AbortTxn(void *txn);
  virtual int OpenFromTxn(void *txn);
  virtual int CommitTxn(void *txn);

 private:
};  // class ExternalCacheManager

}  // namespace cache

#endif  // CVMFS_CACHE_EXTERN_H_
