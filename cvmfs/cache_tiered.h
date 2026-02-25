/**
 * This file is part of the CernVM File System.
 *
 * This provides a framework for a cache with an upper and a lower branch.
 */

#ifndef CVMFS_CACHE_TIERED_H_
#define CVMFS_CACHE_TIERED_H_

#include <string>

#include "cache.h"
#include "duplex_testing.h"

/**
 * Cache manager implementation that provides a hierarchical cache.
 * Given an "upper" and "lower" cache manager object:
 * - Reads are done first from the upper cache
 * - On upper cache miss, then this tries the lower cache.
 *   If there's a lower cache hit, then the file is written
 *   to the upper cache.
 * - Writes are done to both caches simultaneously.
 *
 * The quota manager is only applied to the upper cache.
 */
class TieredCacheManager : public CacheManager {
  FRIEND_TEST(T_MountPoint, TieredCacheMgr);
  FRIEND_TEST(T_MountPoint, TieredComplex);

 public:
  virtual CacheManagerIds id() { return kTieredCacheManager; }
  virtual std::string Describe();

  static CacheManager *Create(CacheManager *upper_cache,
                              CacheManager *lower_cache);
  void SetLowerReadOnly() { lower_readonly_ = true; }

  virtual ~TieredCacheManager();
  virtual bool AcquireQuotaManager(QuotaManager *quota_mgr) {
    const bool result = upper_->AcquireQuotaManager(quota_mgr);
    quota_mgr_ = upper_->quota_mgr();
    return result;
  }

  virtual int Open(const LabeledObject &object);
  virtual int64_t GetSize(int fd) { return upper_->GetSize(fd); }
  virtual int Close(int fd) { return upper_->Close(fd); }
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset) {
    return upper_->Pread(fd, buf, size, offset);
  }
  virtual int Dup(int fd) { return upper_->Dup(fd); }
  virtual int Readahead(int fd) { return upper_->Readahead(fd); }

  virtual uint32_t SizeOfTxn() {
    return upper_->SizeOfTxn() + lower_->SizeOfTxn();
  }
  virtual int StartTxn(const shash::Any &id, uint64_t size, void *txn);
  virtual void CtrlTxn(const Label &label, const int flags, void *txn);
  virtual int64_t Write(const void *buf, uint64_t size, void *txn);
  virtual int Reset(void *txn);
  virtual int OpenFromTxn(void *txn) { return upper_->OpenFromTxn(txn); }
  virtual int AbortTxn(void *txn);
  virtual int CommitTxn(void *txn);
  virtual void Spawn();

  virtual manifest::Breadcrumb LoadBreadcrumb(const std::string &fqrn);
  virtual bool StoreBreadcrumb(const manifest::Manifest &manifest);

 protected:
  virtual void *DoSaveState();
  virtual int DoRestoreState(void *data);
  virtual bool DoFreeState(void *data);

 private:
  static const unsigned kCopyBufferSize = 64 * 1024;  // 64kB

  struct SavedState {
    SavedState() : state_upper(NULL), state_lower(NULL) { }
    void *state_upper;
    void *state_lower;
  };

  // NOTE: TieredCacheManager takes ownership of both caches passed.
  TieredCacheManager(CacheManager *upper_cache, CacheManager *lower_cache)
      : upper_(upper_cache), lower_(lower_cache), lower_readonly_(false) { }

  CacheManager *upper_;
  CacheManager *lower_;
  bool lower_readonly_;
};  // class TieredCacheManager

#endif  // CVMFS_CACHE_TIERED_H_
