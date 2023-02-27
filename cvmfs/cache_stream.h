/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CACHE_STREAM_H_
#define CVMFS_CACHE_STREAM_H_

#include "cache.h"

#include <pthread.h>

#include <string>

#include <crypto/hash.h>
#include <smallhash.h>
#include <util/pointer.h>

namespace download {
class DownloadManager;
}

/**
 * Cache manager that streams regular files using a download manager and stores
 * file catalogs in an underlying cache manager.
 */
class StreamingCacheManager : public CacheManager {
 public:
  StreamingCacheManager(CacheManager *cache_mgr,
                        download::DownloadManager *download_mgr);
  virtual ~StreamingCacheManager();

  virtual CacheManagerIds id() { return kStreamingCacheManager; }
  virtual std::string Describe();

  virtual bool AcquireQuotaManager(QuotaManager *quota_mgr);

  virtual int Open(const BlessedObject &object);
  virtual int64_t GetSize(int fd);
  virtual int Close(int fd);
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset);
  virtual int Dup(int fd);
  virtual int Readahead(int fd);

  virtual void Spawn() { cache_mgr_->Spawn(); }

  virtual manifest::Breadcrumb LoadBreadcrumb(const std::string &fqrn) {
    return cache_mgr_->LoadBreadcrumb(fqrn);
  }
  virtual bool StoreBreadcrumb(const manifest::Manifest &manifest) {
    return cache_mgr_->StoreBreadcrumb(manifest);
  }

 protected:

 private:
  static const uint64_t kMinStreamSize = 64 * 1024;
  struct FdInfo {
    int fd_in_cache_mgr;
    shash::Any object_id;

    FdInfo() : fd_in_cache_mgr(-1) {}
    explicit FdInfo(int fd) : fd_in_cache_mgr(fd) {}
    explicit FdInfo(const shash::Any &id)
      : fd_in_cache_mgr(-1), object_id(id) {}
  };

  UniquePtr<CacheManager> cache_mgr_;
  download::DownloadManager *download_mgr_;
  uint64_t min_stream_size_;

  pthread_mutex_t *lock_fd_table_;
  SmallHashDynamic<int, FdInfo> fd_table_;
};  // class StreamingCacheManager

#endif  // CVMFS_CACHE_STREAM_H_
