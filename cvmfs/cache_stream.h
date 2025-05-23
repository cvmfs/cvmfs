/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CACHE_STREAM_H_
#define CVMFS_CACHE_STREAM_H_

#include <pthread.h>

#include <string>

#include "cache.h"
#include "crypto/hash.h"
#include "fd_table.h"
#include "ring_buffer.h"
#include "smallhash.h"
#include "util/pointer.h"

namespace download {
class DownloadManager;
}
namespace perf {
class Counter;
class Statistics;
}  // namespace perf

/**
 * Cache manager that streams regular files using a download manager and stores
 * file catalogs in an underlying cache manager.
 */
class StreamingCacheManager : public CacheManager {
 public:
  static const size_t kDefaultBufferSize;

  struct Counters {
    perf::Counter *sz_transferred_bytes;
    perf::Counter *sz_transfer_ms;
    perf::Counter *n_downloads;
    perf::Counter *n_buffer_hits;
    perf::Counter *n_buffer_evicts;
    perf::Counter *n_buffer_objects;
    perf::Counter *n_buffer_obstacles;

    explicit Counters(perf::Statistics *statistics);
  };

  StreamingCacheManager(unsigned max_open_fds,
                        CacheManager *cache_mgr,
                        download::DownloadManager *regular_download_mgr,
                        download::DownloadManager *external_download_mgr,
                        size_t buffer_size,
                        perf::Statistics *statistics);
  virtual ~StreamingCacheManager();

  // In the files system / mountpoint initialization, we create the cache
  // manager before we know about the download manager.  Hence we allow to
  // patch in the download manager at a later point.
  void SetRegularDownloadManager(download::DownloadManager *download_mgr) {
    regular_download_mgr_ = download_mgr;
  }
  void SetExternalDownloadManager(download::DownloadManager *download_mgr) {
    external_download_mgr_ = download_mgr;
  }

  virtual CacheManagerIds id() { return kStreamingCacheManager; }
  virtual std::string Describe();

  virtual bool AcquireQuotaManager(QuotaManager *quota_mgr);

  virtual int Open(const LabeledObject &object);
  virtual int64_t GetSize(int fd);
  virtual int Close(int fd);
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset);
  virtual int Dup(int fd);
  virtual int Readahead(int fd);

  // Only pinned objects and catalogs are written to the cache. Transactions
  // are passed through to the backing cache manager.
  virtual uint32_t SizeOfTxn() { return cache_mgr_->SizeOfTxn(); }
  virtual int StartTxn(const shash::Any &id, uint64_t size, void *txn) {
    return cache_mgr_->StartTxn(id, size, txn);
  }
  virtual void CtrlTxn(const Label &label, const int flags, void *txn) {
    cache_mgr_->CtrlTxn(label, flags, txn);
  }
  virtual int64_t Write(const void *buf, uint64_t size, void *txn) {
    return cache_mgr_->Write(buf, size, txn);
  }
  virtual int Reset(void *txn) { return cache_mgr_->Reset(txn); }
  virtual int OpenFromTxn(void *txn);
  virtual int AbortTxn(void *txn) { return cache_mgr_->AbortTxn(txn); }
  virtual int CommitTxn(void *txn) { return cache_mgr_->CommitTxn(txn); }

  virtual void Spawn() { cache_mgr_->Spawn(); }

  virtual manifest::Breadcrumb LoadBreadcrumb(const std::string &fqrn) {
    return cache_mgr_->LoadBreadcrumb(fqrn);
  }
  virtual bool StoreBreadcrumb(const manifest::Manifest &manifest) {
    return cache_mgr_->StoreBreadcrumb(manifest);
  }

  // Used in cvmfs' RestoreState to switch back from the streaming to the
  // regular cache manager. At this point, the streaming cache manager has
  // opened the root file catalog. We need to return the file descriptor in
  // the wrapped cache manager, too.
  CacheManager *MoveOutBackingCacheMgr(int *root_fd);
  // Used in cvmfs' RestoreState to create a virtual file descriptor for the
  // root catalog fd, that has been already opened in the backing cache manager
  int PlantFd(int fd_in_cache_mgr);

  const Counters &counters() const { return *counters_; }

 protected:
  virtual void *DoSaveState();
  virtual int DoRestoreState(void *data);
  virtual bool DoFreeState(void *data);

 private:
  struct FdInfo {
    int fd_in_cache_mgr;
    shash::Any object_id;
    CacheManager::Label label;

    FdInfo() : fd_in_cache_mgr(-1) { }
    explicit FdInfo(int fd) : fd_in_cache_mgr(fd) { }
    explicit FdInfo(const CacheManager::LabeledObject &object)
        : fd_in_cache_mgr(-1), object_id(object.id), label(object.label) { }

    bool operator==(const FdInfo &other) const {
      return this->fd_in_cache_mgr == other.fd_in_cache_mgr
             && this->object_id == other.object_id;
    }
    bool operator!=(const FdInfo &other) const { return !(*this == other); }

    bool IsValid() const { return fd_in_cache_mgr >= 0 || !object_id.IsNull(); }
  };

  struct SavedState {
    SavedState() : version(0), fd_table(NULL), state_backing_cachemgr(NULL) { }
    unsigned int version;
    FdTable<FdInfo> *fd_table;
    void *state_backing_cachemgr;
  };

  /// Depending on info.flags, selects either the regular or the external
  /// download manager
  download::DownloadManager *SelectDownloadManager(const FdInfo &info);

  /// Streams an object using the download manager. The complete object is read
  /// and its size is returned (-errno on error).
  /// The given section of the object is copied into the provided buffer,
  /// which may be NULL if only the size of the object is relevant.
  int64_t Stream(const FdInfo &info, void *buf, uint64_t size, uint64_t offset);

  UniquePtr<CacheManager> cache_mgr_;
  download::DownloadManager *regular_download_mgr_;
  download::DownloadManager *external_download_mgr_;

  pthread_mutex_t *lock_fd_table_;
  FdTable<FdInfo> fd_table_;

  /// A small in-memory cache to avoid frequent re-downloads if multiple blocks
  /// from the same chunk are read
  UniquePtr<RingBuffer> buffer_;
  SmallHashDynamic<shash::Any, RingBuffer::ObjectHandle_t> buffered_objects_;
  pthread_mutex_t *lock_buffer_;

  UniquePtr<Counters> counters_;
};  // class StreamingCacheManager

#endif  // CVMFS_CACHE_STREAM_H_
