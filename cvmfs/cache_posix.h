/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CACHE_POSIX_H_
#define CVMFS_CACHE_POSIX_H_

#include <stdint.h>
#include <sys/types.h>

#include <map>
#include <string>
#include <vector>

#include "backoff.h"
#include "cache.h"
#include "catalog_mgr.h"
#include "crypto/signature.h"
#include "duplex_testing.h"
#include "fd_refcount_mgr.h"
#include "file_chunk.h"
#include "manifest_fetch.h"
#include "shortstring.h"
#include "statistics.h"
#include "util/atomic.h"

namespace catalog {
class DirectoryEntry;
class Catalog;
}  // namespace catalog

namespace download {
class DownloadManager;
}

/**
 * Cache manager implementation using a file system (cache directory) as a
 * backing storage.
 */
class ListOpenHashesMagicXattr;  // FD needed to access fd_mgr_
class PosixCacheManager : public CacheManager {
  FRIEND_TEST(T_CacheManager, CommitTxnQuotaNotifications);
  FRIEND_TEST(T_CacheManager, CommitTxnRenameFail);
  FRIEND_TEST(T_CacheManager, Open);
  FRIEND_TEST(T_CacheManager, OpenFromTxn);
  FRIEND_TEST(T_CacheManager, OpenPinned);
  FRIEND_TEST(T_CacheManager, Rename);
  FRIEND_TEST(T_CacheManager, StartTxn);
  FRIEND_TEST(T_CacheManager, TearDown2ReadOnly);
  friend class ListOpenHashesMagicXattr;

 public:
  enum CacheModes {
    kCacheReadWrite = 0,
    kCacheReadOnly,
  };

  enum RenameWorkarounds {
    kRenameNormal = 0,
    kRenameLink,
    kRenameSamedir
  };

  /**
   * As of 25M, a file is considered a "big file", which means it is dangerous
   * to apply asynchronous semantics.  On start of a transaction with a big file
   * the cache is cleaned up opportunistically.
   */
  static const uint64_t kBigFile;

  virtual CacheManagerIds id() { return kPosixCacheManager; }
  virtual std::string Describe();

  static PosixCacheManager *Create(
      const std::string &cache_path, const bool alien_cache,
      const RenameWorkarounds rename_workaround = kRenameNormal,
      const bool do_refcount = true, const bool cleanup_unused_first = false);
  virtual ~PosixCacheManager() { }
  virtual bool AcquireQuotaManager(QuotaManager *quota_mgr);

  virtual int Open(const LabeledObject &object);
  virtual int64_t GetSize(int fd);
  virtual int Close(int fd);
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset);
  virtual int Dup(int fd);
  virtual int Readahead(int fd);

  virtual uint32_t SizeOfTxn() { return sizeof(Transaction); }
  virtual int StartTxn(const shash::Any &id, uint64_t size, void *txn);
  virtual void CtrlTxn(const Label &label, const int flags, void *txn);
  virtual int64_t Write(const void *buf, uint64_t size, void *txn);
  virtual int Reset(void *txn);
  virtual int OpenFromTxn(void *txn);
  virtual int AbortTxn(void *txn);
  virtual int CommitTxn(void *txn);

  virtual void Spawn() { }

  virtual manifest::Breadcrumb LoadBreadcrumb(const std::string &fqrn);
  virtual bool StoreBreadcrumb(const manifest::Manifest &manifest);
  bool StoreBreadcrumb(std::string fqrn, manifest::Breadcrumb breadcrumb);

  void TearDown2ReadOnly();
  CacheModes cache_mode() { return cache_mode_; }
  bool alien_cache() { return alien_cache_; }
  std::string cache_path() { return cache_path_; }
  bool is_tmpfs() { return is_tmpfs_; }
  bool do_refcount() const { return do_refcount_; }
  bool cleanup_unused_first() const { return cleanup_unused_first_; }

 protected:
  virtual void *DoSaveState();
  virtual int DoRestoreState(void *data);
  virtual bool DoFreeState(void *data);

 private:
  bool InitCacheDirectory(const string &cache_path);

  struct Transaction {
    Transaction(const shash::Any &id, const std::string &final_path)
        : buf_pos(0)
        , size(0)
        , expected_size(kSizeUnknown)
        , fd(-1)
        , label()
        , tmp_path()
        , final_path(final_path)
        , id(id) { }

    unsigned char buffer[4096];
    unsigned buf_pos;
    uint64_t size;
    uint64_t expected_size;
    int fd;
    Label label;
    std::string tmp_path;
    std::string final_path;
    shash::Any id;
  };

  PosixCacheManager(const std::string &cache_path, const bool alien_cache,
                    const bool do_refcount = true,
                    const bool cleanup_unused_first = false)
      : cache_path_(cache_path)
      , txn_template_path_(cache_path_ + "/txn/fetchXXXXXX")
      , alien_cache_(alien_cache)
      , rename_workaround_(kRenameNormal)
      , cache_mode_(kCacheReadWrite)
      , reports_correct_filesize_(true)
      , is_tmpfs_(false)
      , do_refcount_(do_refcount)
      , fd_mgr_(new FdRefcountMgr())
      , cleanup_unused_first_(cleanup_unused_first) {
    atomic_init32(&no_inflight_txns_);
  }

  std::string GetPathInCache(const shash::Any &id);
  int Rename(const char *oldpath, const char *newpath);
  int Flush(Transaction *transaction);


  std::string cache_path_;
  std::string txn_template_path_;
  bool alien_cache_;
  RenameWorkarounds rename_workaround_;
  CacheModes cache_mode_;

  /**
   * The cache can only degrade to a read-only cache once all writable file
   * descriptors from transactions are closed.  This is indicated by a zero
   * value in this variable.
   */
  atomic_int32 no_inflight_txns_;

  static const char kMagicRefcount = 123;
  static const char kMagicNoRefcount = '\0';
  struct SavedState {
    SavedState() : magic_number(kMagicRefcount), version(0), fd_mgr(NULL) { }
    /// this helps to distinguish from the SavedState of the normal
    /// posix cache manager
    char magic_number;
    unsigned int version;
    UniquePtr<FdRefcountMgr> fd_mgr;
  };

  /**
   * Hack for HDFS which writes file sizes asynchronously.
   */
  bool reports_correct_filesize_;

  /**
   * True if posixcache is on tmpfs (and with this already in RAM)
   */
  bool is_tmpfs_;
  /**
   * Refcount and return only unique file descriptors
   */
  bool do_refcount_;
  UniquePtr<FdRefcountMgr> fd_mgr_;

  bool cleanup_unused_first_;
};  // class PosixCacheManager

#endif  // CVMFS_CACHE_POSIX_H_

