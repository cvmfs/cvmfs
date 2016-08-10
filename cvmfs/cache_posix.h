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

#include "atomic.h"
#include "backoff.h"
#include "cache.h"
#include "catalog_mgr.h"
#include "file_chunk.h"
#include "gtest/gtest_prod.h"
#include "manifest_fetch.h"
#include "shortstring.h"
#include "signature.h"
#include "statistics.h"

namespace catalog {
class DirectoryEntry;
class Catalog;
}

namespace download {
class DownloadManager;
}

namespace cache {

/**
 * Cache manger implementation using a file system (cache directory) as a
 * backing storage.
 */
class PosixCacheManager : public CacheManager {
  FRIEND_TEST(T_CacheManager, CommitTxnQuotaNotifications);
  FRIEND_TEST(T_CacheManager, CommitTxnRenameFail);
  FRIEND_TEST(T_CacheManager, Open);
  FRIEND_TEST(T_CacheManager, OpenFromTxn);
  FRIEND_TEST(T_CacheManager, OpenPinned);
  FRIEND_TEST(T_CacheManager, Rename);
  FRIEND_TEST(T_CacheManager, StartTxn);
  FRIEND_TEST(T_CacheManager, TearDown2ReadOnly);

 public:
  enum CacheModes {
    kCacheReadWrite = 0,
    kCacheReadOnly,
  };

  /**
   * As of 25M, a file is considered a "big file", which means it is dangerous
   * to apply asynchronous semantics.  On start of a transaction with a big file
   * the cache is cleaned up opportunistically.
   */
  static const uint64_t kBigFile;

  virtual CacheManagerIds id() { return kPosixCacheManager; }

  static PosixCacheManager *Create(const std::string &cache_path,
                                   const bool alien_cache,
                                   const bool workaround_rename_ = false);
  virtual ~PosixCacheManager() { }
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

  void TearDown2ReadOnly();
  CacheModes cache_mode() { return cache_mode_; }
  bool alien_cache() { return alien_cache_; }
  std::string cache_path() { return cache_path_; }

 private:
  struct Transaction {
    Transaction(const shash::Any &id, const std::string &final_path)
      : buf_pos(0)
      , size(0)
      , expected_size(kSizeUnknown)
      , fd(-1)
      , type(kTypeRegular)
      , tmp_path()
      , final_path(final_path)
      , id(id)
    { }

    unsigned char buffer[4096];
    unsigned buf_pos;
    uint64_t size;
    uint64_t expected_size;
    int fd;
    ObjectType type;
    std::string tmp_path;
    std::string final_path;
    std::string description;
    shash::Any id;
  };

  PosixCacheManager(const std::string &cache_path, const bool alien_cache)
    : cache_path_(cache_path)
    , txn_template_path_(cache_path_ + "/txn/fetchXXXXXX")
    , alien_cache_(alien_cache)
    , workaround_rename_(false)
    , cache_mode_(kCacheReadWrite)
    , reports_correct_filesize_(true)
  {
    atomic_init32(&no_inflight_txns_);
  }

  std::string GetPathInCache(const shash::Any &id);
  int Rename(const char *oldpath, const char *newpath);
  int Flush(Transaction *transaction);

  std::string cache_path_;
  std::string txn_template_path_;
  bool alien_cache_;
  bool workaround_rename_;
  CacheModes cache_mode_;

  /**
   * The cache can only degrade to a read-only cache once all writable file
   * descriptors from transactions are closed.  This is indicated by a zero
   * value in this variable.
   */
  atomic_int32 no_inflight_txns_;

  /**
   * Hack for HDFS which writes file sizes asynchronously.
   */
  bool reports_correct_filesize_;
};  // class PosixCacheManager

}  // namespace cache

#endif  // CVMFS_CACHE_POSIX_H_
