/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CACHE_H_
#define CVMFS_CACHE_H_

#include <stdint.h>

#include <string>

#include "util/pointer.h"

namespace shash {
struct Any;
}

class QuotaManager;

namespace cache {

enum CacheManagerIds {
  kUnknownCacheManager = 0,
  kPosixCacheManager,
  kRamCacheManager,
};

enum CacheModes {
  kCacheReadWrite = 0,
  kCacheReadOnly,
};


/**
 * The Cache Manager provides (virtual) file descriptors to content-addressable
 * objects in the cache.  The implementation can use a POSIX file system or
 * other means such as a key-value store.  A file descriptor must remain
 * readable until closed, no matter if it is removed from the backend storage
 * or not (POSIX semantics).
 *
 * Writing into the cache is streamed and transactional: a file descriptor must
 * be acquired from StartTxn and the object is only visible in the cache after
 * CommitTxn.  The state of the transaction is carried in an opque transaction
 * object, which needs to be provided by the caller.  The size of the object is
 * returned by SizeOfTxn.  This way, users of derived classes can take care of
 * the storage allocation (e.g. on the stack), while the derived class
 * determines the contents of the transaction object.  For race-free read access
 * to objects that are just being written to the cache, the OpenFromTxn routine
 * is used just before the transaction is committed.
 *
 * Writing to the cache can be coupled to a quota manager.  The quota manager
 * maintains some extra information for data chunks: whether they are
 * volatile, whether they are pinned, and a description (usually the path that
 * corresponds to a data chunk).  By default the NoopQuotaManager is used, which
 * ignores all this extra information.  The CtrlTxn() function is used to
 * specify this extra information sometime between StartTxn() and CommitTxn().
 *
 * The integer return values can be negative and, in this case, represent a
 * -errno failure code.  Otherwise the return value 0 indicates a success, or
 * >= 0 for a file descriptor.
 */
class CacheManager : SingleCopy {
 public:
  /**
   * Sizes of objects should be known for StartTxn().  For file catalogs we
   * cannot ensure that, however, because the size field for nested catalogs
   * was only recently added.
   */
  static const uint64_t kSizeUnknown;

  /**
   * Relevant for the quota management
   */
  enum ObjectType {
    kTypeRegular = 0,
    kTypeCatalog,  // implies pinned
    kTypePinned,
    kTypeVolatile,
  };

  virtual CacheManagerIds id() = 0;

  virtual bool AcquireQuotaManager(QuotaManager *quota_mgr) = 0;

  virtual ~CacheManager();
  virtual int Open(const shash::Any &id) = 0;
  virtual int64_t GetSize(int fd) = 0;
  virtual int Close(int fd) = 0;
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset) = 0;
  virtual int Dup(int fd) = 0;
  virtual int Readahead(int fd) = 0;

  virtual uint16_t SizeOfTxn() = 0;
  virtual int StartTxn(const shash::Any &id, uint64_t size, void *txn) = 0;
  virtual void CtrlTxn(const std::string &description,
                       const ObjectType type,
                       const int flags,  // reserved for future use
                       void *txn) = 0;
  virtual int64_t Write(const void *buf, uint64_t sz, void *txn) = 0;
  virtual int Reset(void *txn) = 0;
  virtual int AbortTxn(void *txn) = 0;
  virtual int OpenFromTxn(void *txn) = 0;
  virtual int CommitTxn(void *txn) = 0;

  int OpenPinned(const shash::Any &id,
                 const std::string &description,
                 bool is_catalog);
  int ChecksumFd(int fd, shash::Any *id);
  bool Open2Mem(const shash::Any &id, unsigned char **buffer, uint64_t *size);
  bool CommitFromMem(const shash::Any &id,
                     const unsigned char *buffer,
                     const uint64_t size,
                     const std::string &description);

  QuotaManager *quota_mgr() { return quota_mgr_; }

 protected:
  CacheManager();

  /**
   * Never NULL but defaults to NoopQuotaManager.
   */
  QuotaManager *quota_mgr_;
};  // class CacheManager

}  // namespace cache

#endif  // CVMFS_CACHE_H_
