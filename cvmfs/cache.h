/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CACHE_H_
#define CVMFS_CACHE_H_

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <stdint.h>

#include <string>

#include "hash.h"
#include "manifest.h"
#include "util/pointer.h"


class QuotaManager;

enum CacheManagerIds {
  kUnknownCacheManager = 0,
  kPosixCacheManager,
  kRamCacheManager,
  kTieredCacheManager,
  kExternalCacheManager,
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
 * CommitTxn.  The state of the transaction is carried in an opaque transaction
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

  /**
   * Meta-data of an object that the cache may or may not maintain.  Good cache
   * implementations should at least distinguish between volatile and regular
   * objects.
   */
  struct ObjectInfo {
    ObjectInfo() : type(kTypeRegular), description() { }
    ObjectInfo(ObjectType t, const std::string &d) : type(t), description(d) { }

    ObjectType type;
    /**
     * Typically the path that triggered storing the object in the cache
     */
    std::string description;
  };

  /**
   * A content hash together with a (partial) ObjectInfo meta-data.
   */
  struct BlessedObject {
    explicit BlessedObject(const shash::Any &id) : id(id), info() { }
    BlessedObject(const shash::Any &id, const ObjectInfo info)
      : id(id)
      , info(info) { }
    BlessedObject(const shash::Any &id, ObjectType type)
      : id(id)
      , info(type, "") { }
    BlessedObject(const shash::Any &id, const std::string &description)
      : id(id)
      , info(kTypeRegular, description) { }
    BlessedObject(
      const shash::Any &id,
      ObjectType type,
      const std::string &description)
      : id(id)
      , info(type, description) { }

    shash::Any id;
    ObjectInfo info;
  };
  // Convenience constructors, users can call Open(CacheManager::Bless(my_hash))
  static inline BlessedObject Bless(const shash::Any &id) {
    return BlessedObject(id);
  }
  static inline BlessedObject Bless(
    const shash::Any &id,
    const ObjectInfo &info)
  {
    return BlessedObject(id, info);
  }
  static inline BlessedObject Bless(const shash::Any &id, ObjectType type) {
    return BlessedObject(id, type);
  }
  static inline BlessedObject Bless(
    const shash::Any &id,
    const std::string &description)
  {
    return BlessedObject(id, description);
  }
  static inline BlessedObject Bless(
    const shash::Any &id,
    ObjectType type,
    const std::string &description)
  {
    return BlessedObject(id, type, description);
  }

  virtual CacheManagerIds id() = 0;
  /**
   * Return a human readable description of the cache instance.  Used in
   * cvmfs_talk.
   */
  virtual std::string Describe() = 0;

  virtual bool AcquireQuotaManager(QuotaManager *quota_mgr) = 0;

  virtual ~CacheManager();
  /**
   * Opening an object might get it from a third-party source, e.g. when the
   * tiered cache manager issues a copy-up operation.  In this case it is
   * beneficial to register the object with the accurate meta-data, in the same
   * way it is done during transactions.
   */
  virtual int Open(const BlessedObject &object) = 0;
  virtual int64_t GetSize(int fd) = 0;
  virtual int Close(int fd) = 0;
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset) = 0;
  virtual int Dup(int fd) = 0;
  virtual int Readahead(int fd) = 0;

  virtual uint32_t SizeOfTxn() = 0;
  virtual int StartTxn(const shash::Any &id, uint64_t size, void *txn) = 0;
  virtual void CtrlTxn(const ObjectInfo &object_info,
                       const int flags,  // reserved for future use
                       void *txn) = 0;
  virtual int64_t Write(const void *buf, uint64_t sz, void *txn) = 0;
  virtual int Reset(void *txn) = 0;
  virtual int AbortTxn(void *txn) = 0;
  virtual int OpenFromTxn(void *txn) = 0;
  virtual int CommitTxn(void *txn) = 0;

  virtual void Spawn() = 0;

  int OpenPinned(const shash::Any &id,
                 const std::string &description,
                 bool is_catalog);
  int ChecksumFd(int fd, shash::Any *id);
  bool Open2Mem(const shash::Any &id, const std::string &description,
                unsigned char **buffer, uint64_t *size);
  bool CommitFromMem(const shash::Any &id,
                     const unsigned char *buffer,
                     const uint64_t size,
                     const std::string &description);

  QuotaManager *quota_mgr() { return quota_mgr_; }

  // Rescue the open file table during reload of the fuse module.  For the
  // POSIX cache, nothing needs to be done because the table is keep in the
  // kernel for the process.  Other cache managers need to do it manually.
  void *SaveState(const int fd_progress);
  /**
   * When RestoreState is called, the cache has already exactly one file
   * descriptor open: the root file catalog. This file descriptor might be
   * remapped to another number. A return value of -1 means no action needs
   * to take place. A smaller value indicates an error.
   */
  int RestoreState(const int fd_progress, void *state);
  void FreeState(const int fd_progress, void *state);

  /**
   * While not strictly necessary, cache managers often have a directory
   * associated with them. This directory is currently used to find the
   * cached manifest copy, the cvmfschecksum.$reponame file. This is important
   * to make pre-loaded alien caches work, even in a tiered setup.
   */
  virtual manifest::Breadcrumb LoadBreadcrumb(const std::string & /*fqrn*/) {
    return manifest::Breadcrumb();
  }
  virtual bool StoreBreadcrumb(const manifest::Manifest &/*manifest*/) {
    return false;
  }

 protected:
  CacheManager();

  // Unless overridden, Saving/Restoring states will crash the Fuse module
  virtual void *DoSaveState() { return NULL; }
  virtual int DoRestoreState(void *data) { return false; }
  virtual bool DoFreeState(void *data) { return false; }

  /**
   * Never NULL but defaults to NoopQuotaManager.
   */
  QuotaManager *quota_mgr_;

 private:
  static const unsigned kStateVersion = 0;

  /**
   * Wraps around the concrete cache manager's state block in memory.  The
   * state pointer is used in DoSaveState, DoRestoreState, DoFreeState.
   */
  struct State : SingleCopy {
    State()
      : version(kStateVersion)
      , manager_type(kUnknownCacheManager)
      , concrete_state(NULL)
    { }

    unsigned version;
    CacheManagerIds manager_type;
    void *concrete_state;
  };
};  // class CacheManager

#endif  // CVMFS_CACHE_H_
