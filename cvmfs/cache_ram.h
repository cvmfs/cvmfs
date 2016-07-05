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
#include "kvstore.h"
#include "statistics.h"
#include "util/pointer.h"

namespace cache {

// The null hash (hashed output is all null bytes) serves as a marker for
// an invalid handle
static const shash::Any kInvalidHandle;

static const unsigned kMaxHandles = 8192;

/**
 * ...
 * TODO(jblomer): save open file table for hotpatch
 */
class RamCacheManager : public CacheManager {
 public:
  virtual CacheManagerIds id() { return kRamCacheManager; }

  RamCacheManager(
    uint64_t max_size,
    unsigned max_entries,
    perf::Statistics *statistics)
    : max_size_(max_size)
    , pinned_entries_(max_entries/3, "RamCache.pinned", statistics)
    , regular_entries_(max_entries/3, "RamCache.regular", statistics)
    , volatile_entries_(max_entries/3, "RamCache.volatile", statistics) {
    int retval = pthread_rwlock_init(&rwlock_, NULL);
    assert(retval == 0);
  }
  virtual ~RamCacheManager() {
    pthread_rwlock_destroy(&rwlock_);
  }
  virtual bool AcquireQuotaManager(QuotaManager *quota_mgr);

  /**
   * Open a new file descriptor into the cache. Note that opening entries effectively pins them in the cache,
   * so it may be necessary to close unneeded file descriptors to allow eviction to make room in the cache
   * @param id The hash key
   * @returns The smallest free integer file descriptor, or -ENOENT if the entry
   * is not in the cache, or -ENFILE if no handles are available
   */
  virtual int Open(const shash::Any &id);

  /**
   * Look up the size in bytes of the open cache entry
   * @param id The hash key
   * @returns The size of the entry, or -EBADFD if fd is not valid
   */
  virtual int64_t GetSize(int fd);

  /**
   * Close the descriptor in the cache. Entries not marked as pinned will become subject to
   * eviction once closed
   * @param id The hash key
   * @returns -EBADFD if fd is not valid
   */
  virtual int Close(int fd);

  /**
   * Read a section from the cache entry. See pread(3) for a discussion of the arguments
   * @param id The hash key
   * @returns The number of bytes copied, or -EBADFD if fd is not valid
   */
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset);

  /**
   * Duplicates the open file descriptor, allowing the original and the new one to be
   * used independently
   * @param id The hash key
   * @returns A new fd, -EBADFD if fd is not valid, or -ENFILE if no handles are available
   */
  virtual int Dup(int fd);

  /**
   * No effect for in-memory caches
   * @param id The hash key
   * @returns -EBADFD if fd is not valid
   */
  virtual int Readahead(int fd);

  /** Get the amount of space to be allocated for a call to @ref StartTxn */
  virtual uint16_t SizeOfTxn() { return sizeof(Transaction); }

  /**
   * Start a new transaction, allocating the required memory and initializing the transaction parameters
   * @param id The hash key
   * @param size The total size of the new cache entry
   * @param txn A pointer to space allocated for storing the transaction details
   */
  virtual int StartTxn(const shash::Any &id, uint64_t size, void *txn);

  /**
   * Set the transaction parameters. At present, only @ref type is used for pinned, volatile, etc.
   * @param description Unused
   * @param type The type of the entry, e.g. pinned
   * @param flags Unused
   * @param txn A pointer to space allocated for storing the transaction details
   */
  virtual void CtrlTxn(const std::string &description,
                       const ObjectType type,
                       const int flags,
                       void *txn);

  /**
   * Copy the given memory region into the transaction buffer. Copying starts at the transaction's current offset
   * @param buf The source address
   * @param size The number of bytes to copy
   * @param txn A pointer to space allocated for storing the transaction details
   */
  virtual int64_t Write(const void *buf, uint64_t size, void *txn);

  /**
   * Seek to the beginning of the transaction buffer
   * @param txn A pointer to space allocated for storing the transaction details
   */
  virtual int Reset(void *txn);

  /**
   * Commit a transaction and open the resulting cache entry. This is necessary to avoid a race condition in which the
   * cache entry is evicted between the calls to CommitTxn and Open.
   * @param txn A pointer to space allocated for storing the transaction details
   * @returns A file descriptor to the new cache entry, or a negative error from @ref CommitTxn
   */
  virtual int OpenFromTxn(void *txn);

  /**
   * Free the resources allocated for the pending transaction
   * @param txn A pointer to space allocated for storing the transaction details
   */
  virtual int AbortTxn(void *txn);

   /**
   * Commit a transaction to the cache. If there is not enough free space in the cache, first try to make room by evicting
   * volatile entries. If there is still not enough room, try evicting regular entries. If there is *still* not enough
   * space, give up an return failure. Note that evictions only occur if they will produce enough space for the transaction;
   * if -ENOSPC is returned, the states of the transaction and cache are unchanged.
   * @param txn A pointer to space allocated for storing the transaction details
   * @returns -ENOSPC if the transaction would exceed the size of the cache, or -ENFILE if no handles are available
   */
  virtual int CommitTxn(void *txn);

 private:
  struct ReadOnlyFd {
    ReadOnlyFd() : handle(shash::Any()), pos(0) { }
    ReadOnlyFd(const shash::Any &h, uint64_t pos) : handle(h), pos(pos) { }
    shash::Any handle;
    uint64_t pos;
  };

  struct Transaction {
    Transaction()
      : buffer(NULL)
      , size(0)
      , expected_size(0)
      , pos(0)
      , object_type(kTypeRegular) { }
    shash::Any id;
    void *buffer;
    uint64_t size;
    uint64_t expected_size;
    uint64_t pos;
    ObjectType object_type;
    std::string description;
  };

  inline bool IsValid(int fd) {
    if ((fd < 0) || (static_cast<unsigned>(fd) >= open_fds_.size()))
      return false;
    return open_fds_[fd].handle != kInvalidHandle;
  }

  int AddFd(const ReadOnlyFd &fd);
  int64_t CommitToKvStore(Transaction *transaction);
  virtual int DoOpen(const shash::Any &id);

  uint64_t max_size_;
  std::vector<ReadOnlyFd> open_fds_;
  pthread_rwlock_t rwlock_;
  kvstore::MemoryKvStore pinned_entries_;
  kvstore::MemoryKvStore regular_entries_;
  kvstore::MemoryKvStore volatile_entries_;
};  // class RamCacheManager

}  // namespace cache

#endif  // CVMFS_CACHE_RAM_H_
