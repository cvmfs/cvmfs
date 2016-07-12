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
  struct Counters {
    perf::Counter *n_addfd;
    perf::Counter *n_acquire;
    perf::Counter *n_open;
    perf::Counter *n_getsize;
    perf::Counter *n_close;
    perf::Counter *n_pread;
    perf::Counter *n_dup;
    perf::Counter *n_readahead;
    perf::Counter *n_sizeoftxn;
    perf::Counter *n_starttxn;
    perf::Counter *n_ctrltxn;
    perf::Counter *n_write;
    perf::Counter *n_reset;
    perf::Counter *n_openfromtxn;
    perf::Counter *n_aborttxn;
    perf::Counter *n_committxn;
    perf::Counter *n_committokvstore;
    perf::Counter *n_reusefd;
    perf::Counter *n_appendfd;
    perf::Counter *n_enfile;
    perf::Counter *n_openregular;
    perf::Counter *n_openvolatile;
    perf::Counter *n_openmiss;
    perf::Counter *n_closesweep;
    perf::Counter *n_overrun;
    perf::Counter *n_full;
    perf::Counter *n_realloc;
    perf::Counter *sz_alloc;
    perf::Counter *sz_committed;

    Counters(perf::Statistics *statistics, const std::string &name) {
      n_acquire = statistics->Register(name + ".n_acquire",
        "Number of AcquireQuotaManager calls for " + name);
      n_addfd = statistics->Register(name + ".n_addfd",
        "Number of AddFd calls for " + name);
      n_committokvstore = statistics->Register(name + ".n_committokvstore",
        "Number of CommitToKvStore calls for " + name);
      n_open = statistics->Register(name + ".n_open",
        "Number of Open calls for " + name);
      n_getsize = statistics->Register(name + ".n_getsize",
        "Number of GetSize calls for " + name);
      n_close = statistics->Register(name + ".n_close",
        "Number of Close calls for " + name);
      n_pread = statistics->Register(name + ".n_pread",
        "Number of Pread calls for " + name);
      n_dup = statistics->Register(name + ".n_dup",
        "Number of Dup calls for " + name);
      n_readahead = statistics->Register(name + ".n_readahead",
        "Number of ReadAhead calls for " + name);
      n_sizeoftxn = statistics->Register(name + ".n_sizeoftxn",
        "Number of SizeOfTxn calls for " + name);
      n_starttxn = statistics->Register(name + ".n_starttxn",
        "Number of StartTxn calls for " + name);
      n_ctrltxn = statistics->Register(name + ".n_ctrltxn",
        "Number of CtrlTxn calls for " + name);
      n_write = statistics->Register(name + ".n_write",
        "Number of Write calls for " + name);
      n_reset = statistics->Register(name + ".n_reset",
        "Number of Reset calls for " + name);
      n_openfromtxn = statistics->Register(name + ".n_openfromtxn",
        "Number of OpenFromTxn calls for " + name);
      n_aborttxn = statistics->Register(name + ".n_aborttxn",
        "Number of AbortTxn calls for " + name);
      n_committxn = statistics->Register(name + ".n_committxn",
        "Number of Commit calls for " + name);
      n_reusefd = statistics->Register(name + ".n_reusefd",
        "Number of reused handles for " + name);
      n_appendfd = statistics->Register(name + ".n_appendfd",
        "Number of newly appended handles for " + name);
      n_enfile = statistics->Register(name + ".n_enfile",
        "Number of times " + name + " reached the limit on handles");
      n_openregular = statistics->Register(name + ".n_openregular",
        "Number of opens from the regular cache for " + name);
      n_openvolatile = statistics->Register(name + ".n_openvolatile",
        "Number of opens from the volatile cache for " + name);
      n_openmiss = statistics->Register(name + ".n_openmiss",
        "Number of missed opens for " + name);
      n_realloc = statistics->Register(name + ".n_realloc",
        "Number of reallocs for " + name);
      n_closesweep = statistics->Register(name + ".n_closesweep",
        "Number of times the handles vector was swept for " + name);
      n_overrun = statistics->Register(name + ".n_overrun",
        "Number of cache limit overruns for " + name);
      n_full = statistics->Register(name + ".n_full",
        "Number of overruns that could not be resolved for " + name);
      sz_alloc = statistics->Register(name + ".sz_alloc",
        "Number of bytes allocated for " + name);
      sz_committed = statistics->Register(name + ".sz_committed",
        "Number of bytes committed for " + name);
    }
  };

  virtual CacheManagerIds id() { return kRamCacheManager; }

  RamCacheManager(
    uint64_t max_size,
    unsigned max_entries,
    perf::Statistics *statistics)
    : max_size_(max_size)
    , regular_entries_(max_entries/2, "RamCache.regular", statistics)
    , volatile_entries_(max_entries/2, "RamCache.volatile", statistics)
    , counters_(statistics, "RamCache") {
    int retval = pthread_rwlock_init(&rwlock_, NULL);
    assert(retval == 0);
    LogCvmfs(kLogCache, kLogDebug, "max %u B, %u entries",
             max_size, max_entries);
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
   * space, give up an return failure.
   * @param txn A pointer to space allocated for storing the transaction details
   * @returns -ENOSPC if the transaction would exceed the size of the cache, -ENFILE if no handles are available,
   * or -EEXIST if an entry with nonzero reference count already exists
   */
  virtual int CommitTxn(void *txn);

 private:
  struct ReadOnlyFd {
    ReadOnlyFd() : handle(shash::Any()), pos(0), store(NULL) { }
    ReadOnlyFd(const shash::Any &h, uint64_t pos)
      : handle(h)
      , pos(pos)
      , store(NULL) { }
    shash::Any handle;
    uint64_t pos;
    kvstore::MemoryKvStore *store;
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
  kvstore::MemoryKvStore regular_entries_;
  kvstore::MemoryKvStore volatile_entries_;
  Counters counters_;
};  // class RamCacheManager

}  // namespace cache

#endif  // CVMFS_CACHE_RAM_H_
