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

/**
 * The @p RamCacheManager class provides a cache backend that operates
 * entirely from the host's RAM. This backend does not require any
 * additional privileges on the host such as filesystem access.
 * This cache resides in a single process' memory, and does not
 * support sharing or persistence. This cache backend is a good
 * choice in highly restrictive environments, e.g. HPC resources,
 * where it is not possible to make use of a shared/local filesystem.
 *
 * To use this cache backend, set @p CVMFS_CACHE_PRIMARY=ram. There
 * are not many knobs to configure; at present only the size to be
 * used for cached objects. @p CVMFS_CACHE_RAM_SIZE sets the amount
 * of memory to use (in MB) for caching objects. If the size ends
 * with a percent sign, the cache size will be set to that
 * percentage of the system memory. If no size is specified, the
 * RAM cache size defaults to ~3% of the system memory.
 * The minimum cache size is 200 MB.
 */
class RamCacheManager : public CacheManager {
 public:
  struct Counters {
    perf::Counter *n_getsize;
    perf::Counter *n_close;
    perf::Counter *n_pread;
    perf::Counter *n_dup;
    perf::Counter *n_readahead;
    perf::Counter *n_starttxn;
    perf::Counter *n_write;
    perf::Counter *n_reset;
    perf::Counter *n_aborttxn;
    perf::Counter *n_committxn;
    perf::Counter *n_enfile;
    perf::Counter *n_openregular;
    perf::Counter *n_openvolatile;
    perf::Counter *n_openmiss;
    perf::Counter *n_overrun;
    perf::Counter *n_full;
    perf::Counter *n_realloc;

    Counters(perf::Statistics *statistics, const std::string &name) {
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
      n_starttxn = statistics->Register(name + ".n_starttxn",
        "Number of StartTxn calls for " + name);
      n_write = statistics->Register(name + ".n_write",
        "Number of Write calls for " + name);
      n_reset = statistics->Register(name + ".n_reset",
        "Number of Reset calls for " + name);
      n_aborttxn = statistics->Register(name + ".n_aborttxn",
        "Number of AbortTxn calls for " + name);
      n_committxn = statistics->Register(name + ".n_committxn",
        "Number of Commit calls for " + name);
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
      n_overrun = statistics->Register(name + ".n_overrun",
        "Number of cache limit overruns for " + name);
      n_full = statistics->Register(name + ".n_full",
        "Number of overruns that could not be resolved for " + name);
    }
  };

  virtual CacheManagerIds id() { return kRamCacheManager; }

  RamCacheManager(
    uint64_t max_size,
    unsigned max_entries,
    perf::Statistics *statistics)
    : max_size_(max_size)
    , fd_pivot_(0)
    , open_fds_(max_entries)
    , fd_index_(max_entries)
    , regular_entries_(max_entries/2, "RamCache.regular", statistics)
    , volatile_entries_(max_entries/2, "RamCache.volatile", statistics)
    , counters_(statistics, "RamCache") {
    int retval = pthread_rwlock_init(&rwlock_, NULL);
    assert(retval == 0);
    for (size_t i = 0; i < fd_index_.size(); i++)
      fd_index_[i] = i;
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
   * @returns A nonnegative integer file descriptor
   * @retval -ENOENT The entry is not in the cache
   * @retval -ENFILE No handles are available
   */
  virtual int Open(const shash::Any &id);

  /**
   * Look up the size in bytes of the open cache entry
   * @param id The hash key
   * @returns The size of the entry
   * @retval -EBADF @p fd is not valid
   */
  virtual int64_t GetSize(int fd);

  /**
   * Close the descriptor in the cache. Entries not marked as pinned will become subject to
   * eviction once closed
   * @param id The hash key
   * @retval -EBADF @p fd is not valid
   */
  virtual int Close(int fd);

  /**
   * Read a section from the cache entry. See pread(3) for a discussion of the arguments
   * @param id The hash key
   * @returns The number of bytes copied
   * @retval -EBADF @p fd is not valid
   */
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset);

  /**
   * Duplicates the open file descriptor, allowing the original and the new one to be
   * used independently
   * @param id The hash key
   * @returns A new, nonnegative integer fd
   * @retval -EBADF @p fd is not valid
   * @retval -ENFILE No handles are available
   */
  virtual int Dup(int fd);

  /**
   * No effect for in-memory caches
   * @param id The hash key
   * @retval -EBADF @p fd is not valid
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
   * @retval -ENOSPC The transaction would exceed the size of the cache
   * @retval -ENFILE No handles are available
   * @retval -EEXIST An entry with nonzero reference count already exists
   */
  virtual int CommitTxn(void *txn);

 private:
  struct ReadOnlyFd {
    ReadOnlyFd() : handle(kInvalidHandle) { }
    explicit ReadOnlyFd(const shash::Any &h) : handle(h) { }
    shash::Any handle;
    bool is_volatile;
    size_t index;
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

  inline MemoryKvStore *GetStore(const ReadOnlyFd &fd) {
    if (fd.is_volatile) {
      return &volatile_entries_;
    } else {
      return &regular_entries_;
    }
  }

  int AddFd(const ReadOnlyFd &fd);
  int64_t CommitToKvStore(Transaction *transaction);
  virtual int DoOpen(const shash::Any &id);

  uint64_t max_size_;
  size_t fd_pivot_;
  std::vector<ReadOnlyFd> open_fds_;
  std::vector<size_t> fd_index_;
  pthread_rwlock_t rwlock_;
  MemoryKvStore regular_entries_;
  MemoryKvStore volatile_entries_;
  Counters counters_;
};  // class RamCacheManager

}  // namespace cache

#endif  // CVMFS_CACHE_RAM_H_
