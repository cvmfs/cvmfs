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
#include "crypto/hash.h"
#include "fd_table.h"
#include "kvstore.h"
#include "statistics.h"
#include "util/pointer.h"


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
 *
 * RamCacheManager uses a custom heap allocator rather than
 * the system's libc @p malloc(). To switch to libc malloc, set
 * @p CVMFS_CACHE_RAM_MALLOC=libc
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

    explicit Counters(perf::StatisticsTemplate statistics) {
      n_getsize = statistics.RegisterTemplated("n_getsize",
                                               "Number of GetSize calls");
      n_close = statistics.RegisterTemplated("n_close",
                                             "Number of Close calls");
      n_pread = statistics.RegisterTemplated("n_pread",
                                             "Number of Pread calls");
      n_dup = statistics.RegisterTemplated("n_dup", "Number of Dup calls");
      n_readahead = statistics.RegisterTemplated("n_readahead",
                                                 "Number of ReadAhead calls");
      n_starttxn = statistics.RegisterTemplated("n_starttxn",
                                                "Number of StartTxn calls");
      n_write = statistics.RegisterTemplated("n_write",
                                             "Number of Write calls");
      n_reset = statistics.RegisterTemplated("n_reset",
                                             "Number of Reset calls");
      n_aborttxn = statistics.RegisterTemplated("n_aborttxn",
                                                "Number of AbortTxn calls");
      n_committxn = statistics.RegisterTemplated("n_committxn",
                                                 "Number of Commit calls");
      n_enfile = statistics.RegisterTemplated(
          "n_enfile", "Number of times the limit on handles was reached");
      n_openregular = statistics.RegisterTemplated(
          "n_openregular", "Number of opens from the regular cache");
      n_openvolatile = statistics.RegisterTemplated(
          "n_openvolatile", "Number of opens from the volatile cache");
      n_openmiss = statistics.RegisterTemplated("n_openmiss",
                                                "Number of missed opens");
      n_realloc = statistics.RegisterTemplated("n_realloc",
                                               "Number of reallocs");
      n_overrun = statistics.RegisterTemplated(
          "n_overrun", "Number of cache limit overruns");
      n_full = statistics.RegisterTemplated(
          "n_full", "Number of overruns that could not be resolved");
    }
  };

  virtual CacheManagerIds id() { return kRamCacheManager; }
  virtual std::string Describe();

  RamCacheManager(uint64_t max_size,
                  unsigned max_entries,
                  MemoryKvStore::MemoryAllocator alloc,
                  perf::StatisticsTemplate statistics);

  virtual ~RamCacheManager();

  virtual bool AcquireQuotaManager(QuotaManager *quota_mgr);

  /**
   * Open a new file descriptor into the cache. Note that opening entries
   * effectively pins them in the cache, so it may be necessary to close
   * unneeded file descriptors to allow eviction to make room in the cache
   * @param object The tagged hash key
   * @returns A nonnegative integer file descriptor
   * @retval -ENOENT The entry is not in the cache
   * @retval -ENFILE No handles are available
   */
  virtual int Open(const LabeledObject &object);

  /**
   * Look up the size in bytes of the open cache entry
   * @param id The hash key
   * @returns The size of the entry
   * @retval -EBADF @p fd is not valid
   */
  virtual int64_t GetSize(int fd);

  /**
   * Close the descriptor in the cache. Entries not marked as pinned will become
   * subject to eviction once closed
   * @param id The hash key
   * @retval -EBADF @p fd is not valid
   */
  virtual int Close(int fd);

  /**
   * Read a section from the cache entry. See pread(3) for a discussion of the
   * arguments
   * @param id The hash key
   * @returns The number of bytes copied
   * @retval -EBADF @p fd is not valid
   */
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset);

  /**
   * Duplicates the open file descriptor, allowing the original and the new one
   * to be used independently
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


  /**
   * Get the amount of space to be allocated for a call to @ref StartTxn
   */
  virtual uint32_t SizeOfTxn() { return sizeof(Transaction); }


  /**
   * Start a new transaction, allocating the required memory and initializing
   * the transaction parameters
   * @param id The hash key
   * @param size The total size of the new cache entry
   * @param txn A pointer to space allocated for storing the transaction details
   */
  virtual int StartTxn(const shash::Any &id, uint64_t size, void *txn);

  /**
   * Set the transaction parameters. At present, only @ref type is used for
   * pinned, volatile, etc.
   * @param description Unused
   * @param type The type of the entry, e.g. pinned
   * @param flags Unused
   * @param txn A pointer to space allocated for storing the transaction details
   */
  virtual void CtrlTxn(const Label &label, const int flags, void *txn);

  /**
   * Copy the given memory region into the transaction buffer. Copying starts at
   * the transaction's current offset
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
   * Commit a transaction and open the resulting cache entry. This is necessary
   * to avoid a race condition in which the cache entry is evicted between the
   * calls to CommitTxn and Open.
   * @param txn A pointer to space allocated for storing the transaction details
   * @returns A file descriptor to the new cache entry, or a negative error from
   *          @ref CommitTxn
   */
  virtual int OpenFromTxn(void *txn);

  /**
   * Free the resources allocated for the pending transaction
   * @param txn A pointer to space allocated for storing the transaction details
   */
  virtual int AbortTxn(void *txn);

  /**
   * Commit a transaction to the cache. If there is not enough free space in the
   * cache, first try to make room by evicting volatile entries. If there is
   * still not enough room, try evicting regular entries. If there is *still*
   * not enough space, give up an return failure.
   * @param txn A pointer to space allocated for storing the transaction details
   * @retval -ENOSPC The transaction would exceed the size of the cache
   * @retval -ENFILE No handles are available
   * @retval -EEXIST An entry with nonzero reference count already exists
   */
  virtual int CommitTxn(void *txn);

  virtual void Spawn() { }

 private:
  // The null hash (hashed output is all null bytes) serves as a marker for
  // an invalid handle
  static const shash::Any kInvalidHandle;

  struct ReadOnlyHandle {
    ReadOnlyHandle() : handle(kInvalidHandle), is_volatile(false) { }
    ReadOnlyHandle(const shash::Any &h, bool v) : handle(h), is_volatile(v) { }
    bool operator==(const ReadOnlyHandle &other) const {
      return this->handle == other.handle;
    }
    bool operator!=(const ReadOnlyHandle &other) const {
      return this->handle != other.handle;
    }

    shash::Any handle;
    bool is_volatile;
  };

  struct Transaction {
    Transaction() : buffer(), expected_size(0), pos(0) { }
    MemoryBuffer buffer;
    uint64_t expected_size;
    uint64_t pos;
    std::string description;
  };

  inline MemoryKvStore *GetStore(const ReadOnlyHandle &fd) {
    if (fd.is_volatile) {
      return &volatile_entries_;
    } else {
      return &regular_entries_;
    }
  }

  inline MemoryKvStore *GetTransactionStore(Transaction *txn) {
    if (txn->buffer.object_flags & CacheManager::kLabelVolatile) {
      return &volatile_entries_;
    } else {
      return &regular_entries_;
    }
  }

  int AddFd(const ReadOnlyHandle &handle);
  int64_t CommitToKvStore(Transaction *transaction);
  virtual int DoOpen(const shash::Any &id);

  uint64_t max_size_;
  FdTable<ReadOnlyHandle> fd_table_;
  pthread_rwlock_t rwlock_;
  MemoryKvStore regular_entries_;
  MemoryKvStore volatile_entries_;
  Counters counters_;
};  // class RamCacheManager

#endif  // CVMFS_CACHE_RAM_H_
