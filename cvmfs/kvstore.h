/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_KVSTORE_H_
#define CVMFS_KVSTORE_H_

#include <pthread.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "cache.h"
#include "lru.h"
#include "malloc_heap.h"
#include "statistics.h"
#include "util/async.h"
#include "util/single_copy.h"

using namespace std;  // NOLINT


/**
 * All objects in memory are prepended by an AllocHeader that allows the
 * Key-Value store to find the pointer when the heap memory manager compacts
 * the allocations.
 */
struct AllocHeader {
  AllocHeader() : version(0), id() { }
  uint8_t version;
  shash::Any id;
};


/**
 * A MmeoryBuffer is used in the staging phase of new objects until they are
 * committed.
 */
struct MemoryBuffer {
  MemoryBuffer()
      : address(NULL), size(0), refcount(0), object_flags(0), id() { }
  void *address;
  size_t size;
  unsigned int refcount;
  int object_flags;
  shash::Any id;
};


/**
 * @p MemoryKvStore provides a simple RAM-backed key/value store suited to
 * use with @ref RamCacheManager. To insert entries, the caller must allocate
 * some memory and can choose to set some metadata such as object type.
 * @p MemoryKvStore takes ownership of the passed-in memory and maintains
 * reference counts for all its objects. Callers must increment the reference
 * count on an entry before reading to ensure that the entry is not removed
 * mid-operation, and decrement the reference count when done. The store
 * can attempt to reduce its size by removing the least recently used
 * entries without any outstanding references.
 */
class MemoryKvStore : SingleCopy, public Callbackable<MallocHeap::BlockPtr> {
 public:
  enum MemoryAllocator {
    kMallocLibc,
    kMallocHeap,
  };

  struct Counters {
    perf::Counter *sz_size;
    perf::Counter *n_getsize;
    perf::Counter *n_getrefcount;
    perf::Counter *n_incref;
    perf::Counter *n_unref;
    perf::Counter *n_read;
    perf::Counter *n_commit;
    perf::Counter *n_delete;
    perf::Counter *n_shrinkto;
    perf::Counter *sz_read;
    perf::Counter *sz_committed;
    perf::Counter *sz_deleted;
    perf::Counter *sz_shrunk;

    explicit Counters(perf::StatisticsTemplate statistics) {
      sz_size = statistics.RegisterTemplated("sz_size", "Total size");
      n_getsize = statistics.RegisterTemplated("n_getsize",
                                               "Number of GetSize calls");
      n_getrefcount = statistics.RegisterTemplated(
          "n_getrefcount", "Number of GetRefcount calls");
      n_incref = statistics.RegisterTemplated("n_incref",
                                              "Number of IncRef calls");
      n_unref = statistics.RegisterTemplated("n_unref",
                                             "Number of Unref calls");
      n_read = statistics.RegisterTemplated("n_read", "Number of Read calls");
      n_commit = statistics.RegisterTemplated("n_commit",
                                              "Number of Commit calls");
      n_delete = statistics.RegisterTemplated("n_delete",
                                              "Number of Delete calls");
      n_shrinkto = statistics.RegisterTemplated("n_shrinkto",
                                                "Number of ShrinkTo calls");
      sz_read = statistics.RegisterTemplated("sz_read", "Bytes read");
      sz_committed = statistics.RegisterTemplated("sz_committed",
                                                  "Bytes committed");
      sz_deleted = statistics.RegisterTemplated("sz_deleted", "Bytes deleted");
      sz_shrunk = statistics.RegisterTemplated("sz_shrunk", "Bytes shrunk");
    }
  };

  MemoryKvStore(unsigned int cache_entries,
                MemoryAllocator alloc,
                unsigned alloc_size,
                perf::StatisticsTemplate statistics);

  ~MemoryKvStore();

  /**
   * Check for the existence of an entry
   * @param id The hash key
   * @returns True iff the entry exists
   */
  bool Contains(const shash::Any &id);

  /**
   * Get the size in bytes of the entry at id
   * @param id The hash key
   * @returns The entry's size
   * @retval -ENOENT The entry is absent
   */
  int64_t GetSize(const shash::Any &id);

  /**
   * Get the number of references to the entry at id
   * @param id The hash key
   * @returns A reference count
   * @retval -ENOENT The entry is absent
   */
  int64_t GetRefcount(const shash::Any &id);

  /**
   * Increase the reference count on the entry at id
   * @param id The hash key
   * @returns True if the entry exists and was updated
   */
  bool IncRef(const shash::Any &id);

  /**
   * Decrease the reference count on the entry at id. If the refcount is zero,
   * no effect
   * @param id The hash key
   * @returns True if the entry exists and was updated
   */
  bool Unref(const shash::Any &id);

  /**
   * Copy a portion of the entry at id to the given address. See pread(2)
   * @param id The hash key
   * @param buf The address at which to write the data
   * @param size The number of bytes to copy
   * @param offset The offset within the entry to start the copy
   * @returns The number of bytes copied
   * @retval -ENOENT The entry is absent
   */
  int64_t Read(const shash::Any &id, void *buf, size_t size, size_t offset);

  /**
   * Insert a new memory buffer. The KvStore copies the referred memory, so
   * callers may free() their buffers after Commit returns
   * @param buf The memory buffer to insert
   * @returns -ENFILE if too many file handles are in use
   * @returns -EIO if memory allocation fails
   */
  int Commit(const MemoryBuffer &buf);

  /**
   * Delete an entry, free()ing its memory. Note that the entry not have any
   * references
   * @param id The hash key
   * @returns True iff the entry was successfully deleted
   */
  bool Delete(const shash::Any &id);

  /**
   * Delete the oldest entries until the KvStore uses less than the given size.
   * Entries with nonzero refcount will not be deleted.
   * @param size The maximum size to make the KvStore
   * @returns True iff the shrink succeeds
   */
  bool ShrinkTo(size_t size);

  /**
   * Get the total space used for data
   */
  size_t GetUsed() { return used_bytes_; }

 private:
  // Compact memory once utilization falls below the threshold
  static const double kCompactThreshold;  // = 0.8

  bool DoDelete(const shash::Any &id);
  int DoMalloc(MemoryBuffer *buf);
  void DoFree(MemoryBuffer *buf);
  int DoCommit(const MemoryBuffer &buf);
  void OnBlockMove(const MallocHeap::BlockPtr &ptr);
  bool CompactMemory();

  MemoryAllocator allocator_;
  size_t used_bytes_;
  unsigned int entry_count_;
  unsigned int max_entries_;
  lru::LruCache<shash::Any, MemoryBuffer> entries_;
  MallocHeap *heap_;
  pthread_rwlock_t rwlock_;
  Counters counters_;
};

#endif  // CVMFS_KVSTORE_H_
