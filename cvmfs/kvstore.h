/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_KVSTORE_H_
#define CVMFS_KVSTORE_H_

#include <unistd.h>
#include "statistics.h"
#include "lru.h"
#include "cache.h"

using namespace std;  // NOLINT

namespace kvstore {

struct MemoryBuffer {
  void *address;
  size_t size;
  unsigned int refcount;
  cache::CacheManager::ObjectType object_type;
};

class MemoryKvStore :SingleCopy {
 public:
  MemoryKvStore(unsigned int cache_entries, perf::Statistics *statistics)
    : used_bytes(0)
    , Entries(cache_entries, shash::Any(), lru::hasher_any,
        statistics, "memory_cache") {}

  /**
   * Get the size in bytes of the entry at id
   * @param id The hash key
   * @returns A size, or -ENOENT if the entry is absent
   */
  virtual int64_t GetSize(const shash::Any &id);

  /**
   * Get the number of references to the entry at id
   * @param id The hash key
   * @returns A reference count, or -ENOENT if the entry is absent
   */
  virtual int64_t GetRefcount(const shash::Any &id);

  /**
   * Increase the reference count on the entry at id
   * @param id The hash key
   * @returns True if the entry exists and was updated
   */
  virtual bool Ref(const shash::Any &id);

  /**
   * Decrease the reference count on the entry at id. If the refcount is zero, no effect
   * @param id The hash key
   * @returns True if the entry exists and was updated
   */
  virtual bool Unref(const shash::Any &id);

  /**
   * Copy a portion of the entry at id to the given address. See pread(2)
   * @param id The hash key
   * @param buf The address at which to write the data
   * @param size The number of bytes to copy
   * @param offset The offset within the entry to start the copy
   * @returns The number of bytes copied, or -ENOENT if the entry is absent
   */
  virtual int64_t Read(const shash::Any &id, void *buf, size_t size, off_t offset);

  /**
   * Insert a new memory buffer. The KvStore takes ownership of the referred memory, so
   * callers must not free() it themselves.
   * @param id The hash key
   * @param buf The memory buffer to insert
   * @returns True iff this overwrites an existing entry at id
   */
  virtual bool Commit(const shash::Any &id, const kvstore::MemoryBuffer &buf);

  /**
   * Delete an entry, free()ing its memory
   * @param id The hash key
   * @returns True iff there was an entry at id
   */
  virtual bool Delete(const shash::Any &id);

  /**
   * Delete the oldest entries until the KvStore uses less than the given size
   * @param size The maximum size to make the KvStore
   * @returns True iff the shrink succeeds
   */
  virtual bool Shrink(size_t size);

  /**
   * Get the memory buffer describing the entry at id
   * @param id The hash key
   * @returns True iff the entry is present
   */
  virtual bool GetBuffer(const shash::Any &id, MemoryBuffer *buf);

  /**
   * Get the memory buffer describing the entry at id as in @ref GetBuffer,
   * and remove the entry *without* freeing the associated memory
   * @param id The hash key
   * @returns True iff the entry is present
   */
  virtual bool PopBuffer(const shash::Any &id, MemoryBuffer *buf);

  /**
   * Get the total space used for data
   */
  virtual size_t GetUsed() { return used_bytes; }
 protected:
  size_t used_bytes;
  lru::LruCache<shash::Any, MemoryBuffer> Entries;
};
} // namespace kvstore
#endif // CVMFS_KVSTORE_H_
