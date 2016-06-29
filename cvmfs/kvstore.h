/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_KVSTORE_H_
#define CVMFS_KVSTORE_H_

#include "statistics.h"
#include "lru.h"

using namespace std;  // NOLINT

namespace kvstore {
struct MemoryBuffer {
  void *address;
  uint64_t size;
  int64_t refcount;
  
};
} // namespace kvstore

namespace lru {
class MemoryCache : public LruCache<shash::Any, kvstore::MemoryBuffer> {
 public:
  MemoryCache(unsigned int cache_size, perf::Statistics *statistics) :
    LruCache<shash::Any, kvstore::MemoryBuffer>(cache_size, shash::Any(), hasher_any,
        statistics, "memory_cache")
  {
  }

  bool Insert(const shash::Any &hash, const kvstore::MemoryBuffer &buf);
  bool Lookup(const shash::Any &hash, kvstore::MemoryBuffer *buf);
  bool Forget(const shash::Any &hash);
  void Drop();
};
}  // namespace lru

namespace kvstore {
class KvStore : SingleCopy {
 public:
  KvStore()
    : used_bytes(0) {}
  virtual int64_t GetSize(const shash::Any &id) = 0;
  virtual int64_t GetRefcount(const shash::Any &id) = 0;
  virtual bool Ref(const shash::Any &id) = 0;
  virtual bool Unref(const shash::Any &id) = 0;
  virtual int64_t Read(const shash::Any &id, void *buf, uint64_t size, uint64_t offset) = 0;
  virtual bool Commit(const shash::Any &id, const kvstore::MemoryBuffer &buf) = 0;
  virtual bool Delete(const shash::Any &id) = 0;
  uint64_t GetUsed() { return used_bytes; }
 protected:
  uint64_t used_bytes;
};

class MemoryKvStore : KvStore {
 public:
  explicit MemoryKvStore(unsigned int max_entries, perf::Statistics *statistics)
    : Entries(max_entries, statistics) {}
  virtual int64_t GetSize(const shash::Any &id);
  virtual int64_t GetRefcount(const shash::Any &id);
  virtual bool Ref(const shash::Any &id);
  virtual bool Unref(const shash::Any &id);
  virtual int64_t Read(const shash::Any &id, void *buf, uint64_t size, uint64_t offset);
  virtual bool Commit(const shash::Any &id, const kvstore::MemoryBuffer &buf);
  virtual bool Delete(const shash::Any &id);
 protected:
  virtual bool Lookup(const shash::Any &id, MemoryBuffer *mem);
  lru::MemoryCache Entries;
};
} // namespace kvstore
#endif // CVMFS_KVSTORE_H_
