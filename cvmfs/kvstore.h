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
  MemoryBuffer() :
    address(NULL), size(0), refcount(0) {}
  void *address;
  uint64_t size;
  int64_t refcount;
  
};

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
  virtual uint64_t GetUsed() { return used_bytes; }
  virtual bool Shrink(uint64_t size) = 0;
 protected:
  uint64_t used_bytes;
};

class MemoryKvStore : public KvStore {
 public:
  MemoryKvStore(unsigned int cache_entries, perf::Statistics *statistics)
    : Entries(cache_entries, shash::Any(), lru::hasher_any,
        statistics, "memory_cache") {}
  virtual int64_t GetSize(const shash::Any &id);
  virtual int64_t GetRefcount(const shash::Any &id);
  virtual bool Ref(const shash::Any &id);
  virtual bool Unref(const shash::Any &id);
  virtual int64_t Read(const shash::Any &id, void *buf, uint64_t size, uint64_t offset);
  virtual bool Commit(const shash::Any &id, const kvstore::MemoryBuffer &buf);
  virtual bool Delete(const shash::Any &id);
  virtual bool Shrink(uint64_t size);
  virtual bool GetBuffer(const shash::Any &id, MemoryBuffer *buf);
 protected:
  lru::LruCache<shash::Any, MemoryBuffer> Entries;
};
} // namespace kvstore
#endif // CVMFS_KVSTORE_H_
