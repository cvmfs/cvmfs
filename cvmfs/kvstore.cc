#include <errno.h>
#include <string.h>
#include "kvstore.h"

using namespace std;  // NOLINT

namespace kvstore {
bool MemoryKvStore::Lookup(const shash::Any &id, MemoryBuffer *mem) {
  MemoryBuffer m;
  if (!mem) mem = &m;
  return Entries.Lookup(id, mem);
}

int64_t MemoryKvStore::GetSize(const shash::Any &id) {
  MemoryBuffer mem;
  if (Lookup(id, &mem)) {
    return mem.size;
  } else {
    return -ENOENT;
  }
}

int64_t MemoryKvStore::GetRefcount(const shash::Any &id) {
  MemoryBuffer mem;
  if (Lookup(id, &mem)) {
    return mem.refcount;
  } else {
    return -ENOENT;
  }
}

bool MemoryKvStore::Ref(const shash::Any &id) {
  MemoryBuffer mem;
  if (Lookup(id, &mem)) {
    // is an overflow check necessary here?
    ++mem.refcount;
    return true;
  } else {
    return false;
  }
}

bool MemoryKvStore::Unref(const shash::Any &id) {
  MemoryBuffer mem;
  if (Lookup(id, &mem) && mem.refcount > 0) {
    --mem.refcount;
    return true;
  } else {
    return false;
  }
}

int64_t MemoryKvStore::Read(const shash::Any &id, void *buf, uint64_t size, uint64_t offset) {
  MemoryBuffer mem;
  if (Lookup(id, &mem)) {
    uint64_t copy_size = min(mem.size - offset, size);
    memcpy(buf, static_cast<char *>(mem.address) + offset, copy_size);
    return copy_size;
  } else {
    return -ENOENT;
  }
}

bool MemoryKvStore::Commit(const shash::Any &id, const kvstore::MemoryBuffer &buf) {
  bool existed = false;
  if (Lookup(id, NULL)) {
    Delete(id);
    existed = true;
  }
  Entries.Insert(id, buf);
  used_bytes += buf.size;
  return existed;
}

bool MemoryKvStore::Delete(const shash::Any &id) {
  MemoryBuffer buf;
  if (Lookup(id, &buf)) {
    used_bytes -= buf.size;
    Entries.Forget(id);
    return true;
  } else {
    return false;
  }
}

} // namespace kvstore

namespace lru {
  bool MemoryCache::Insert(const shash::Any &hash, const kvstore::MemoryBuffer &buf) {
    LogCvmfs(kLogLru, kLogDebug, "insert hash --> memory %s -> '%p'",
             hash.ToString().c_str(), buf.address);
    const bool result = LruCache<shash::Any, kvstore::MemoryBuffer>::Insert(hash, buf);
    return result;
  }

  bool MemoryCache::Lookup(const shash::Any &hash, kvstore::MemoryBuffer *buf) {
    const bool found = LruCache<shash::Any, kvstore::MemoryBuffer>::Lookup(hash, buf);
    LogCvmfs(kLogLru, kLogDebug, "lookup hash --> memory: %s (%s)",
             hash.ToString().c_str(), found ? "hit" : "miss");
    return found;
  }

  bool MemoryCache::Forget(const shash::Any &hash) {
    const bool found = LruCache<shash::Any, kvstore::MemoryBuffer>::Forget(hash);
    LogCvmfs(kLogLru, kLogDebug, "forget hash: %s (%s)",
             hash.ToString().c_str(), found ? "hit" : "miss");
    return found;
  }

  void MemoryCache::Drop() {
    LogCvmfs(kLogLru, kLogDebug, "dropping memory cache");
    LruCache<shash::Any, kvstore::MemoryBuffer>::Drop();
  }
} // namespace lru
