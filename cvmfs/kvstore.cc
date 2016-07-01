#include <errno.h>
#include <string.h>
#include <assert.h>
#include <limits.h>
#include "kvstore.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace kvstore {

bool MemoryKvStore::GetBuffer(const shash::Any &id, MemoryBuffer *buf) {
  return Entries.Lookup(id, buf);
}

bool MemoryKvStore::PopBuffer(const shash::Any &id, MemoryBuffer *buf) {
  WriteLockGuard guard(rwlock_);
  if (!Entries.Lookup(id, buf)) return false;
  used_bytes -= (*buf).size;
  Entries.Forget(id);
  return true;
}

int64_t MemoryKvStore::GetSize(const shash::Any &id) {
  MemoryBuffer mem;
  if (Entries.Lookup(id, &mem)) {
    return mem.size;
  } else {
    return -ENOENT;
  }
}

int64_t MemoryKvStore::GetRefcount(const shash::Any &id) {
  MemoryBuffer mem;
  if (Entries.Lookup(id, &mem)) {
    return mem.refcount;
  } else {
    return -ENOENT;
  }
}

bool MemoryKvStore::Ref(const shash::Any &id) {
  WriteLockGuard guard(rwlock_);
  MemoryBuffer mem;
  if (Entries.Lookup(id, &mem)) {
    assert(mem.refcount < UINT_MAX);
    ++mem.refcount;
    Entries.Insert(id, mem);
    return true;
  } else {
    return false;
  }
}

bool MemoryKvStore::Unref(const shash::Any &id) {
  WriteLockGuard guard(rwlock_);
  MemoryBuffer mem;
  if (Entries.Lookup(id, &mem) && mem.refcount > 0) {
    --mem.refcount;
    Entries.Insert(id, mem);
    return true;
  } else {
    return false;
  }
}

int64_t MemoryKvStore::Read(const shash::Any &id, void *buf, size_t size, off_t offset) {
  MemoryBuffer mem;
  ReadLockGuard guard(rwlock_);
  if (Entries.Lookup(id, &mem)) {
    uint64_t copy_size = min(mem.size - offset, size);
    memcpy(buf, static_cast<char *>(mem.address) + offset, copy_size);
    return copy_size;
  } else {
    return -ENOENT;
  }
}

bool MemoryKvStore::Commit(const shash::Any &id, const kvstore::MemoryBuffer &buf) {
  WriteLockGuard guard(rwlock_);
  bool existed = false;
  MemoryBuffer mem;
  if (Entries.Lookup(id, &mem)) {
    Delete(id);
    existed = true;
  }
  Entries.Insert(id, buf);
  used_bytes += buf.size;
  return existed;
}

bool MemoryKvStore::Delete(const shash::Any &id) {
  WriteLockGuard guard(rwlock_);
  return DoDelete(id);
}

bool MemoryKvStore::DoDelete(const shash::Any &id) {
  MemoryBuffer buf;
  if (Entries.Lookup(id, &buf)) {
    used_bytes -= buf.size;
    free(buf.address);
    Entries.Forget(id);
    return true;
  } else {
    return false;
  }
}

bool MemoryKvStore::Shrink(size_t size) {
  WriteLockGuard guard(rwlock_);
  shash::Any key;
  MemoryBuffer buf;
  while (used_bytes > size) {
    if (!Entries.PopOldest(&key, &buf)) return false;
    used_bytes -= buf.size;
    free(buf.address);
  }
  return true;
}

} // namespace kvstore
