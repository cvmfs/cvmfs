#include <errno.h>
#include <string.h>
#include "kvstore.h"

using namespace std;  // NOLINT

namespace kvstore {

bool MemoryKvStore::GetBuffer(const shash::Any &id, MemoryBuffer *buf) {
  return Entries.Lookup(id, buf);
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
  MemoryBuffer mem;
  if (Entries.Lookup(id, &mem)) {
    // is an overflow check necessary here?
    ++mem.refcount;
    return true;
  } else {
    return false;
  }
}

bool MemoryKvStore::Unref(const shash::Any &id) {
  MemoryBuffer mem;
  if (Entries.Lookup(id, &mem) && mem.refcount > 0) {
    --mem.refcount;
    return true;
  } else {
    return false;
  }
}

int64_t MemoryKvStore::Read(const shash::Any &id, void *buf, uint64_t size, uint64_t offset) {
  MemoryBuffer mem;
  if (Entries.Lookup(id, &mem)) {
    uint64_t copy_size = min(mem.size - offset, size);
    memcpy(buf, static_cast<char *>(mem.address) + offset, copy_size);
    return copy_size;
  } else {
    return -ENOENT;
  }
}

bool MemoryKvStore::Commit(const shash::Any &id, const kvstore::MemoryBuffer &buf) {
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

bool MemoryKvStore::Shrink(uint64_t size) {
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
