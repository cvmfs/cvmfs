/**
 * This file is part of the CernVM File System.
 */

#include "kvstore.h"

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <string.h>

#include <algorithm>

#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace kvstore {

bool MemoryKvStore::GetBuffer(const shash::Any &id, MemoryBuffer *buf) {
  return entries_.Lookup(id, buf);
}

bool MemoryKvStore::PopBuffer(const shash::Any &id, MemoryBuffer *buf) {
  WriteLockGuard guard(rwlock_);
  if (!entries_.Lookup(id, buf)) return false;
  used_bytes_ -= (*buf).size;
  entries_.Forget(id);
  return true;
}

int64_t MemoryKvStore::GetSize(const shash::Any &id) {
  MemoryBuffer mem;
  if (entries_.Lookup(id, &mem)) {
    return mem.size;
  } else {
    return -ENOENT;
  }
}

int64_t MemoryKvStore::GetRefcount(const shash::Any &id) {
  MemoryBuffer mem;
  if (entries_.Lookup(id, &mem)) {
    return mem.refcount;
  } else {
    return -ENOENT;
  }
}

bool MemoryKvStore::IncRef(const shash::Any &id) {
  WriteLockGuard guard(rwlock_);
  MemoryBuffer mem;
  if (entries_.Lookup(id, &mem)) {
    assert(mem.refcount < UINT_MAX);
    ++mem.refcount;
    entries_.Insert(id, mem);
    return true;
  } else {
    return false;
  }
}

bool MemoryKvStore::Unref(const shash::Any &id) {
  WriteLockGuard guard(rwlock_);
  MemoryBuffer mem;
  if (entries_.Lookup(id, &mem)) {
    assert(mem.refcount > 0);
    --mem.refcount;
    entries_.Insert(id, mem);
    return true;
  } else {
    return false;
  }
}

int64_t MemoryKvStore::Read(
  const shash::Any &id,
  void *buf,
  size_t size,
  size_t offset
) {
  MemoryBuffer mem;
  ReadLockGuard guard(rwlock_);
  if (entries_.Lookup(id, &mem)) {
    if (offset > mem.size) return 0;
    uint64_t copy_size = min(mem.size - offset, size);
    memcpy(buf, static_cast<char *>(mem.address) + offset, copy_size);
    return copy_size;
  } else {
    return -ENOENT;
  }
}

bool MemoryKvStore::Commit(
  const shash::Any &id,
  const kvstore::MemoryBuffer &buf
) {
  WriteLockGuard guard(rwlock_);
  MemoryBuffer mem;
  if (entries_.Lookup(id, &mem)) {
    if (!DoDelete(id)) return false;
  }
  entries_.Insert(id, buf);
  used_bytes_ += buf.size;
  return true;
}

bool MemoryKvStore::Delete(const shash::Any &id) {
  WriteLockGuard guard(rwlock_);
  return DoDelete(id);
}

bool MemoryKvStore::DoDelete(const shash::Any &id) {
  MemoryBuffer buf;
  if (entries_.Lookup(id, &buf)) {
    if (buf.refcount > 0) return false;
    used_bytes_ -= buf.size;
    free(buf.address);
    entries_.Forget(id);
  }
  return true;
}

bool MemoryKvStore::ShrinkTo(size_t size) {
  WriteLockGuard guard(rwlock_);
  shash::Any key;
  MemoryBuffer buf;

  if (used_bytes_ <= size) return true;

  entries_.FilterBegin();
  while (entries_.FilterNext()) {
    if (used_bytes_ <= size) break;
    entries_.FilterGet(&key, &buf);
    if (buf.refcount > 0) continue;
    entries_.FilterDelete();
    used_bytes_ -= buf.size;
    free(buf.address);
  }
  entries_.FilterEnd();
  return used_bytes_ <= size;
}

}  // namespace kvstore
