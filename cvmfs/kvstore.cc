/**
 * This file is part of the CernVM File System.
 */

#include "kvstore.h"

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <string.h>

#include <algorithm>

#include "logging.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace kvstore {

bool MemoryKvStore::GetBuffer(const shash::Any &id, MemoryBuffer *buf) {
  LogCvmfs(kLogKvStore, kLogDebug, "get buffer %s", id.ToString().c_str());
  return entries_.Lookup(id, buf);
}

bool MemoryKvStore::PopBuffer(const shash::Any &id, MemoryBuffer *buf) {
  WriteLockGuard guard(rwlock_);
  if (!entries_.Lookup(id, buf)) {
    LogCvmfs(kLogKvStore, kLogDebug, "miss %s on PopBuffer",
             id.ToString().c_str() );
    return false;
  }
  used_bytes_ -= (*buf).size;
  entries_.Forget(id);
  LogCvmfs(kLogKvStore, kLogDebug, "popped %s (%uB)", id.ToString().c_str(),
           (*buf).size);
  return true;
}

int64_t MemoryKvStore::GetSize(const shash::Any &id) {
  MemoryBuffer mem;
  if (entries_.Lookup(id, &mem)) {
    LogCvmfs(kLogKvStore, kLogDebug, "%s is %u B", id.ToString().c_str(),
             mem.size);
    return mem.size;
  } else {
    LogCvmfs(kLogKvStore, kLogDebug,
             "miss%s on GetSize",
             id.ToString().c_str());
    return -ENOENT;
  }
}

int64_t MemoryKvStore::GetRefcount(const shash::Any &id) {
  MemoryBuffer mem;
  if (entries_.Lookup(id, &mem)) {
    LogCvmfs(kLogKvStore, kLogDebug, "%s has refcount %u",
             id.ToString().c_str(), mem.refcount);
    return mem.refcount;
  } else {
    LogCvmfs(kLogKvStore, kLogDebug, "miss %s on GetRefcount",
             id.ToString().c_str());
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
    LogCvmfs(kLogKvStore, kLogDebug, "increased refcount of %s to %u",
             id.ToString().c_str(), mem.refcount);
    return true;
  } else {
    LogCvmfs(kLogKvStore, kLogDebug, "miss %s on IncRef",
             id.ToString().c_str());
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
    LogCvmfs(kLogKvStore, kLogDebug, "decreased refcount of %s to %u",
             id.ToString().c_str(), mem.refcount);
    return true;
  } else {
    LogCvmfs(kLogKvStore, kLogDebug, "miss %s on Unref",
             id.ToString().c_str());
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
    if (offset > mem.size) {
      LogCvmfs(kLogKvStore, kLogDebug, "out of bounds read (%u>%u) on %s",
               offset, mem.size, id.ToString().c_str());
      return 0;
    }
    uint64_t copy_size = min(mem.size - offset, size);
    LogCvmfs(kLogKvStore, kLogDebug, "copy %u B from offset %u of %s",
             copy_size, offset, id.ToString().c_str());
    memcpy(buf, static_cast<char *>(mem.address) + offset, copy_size);
    return copy_size;
  } else {
    LogCvmfs(kLogKvStore, kLogDebug, "miss %s on Read", id.ToString().c_str());
    return -ENOENT;
  }
}

bool MemoryKvStore::Commit(
  const shash::Any &id,
  const kvstore::MemoryBuffer &buf
) {
  bool overwrote = false;
  MemoryBuffer mem;
  WriteLockGuard guard(rwlock_);
  LogCvmfs(kLogKvStore, kLogDebug, "commit %s", id.ToString().c_str());
  if (entries_.Lookup(id, &mem)) {
    LogCvmfs(kLogKvStore, kLogDebug, "commit overwrites existing entry");
    used_bytes_ -= mem.size;
    overwrote = true;
  } else {
    mem.refcount = buf.refcount;
  }
  mem.address = buf.address;
  mem.size = buf.size;
  mem.object_type = buf.object_type;
  entries_.Insert(id, mem);
  used_bytes_ += mem.size;
  return overwrote;
}

bool MemoryKvStore::Delete(const shash::Any &id) {
  WriteLockGuard guard(rwlock_);
  return DoDelete(id);
}

bool MemoryKvStore::DoDelete(const shash::Any &id) {
  MemoryBuffer buf;
  if (entries_.Lookup(id, &buf)) {
    if (buf.refcount > 0) {
      LogCvmfs(kLogKvStore, kLogDebug, "can't delete %s, nonzero refcount",
               id.ToString().c_str());
      return false;
    }
    used_bytes_ -= buf.size;
    free(buf.address);
    entries_.Forget(id);
  }
  LogCvmfs(kLogKvStore, kLogDebug, "deleted %s", id.ToString().c_str());
  return true;
}

bool MemoryKvStore::ShrinkTo(size_t size) {
  WriteLockGuard guard(rwlock_);
  shash::Any key;
  MemoryBuffer buf;

  if (used_bytes_ <= size) {
    LogCvmfs(kLogKvStore, kLogDebug, "no need to shrink");
    return true;
  }

  LogCvmfs(kLogKvStore, kLogDebug, "shrinking to %u B", size);
  entries_.FilterBegin();
  while (entries_.FilterNext()) {
    if (used_bytes_ <= size) break;
    entries_.FilterGet(&key, &buf);
    if (buf.refcount > 0) {
      LogCvmfs(kLogKvStore, kLogDebug, "skip %s, nonzero refcount",
               key.ToString().c_str());
      continue;
    }
    entries_.FilterDelete();
    used_bytes_ -= buf.size;
    free(buf.address);
    LogCvmfs(kLogKvStore, kLogDebug, "delete %s", key.ToString().c_str());
  }
  entries_.FilterEnd();
  LogCvmfs(kLogKvStore, kLogDebug, "shrunk to %u B", used_bytes_);
  return used_bytes_ <= size;
}

}  // namespace kvstore
