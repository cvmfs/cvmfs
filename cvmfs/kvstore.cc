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

bool MemoryKvStore::GetBuffer(const shash::Any &id, MemoryBuffer *buf) {
  perf::Inc(counters_.n_getbuffer);
  // LogCvmfs(kLogKvStore, kLogDebug, "get buffer %s", id.ToString().c_str());
  return entries_.Lookup(id, buf);
}

bool MemoryKvStore::PopBuffer(const shash::Any &id, MemoryBuffer *buf) {
  perf::Inc(counters_.n_popbuffer);
  WriteLockGuard guard(rwlock_);
  if (!entries_.Lookup(id, buf)) {
    LogCvmfs(kLogKvStore, kLogDebug, "miss %s on PopBuffer",
             id.ToString().c_str() );
    return false;
  }
  assert(used_bytes_ >= (*buf).size);
  used_bytes_ -= (*buf).size;
  counters_.sz_size->Set(used_bytes_);
  assert(entry_count_ > 0);
  --entry_count_;
  entries_.Forget(id);
  // LogCvmfs(kLogKvStore, kLogDebug, "popped %s (%uB)", id.ToString().c_str(),
  //          (*buf).size);
  return true;
}

size_t MemoryKvStore::GetBufferSize(void *ptr) {
  return MallocArena::GetMallocArena(ptr, kArenaSize)->GetSize(ptr);
}

void *MemoryKvStore::MallocBuffer(size_t size) {
  WriteLockGuard guard(rwlock_);
  return DoMalloc(size);
}

void *MemoryKvStore::DoMalloc(size_t size) {
  size_t N;
  void *p;
  MallocArena *M;

  switch (allocator_) {
  case kMallocLibc:
    return malloc(size);
  case kMallocArena:
    if (size > kArenaSize) {
      errno = ENOMEM;
      return NULL;
    }
    N = malloc_arenas_.size();
    assert(idx_last_arena_ < N);
    p = malloc_arenas_[idx_last_arena_]->Malloc(size);
    if (p != NULL)
      return p;
    for (unsigned i = 0; i < N; ++i) {
      p = malloc_arenas_[i]->Malloc(size);
      if (p != NULL) {
        idx_last_arena_ = i;
        return p;
      }
    }
    idx_last_arena_ = N;
    M = new MallocArena(kArenaSize);
    malloc_arenas_.push_back(M);
    p = M->Malloc(size);
    assert(p != NULL);
    return p;
  }
  // Shouldn't be reachable
  abort();
}

void *MemoryKvStore::ReallocBuffer(void *ptr, size_t size) {
  WriteLockGuard guard(rwlock_);
  return DoRealloc(ptr, size);
}

void *MemoryKvStore::DoRealloc(void *ptr, size_t size) {
  size_t old_size;
  void *new_ptr;

  switch (allocator_) {
  case kMallocLibc:
    return realloc(ptr, size);
  case kMallocArena:
    old_size = GetBufferSize(ptr);
    if (old_size >= size)
      return ptr;

    new_ptr = DoMalloc(size);
    if (!new_ptr) return NULL;
    memcpy(new_ptr, ptr, old_size);
    DoFree(ptr);
    return new_ptr;
  }
  // Shouldn't be reachable
  abort();
}

void MemoryKvStore::FreeBuffer(void *ptr) {
  WriteLockGuard guard(rwlock_);
  DoFree(ptr);
}

void MemoryKvStore::DoFree(void *ptr) {
  MallocArena *M;
  size_t N;

  switch (allocator_) {
  case kMallocLibc:
    free(ptr);
    return;
  case kMallocArena:
    M = MallocArena::GetMallocArena(ptr, kArenaSize);
    M->Free(ptr);
    N = malloc_arenas_.size();
    if ((N > 1) && M->IsEmpty()) {
      for (unsigned i = 0; i < N; ++i) {
        if (malloc_arenas_[i] == M) {
          delete malloc_arenas_[i];
          malloc_arenas_.erase(malloc_arenas_.begin() + i);
          idx_last_arena_ = 0;
          return;
        }
      }
      assert(false);
    }
  }
}

int64_t MemoryKvStore::GetSize(const shash::Any &id) {
  MemoryBuffer mem;
  perf::Inc(counters_.n_getsize);
  if (entries_.Lookup(id, &mem)) {
    // LogCvmfs(kLogKvStore, kLogDebug, "%s is %u B", id.ToString().c_str(),
    //          mem.size);
    return mem.size;
  } else {
    LogCvmfs(kLogKvStore, kLogDebug,
             "miss %s on GetSize",
             id.ToString().c_str());
    return -ENOENT;
  }
}

int64_t MemoryKvStore::GetRefcount(const shash::Any &id) {
  MemoryBuffer mem;
  perf::Inc(counters_.n_getrefcount);
  if (entries_.Lookup(id, &mem)) {
    // LogCvmfs(kLogKvStore, kLogDebug, "%s has refcount %u",
    //          id.ToString().c_str(), mem.refcount);
    return mem.refcount;
  } else {
    LogCvmfs(kLogKvStore, kLogDebug, "miss %s on GetRefcount",
             id.ToString().c_str());
    return -ENOENT;
  }
}

bool MemoryKvStore::IncRef(const shash::Any &id) {
  perf::Inc(counters_.n_incref);
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
  perf::Inc(counters_.n_unref);
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
  perf::Inc(counters_.n_read);
  ReadLockGuard guard(rwlock_);
  if (!entries_.Lookup(id, &mem)) {
    LogCvmfs(kLogKvStore, kLogDebug, "miss %s on Read", id.ToString().c_str());
    return -ENOENT;
  }
  if (offset > mem.size) {
    LogCvmfs(kLogKvStore, kLogDebug, "out of bounds read (%u>%u) on %s",
             offset, mem.size, id.ToString().c_str());
    return 0;
  }
  uint64_t copy_size = min(mem.size - offset, size);
  // LogCvmfs(kLogKvStore, kLogDebug, "copy %u B from offset %u of %s",
  //          copy_size, offset, id.ToString().c_str());
  memcpy(buf, static_cast<char *>(mem.address) + offset, copy_size);
  perf::Xadd(counters_.sz_read, copy_size);
  return copy_size;
}

bool MemoryKvStore::Commit(
  const shash::Any &id,
  const MemoryBuffer &buf
) {
  /*
   * Commit allows overwriting existing entries without free()ing the
   * associated memory buffer. It's assumed that the caller knows about the
   * existing entry and free()ed or realloc()ed themselves.
   * To support both RamCacheManager::CommitTxn and RamCacheManager::OpenFromTxn,
   * we need to be careful about refcounts. If another thread wants to read
   * a cache entry while it's being written (OpenFromTxn put partial data in
   * the kvstore, will be committed again later) the refcount in the kvstore
   * will differ from the refcount in the cache transaction. To avoid leaks,
   * either the caller needs to fetch the cache entry before every write to
   * find the current refcount, or the kvstore can ignore the passed-in
   * refcount if the entry already exists. This implementation does the latter,
   * and as a result it's not possible to directly modify the refcount
   * without a race condition. This is a hint that callers should use the
   * refcount like a lock and not directly modify the numeric value.
   */

  MemoryBuffer mem;
  perf::Inc(counters_.n_commit);
  WriteLockGuard guard(rwlock_);
  LogCvmfs(kLogKvStore, kLogDebug, "commit %s", id.ToString().c_str());
  if (entries_.Lookup(id, &mem)) {
    LogCvmfs(kLogKvStore, kLogDebug, "commit overwrites existing entry");
    used_bytes_ -= mem.size;
    perf::Xadd(counters_.sz_deleted, mem.size);
    counters_.sz_size->Set(used_bytes_);
  } else {
    if (entry_count_ == max_entries_) {
      LogCvmfs(kLogKvStore, kLogDebug, "too many entries in kvstore");
      return false;
    }
    // since this is a new entry, the caller can choose the starting
    // refcount (starting at 1 for pinning, for example)
    mem.refcount = buf.refcount;
  }
  mem.address = buf.address;
  mem.size = buf.size;
  mem.object_type = buf.object_type;
  entries_.Insert(id, mem);
  ++entry_count_;
  assert(SSIZE_MAX - mem.size > used_bytes_);
  used_bytes_ += mem.size;
  counters_.sz_size->Set(used_bytes_);
  perf::Xadd(counters_.sz_committed, mem.size);
  return true;
}

bool MemoryKvStore::Delete(const shash::Any &id) {
  perf::Inc(counters_.n_delete);
  WriteLockGuard guard(rwlock_);
  return DoDelete(id);
}

bool MemoryKvStore::DoDelete(const shash::Any &id) {
  MemoryBuffer buf;
  if (!entries_.Lookup(id, &buf)) {
    LogCvmfs(kLogKvStore, kLogDebug, "miss %s on Delete",
             id.ToString().c_str());
    return false;
  }
  if (buf.refcount > 0) {
    LogCvmfs(kLogKvStore, kLogDebug, "can't delete %s, nonzero refcount",
             id.ToString().c_str());
    return false;
  }
  assert(entry_count_ > 0);
  --entry_count_;
  used_bytes_ -= buf.size;
  counters_.sz_size->Set(used_bytes_);
  perf::Xadd(counters_.sz_deleted, buf.size);
  DoFree(buf.address);
  entries_.Forget(id);
  LogCvmfs(kLogKvStore, kLogDebug, "deleted %s", id.ToString().c_str());
  return true;
}

bool MemoryKvStore::ShrinkTo(size_t size) {
  perf::Inc(counters_.n_shrinkto);
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
    assert(entry_count_ > 0);
    --entry_count_;
    entries_.FilterDelete();
    used_bytes_ -= buf.size;
    perf::Xadd(counters_.sz_shrunk, buf.size);
    counters_.sz_size->Set(used_bytes_);
    DoFree(buf.address);
    LogCvmfs(kLogKvStore, kLogDebug, "delete %s", key.ToString().c_str());
  }
  entries_.FilterEnd();
  LogCvmfs(kLogKvStore, kLogDebug, "shrunk to %u B", used_bytes_);
  return used_bytes_ <= size;
}
