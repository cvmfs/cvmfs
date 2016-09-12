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
#include "util/async.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

MemoryKvStore::MemoryKvStore(
  unsigned int cache_entries,
  const string &name,
  MemoryAllocator alloc,
  unsigned alloc_size,
  perf::Statistics *statistics)
  : allocator_(alloc)
  , used_bytes_(0)
  , idx_last_arena_(0)
  , entry_count_(0)
  , max_entries_(cache_entries)
  , entries_(cache_entries, shash::Any(), lru::hasher_any,
      statistics, name)
  , heap_(NULL)
  , counters_(statistics, name + ".lru") {
  int retval = pthread_rwlock_init(&rwlock_, NULL);
  assert(retval == 0);
  switch (alloc) {
  case kMallocArena:
    malloc_arenas_.push_back(new MallocArena(kArenaSize));
    break;
  case kMallocHeap:
    heap_ = new MallocHeap(alloc_size,
        this->MakeCallback(&MemoryKvStore::OnBlockMove, this));
    break;
  default:
    break;
  }
}

MemoryKvStore::~MemoryKvStore() {
  for (size_t i = 0; i < malloc_arenas_.size(); ++i) {
    delete malloc_arenas_[i];
  }
  if (heap_) {
    delete heap_;
  }
  pthread_rwlock_destroy(&rwlock_);
}

void MemoryKvStore::OnBlockMove(const MallocHeap::BlockPtr &ptr) {
  bool ok;
  shash::Any id;
  MemoryBuffer buf;

  // must be locked by caller
  assert(ptr.pointer);
  memcpy(&id, ptr.pointer, sizeof(id));
  LogCvmfs(kLogKvStore, kLogDebug, "compaction moved %s to %p",
    id.ToString().c_str(), ptr.pointer);

  ok = entries_.Lookup(id, &buf);
  assert(ok);
  buf.address = static_cast<char *>(ptr.pointer) + sizeof(id);
  ok = DoCommit(buf);
  assert(ok);
}

bool MemoryKvStore::Contains(const shash::Any &id) {
  MemoryBuffer buf;
  // LogCvmfs(kLogKvStore, kLogDebug, "check buffer %s", id.ToString().c_str());
  return entries_.Lookup(id, &buf);
}

size_t MemoryKvStore::GetBufferSize(MemoryBuffer *buf) {
  assert(buf);
  switch (allocator_) {
  case kMallocArena:
    return MallocArena::GetMallocArena(buf->address, kArenaSize)
      ->GetSize(buf->address);
  case kMallocHeap:
    return heap_->GetSize(static_cast<char *>(buf->address) - sizeof(buf->id));
  default:
    abort();  // shouldn't be reachable
  }
}

int MemoryKvStore::DoMalloc(MemoryBuffer *buf) {
  size_t N;
  MallocArena *M;
  MemoryBuffer tmp;

  assert(buf);
  memcpy(&tmp, buf, sizeof(tmp));

  switch (allocator_) {
  case kMallocLibc:
    tmp.address = malloc(tmp.size);
    if (!tmp.address) return -errno;
    break;
  case kMallocArena:
    if (tmp.size > kArenaSize - 2*1024*1024) {
      return -ENOMEM;
    } else if (tmp.size == 0) {
      tmp.address = NULL;
      break;
    }
    N = malloc_arenas_.size();
    assert(idx_last_arena_ < N);
    tmp.address = malloc_arenas_[idx_last_arena_]
      ->Malloc(tmp.size);
    if (tmp.address != NULL) break;
    for (unsigned i = 0; i < N; ++i) {
      tmp.address = malloc_arenas_[i]->Malloc(tmp.size);
      if (tmp.address != NULL) {
        idx_last_arena_ = i;
        break;
      }
    }
    idx_last_arena_ = N;
    M = new MallocArena(kArenaSize);
    assert(M != NULL);
    malloc_arenas_.push_back(M);
    tmp.address = M->Malloc(tmp.size);
    assert(tmp.address != NULL);
    break;
  case kMallocHeap:
    assert(heap_);
    if (tmp.size == 0) {
      tmp.address = NULL;
      break;
    }
    tmp.address = heap_->Allocate(tmp.size + sizeof(tmp.id),
      &tmp.id, sizeof(tmp.id));
    if (!tmp.address) return -ENOMEM;
    tmp.address = static_cast<char *>(tmp.address) + sizeof(tmp.id);
    break;
  }
  memcpy(buf, &tmp, sizeof(*buf));
  return 0;
}

int MemoryKvStore::DoRealloc(MemoryBuffer *buf, size_t size) {
  MemoryBuffer tmp;
  int rc;

  assert(buf);
  memcpy(&tmp, buf, sizeof(tmp));
  tmp.size = size;

  switch (allocator_) {
  case kMallocLibc:
    tmp.address = realloc(tmp.address, tmp.size);
    if (!tmp.address && tmp.size > 0) return -errno;
    break;
  case kMallocArena:
  case kMallocHeap:
    if (size == 0) {
      DoFree(buf);
      return 0;
    }
    if (buf->size >= size) return 0;

    rc = DoMalloc(&tmp);
    if (rc < 0) return rc;
    if (buf->address) {
      memcpy(tmp.address, buf->address, buf->size);
      DoFree(buf);
    }
    break;
  }
  memcpy(buf, &tmp, sizeof(*buf));
  return 0;
}

void MemoryKvStore::DoFree(MemoryBuffer *buf) {
  MallocArena *M;
  size_t N;

  assert(buf);
  if (!buf->address) return;
  switch (allocator_) {
  case kMallocLibc:
    free(buf->address);
    return;
  case kMallocArena:
    M = MallocArena::GetMallocArena(buf->address, kArenaSize);
    M->Free(buf->address);
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
  case kMallocHeap:
    heap_->MarkFree(static_cast<char *>(buf->address) - sizeof(buf->id));
    return;
  }
}

bool MemoryKvStore::Cleanup() {
  double utilization;
  switch (allocator_) {
  case kMallocHeap:
    utilization = heap_->utilization();
    LogCvmfs(kLogKvStore, kLogDebug, "cleanup requested (%f)", utilization);
    if (utilization < 0.8) {
      LogCvmfs(kLogKvStore, kLogDebug, "compacting heap");
      heap_->Compact();
      if (heap_->utilization() > utilization) return true;
    }
    return false;
  default:
    // the others can't do any cleanup, so just ignore
    LogCvmfs(kLogKvStore, kLogDebug, "cleanup requested");
    return false;
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

bool MemoryKvStore::Commit(const MemoryBuffer &buf) {
  WriteLockGuard guard(rwlock_);
  return DoCommit(buf);
}

bool MemoryKvStore::DoCommit(const MemoryBuffer &buf) {
  /*
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

  Cleanup();

  MemoryBuffer mem;
  perf::Inc(counters_.n_commit);
  LogCvmfs(kLogKvStore, kLogDebug, "commit %s", buf.id.ToString().c_str());
  if (entries_.Lookup(buf.id, &mem)) {
    LogCvmfs(kLogKvStore, kLogDebug, "commit overwrites existing entry");
    size_t old_size = mem.size;
    if (DoRealloc(&mem, buf.size) < 0) {
      LogCvmfs(kLogKvStore, kLogDebug, "failed to reallocate %s",
        buf.id.ToString().c_str());
      return false;
    }
    used_bytes_ -= old_size;
    counters_.sz_size->Set(used_bytes_);
  } else {
    if (entry_count_ == max_entries_) {
      LogCvmfs(kLogKvStore, kLogDebug, "too many entries in kvstore");
      return false;
    }
    mem.size = buf.size;
    if (DoMalloc(&mem) < 0) {
      LogCvmfs(kLogKvStore, kLogDebug, "failed to allocate %s",
        buf.id.ToString().c_str());
      return false;
    }
    // since this is a new entry, the caller can choose the starting
    // refcount (starting at 1 for pinning, for example)
    mem.refcount = buf.refcount;
  }
  memcpy(mem.address, buf.address, min(mem.size, buf.size));
  mem.object_type = buf.object_type;
  mem.id = buf.id;
  entries_.Insert(buf.id, mem);
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
  DoFree(&buf);
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
    DoFree(&buf);
    LogCvmfs(kLogKvStore, kLogDebug, "delete %s", key.ToString().c_str());
  }
  entries_.FilterEnd();
  LogCvmfs(kLogKvStore, kLogDebug, "shrunk to %u B", used_bytes_);
  return used_bytes_ <= size;
}
