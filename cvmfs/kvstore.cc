/**
 * This file is part of the CernVM File System.
 */

#include "kvstore.h"

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <string.h>
#include <unistd.h>

#include <algorithm>

#include "util/async.h"
#include "util/concurrency.h"
#include "util/logging.h"

using namespace std;  // NOLINT

namespace {

static inline uint32_t hasher_any(const shash::Any &key) {
  // We'll just do the same thing as hasher_md5, since every hash is at
  // least as large.
  return (uint32_t) * (reinterpret_cast<const uint32_t *>(key.digest) + 1);
}

}  // anonymous namespace

const double MemoryKvStore::kCompactThreshold = 0.8;


MemoryKvStore::MemoryKvStore(unsigned int cache_entries,
                             MemoryAllocator alloc,
                             unsigned alloc_size,
                             perf::StatisticsTemplate statistics)
    : allocator_(alloc)
    , used_bytes_(0)
    , entry_count_(0)
    , max_entries_(cache_entries)
    , entries_(cache_entries, shash::Any(), hasher_any,
               perf::StatisticsTemplate("lru", statistics))
    , heap_(NULL)
    , counters_(statistics) {
  int retval = pthread_rwlock_init(&rwlock_, NULL);
  assert(retval == 0);
  switch (alloc) {
    case kMallocHeap:
      heap_ = new MallocHeap(
          alloc_size, this->MakeCallback(&MemoryKvStore::OnBlockMove, this));
      break;
    default:
      break;
  }
}


MemoryKvStore::~MemoryKvStore() {
  delete heap_;
  pthread_rwlock_destroy(&rwlock_);
}


void MemoryKvStore::OnBlockMove(const MallocHeap::BlockPtr &ptr) {
  bool ok;
  struct AllocHeader a;
  MemoryBuffer buf;

  // must be locked by caller
  assert(ptr.pointer);
  memcpy(&a, ptr.pointer, sizeof(a));
  LogCvmfs(kLogKvStore, kLogDebug, "compaction moved %s to %p",
           a.id.ToString().c_str(), ptr.pointer);
  assert(a.version == 0);
  const bool update_lru = false;
  ok = entries_.Lookup(a.id, &buf, update_lru);
  assert(ok);
  buf.address = static_cast<char *>(ptr.pointer) + sizeof(a);
  ok = entries_.UpdateValue(buf.id, buf);
  assert(ok);
}


bool MemoryKvStore::Contains(const shash::Any &id) {
  MemoryBuffer buf;
  // LogCvmfs(kLogKvStore, kLogDebug, "check buffer %s", id.ToString().c_str());
  const bool update_lru = false;
  return entries_.Lookup(id, &buf, update_lru);
}


int MemoryKvStore::DoMalloc(MemoryBuffer *buf) {
  MemoryBuffer tmp;
  AllocHeader a;

  assert(buf);
  memcpy(&tmp, buf, sizeof(tmp));

  tmp.address = NULL;
  if (tmp.size > 0) {
    switch (allocator_) {
      case kMallocLibc:
        tmp.address = malloc(tmp.size);
        if (!tmp.address)
          return -errno;
        break;
      case kMallocHeap:
        assert(heap_);
        a.id = tmp.id;
        tmp.address = heap_->Allocate(tmp.size + sizeof(a), &a, sizeof(a));
        if (!tmp.address)
          return -ENOMEM;
        tmp.address = static_cast<char *>(tmp.address) + sizeof(a);
        break;
      default:
        abort();
    }
  }

  memcpy(buf, &tmp, sizeof(*buf));
  return 0;
}


void MemoryKvStore::DoFree(MemoryBuffer *buf) {
  AllocHeader a;

  assert(buf);
  if (!buf->address)
    return;
  switch (allocator_) {
    case kMallocLibc:
      free(buf->address);
      return;
    case kMallocHeap:
      heap_->MarkFree(static_cast<char *>(buf->address) - sizeof(a));
      return;
    default:
      abort();
  }
}


bool MemoryKvStore::CompactMemory() {
  double utilization;
  switch (allocator_) {
    case kMallocHeap:
      utilization = heap_->utilization();
      LogCvmfs(kLogKvStore, kLogDebug, "compact requested (%f)", utilization);
      if (utilization < kCompactThreshold) {
        LogCvmfs(kLogKvStore, kLogDebug, "compacting heap");
        heap_->Compact();
        if (heap_->utilization() > utilization)
          return true;
      }
      return false;
    default:
      // the others can't do any compact, so just ignore
      LogCvmfs(kLogKvStore, kLogDebug, "compact requested");
      return false;
  }
}


int64_t MemoryKvStore::GetSize(const shash::Any &id) {
  MemoryBuffer mem;
  perf::Inc(counters_.n_getsize);
  const bool update_lru = false;
  if (entries_.Lookup(id, &mem, update_lru)) {
    // LogCvmfs(kLogKvStore, kLogDebug, "%s is %u B", id.ToString().c_str(),
    //          mem.size);
    return mem.size;
  } else {
    LogCvmfs(kLogKvStore, kLogDebug, "miss %s on GetSize",
             id.ToString().c_str());
    return -ENOENT;
  }
}


int64_t MemoryKvStore::GetRefcount(const shash::Any &id) {
  MemoryBuffer mem;
  perf::Inc(counters_.n_getrefcount);
  const bool update_lru = false;
  if (entries_.Lookup(id, &mem, update_lru)) {
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
    LogCvmfs(kLogKvStore, kLogDebug, "miss %s on Unref", id.ToString().c_str());
    return false;
  }
}


int64_t MemoryKvStore::Read(const shash::Any &id,
                            void *buf,
                            size_t size,
                            size_t offset) {
  MemoryBuffer mem;
  perf::Inc(counters_.n_read);
  ReadLockGuard guard(rwlock_);
  if (!entries_.Lookup(id, &mem)) {
    LogCvmfs(kLogKvStore, kLogDebug, "miss %s on Read", id.ToString().c_str());
    return -ENOENT;
  }
  if (offset > mem.size) {
    LogCvmfs(kLogKvStore, kLogDebug, "out of bounds read (%zu>%zu) on %s",
             offset, mem.size, id.ToString().c_str());
    return 0;
  }
  uint64_t copy_size = std::min(mem.size - offset, size);
  // LogCvmfs(kLogKvStore, kLogDebug, "copy %u B from offset %u of %s",
  //          copy_size, offset, id.ToString().c_str());
  memcpy(buf, static_cast<char *>(mem.address) + offset, copy_size);
  perf::Xadd(counters_.sz_read, copy_size);
  return copy_size;
}


int MemoryKvStore::Commit(const MemoryBuffer &buf) {
  WriteLockGuard guard(rwlock_);
  return DoCommit(buf);
}


int MemoryKvStore::DoCommit(const MemoryBuffer &buf) {
  // we need to be careful about refcounts. If another thread wants to read
  // a cache entry while it's being written (OpenFromTxn put partial data in
  // the kvstore, will be committed again later) the refcount in the kvstore
  // will differ from the refcount in the cache transaction. To avoid leaks,
  // either the caller needs to fetch the cache entry before every write to
  // find the current refcount, or the kvstore can ignore the passed-in
  // refcount if the entry already exists. This implementation does the latter,
  // and as a result it's not possible to directly modify the refcount
  // without a race condition. This is a hint that callers should use the
  // refcount like a lock and not directly modify the numeric value.

  CompactMemory();

  MemoryBuffer mem;
  perf::Inc(counters_.n_commit);
  LogCvmfs(kLogKvStore, kLogDebug, "commit %s", buf.id.ToString().c_str());
  if (entries_.Lookup(buf.id, &mem)) {
    LogCvmfs(kLogKvStore, kLogDebug, "commit overwrites existing entry");
    size_t old_size = mem.size;
    DoFree(&mem);
    used_bytes_ -= old_size;
    counters_.sz_size->Set(used_bytes_);
    --entry_count_;
  } else {
    // since this is a new entry, the caller can choose the starting
    // refcount (starting at 1 for pinning, for example)
    mem.refcount = buf.refcount;
  }
  mem.object_flags = buf.object_flags;
  mem.id = buf.id;
  mem.size = buf.size;
  if (entry_count_ == max_entries_) {
    LogCvmfs(kLogKvStore, kLogDebug, "too many entries in kvstore");
    return -ENFILE;
  }
  if (DoMalloc(&mem) < 0) {
    LogCvmfs(kLogKvStore, kLogDebug, "failed to allocate %s",
             buf.id.ToString().c_str());
    return -EIO;
  }
  assert(SSIZE_MAX - mem.size > used_bytes_);
  memcpy(mem.address, buf.address, mem.size);
  entries_.Insert(buf.id, mem);
  ++entry_count_;
  used_bytes_ += mem.size;
  counters_.sz_size->Set(used_bytes_);
  perf::Xadd(counters_.sz_committed, mem.size);
  return 0;
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

  LogCvmfs(kLogKvStore, kLogDebug, "shrinking to %zu B", size);
  entries_.FilterBegin();
  while (entries_.FilterNext()) {
    if (used_bytes_ <= size)
      break;
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
  LogCvmfs(kLogKvStore, kLogDebug, "shrunk to %zu B", used_bytes_);
  return used_bytes_ <= size;
}
