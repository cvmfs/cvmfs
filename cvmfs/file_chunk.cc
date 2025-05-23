/**
 * This file is part of the CernVM File System.
 */


#include "file_chunk.h"

#include <cassert>

#include "util/murmur.hxx"
#include "util/platform.h"

using namespace std;  // NOLINT

static inline uint32_t hasher_uint64t(const uint64_t &value) {
  return MurmurHash2(&value, sizeof(value), 0x07387a4f);
}


//------------------------------------------------------------------------------


unsigned FileChunkReflist::FindChunkIdx(const uint64_t off) {
  assert(list && (list->size() > 0));
  unsigned idx_low = 0;
  unsigned idx_high = list->size() - 1;
  unsigned chunk_idx = idx_high / 2;
  while (idx_low < idx_high) {
    if (static_cast<uint64_t>(list->AtPtr(chunk_idx)->offset()) > off) {
      assert(idx_high > 0);
      idx_high = chunk_idx - 1;
    } else {
      if ((chunk_idx == list->size() - 1)
          || (static_cast<uint64_t>(list->AtPtr(chunk_idx + 1)->offset())
              > off)) {
        break;
      }
      idx_low = chunk_idx + 1;
    }
    chunk_idx = idx_low + (idx_high - idx_low) / 2;
  }
  return chunk_idx;
}


/**
 * Returns a consistent hash over hashes of the chunks. Used by libcvmfs.
 */
shash::Any FileChunkReflist::HashChunkList() {
  shash::Algorithms algo = list->AtPtr(0)->content_hash().algorithm;
  shash::ContextPtr ctx(algo);
  ctx.buffer = alloca(ctx.size);
  shash::Init(ctx);
  for (unsigned i = 0; i < list->size(); ++i) {
    shash::Update(
        list->AtPtr(i)->content_hash().digest, shash::kDigestSizes[algo], ctx);
  }
  shash::Any result(algo);
  shash::Final(ctx, &result);
  return result;
}


//------------------------------------------------------------------------------


void ChunkTables::InitLocks() {
  lock = reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock, NULL);
  assert(retval == 0);

  for (unsigned i = 0; i < kNumHandleLocks; ++i) {
    pthread_mutex_t *m = reinterpret_cast<pthread_mutex_t *>(
        smalloc(sizeof(pthread_mutex_t)));
    int retval = pthread_mutex_init(m, NULL);
    assert(retval == 0);
    handle_locks.PushBack(m);
  }
}


void ChunkTables::InitHashmaps() {
  handle2uniqino.Init(16, 0, hasher_uint64t);
  handle2fd.Init(16, 0, hasher_uint64t);
  inode2chunks.Init(16, 0, hasher_uint64t);
  inode2references.Init(16, 0, hasher_uint64t);
}


ChunkTables::ChunkTables() {
  next_handle = 2;
  version = kVersion;
  InitLocks();
  InitHashmaps();
}


ChunkTables::~ChunkTables() {
  pthread_mutex_destroy(lock);
  free(lock);
  for (unsigned i = 0; i < kNumHandleLocks; ++i) {
    pthread_mutex_destroy(handle_locks.At(i));
    free(handle_locks.At(i));
  }
}


ChunkTables::ChunkTables(const ChunkTables &other) {
  version = kVersion;
  InitLocks();
  InitHashmaps();
  CopyFrom(other);
}


ChunkTables &ChunkTables::operator=(const ChunkTables &other) {
  if (&other == this)
    return *this;

  handle2uniqino.Clear();
  handle2fd.Clear();
  inode2chunks.Clear();
  inode2references.Clear();
  CopyFrom(other);
  return *this;
}


void ChunkTables::CopyFrom(const ChunkTables &other) {
  assert(version == other.version);
  next_handle = other.next_handle;
  inode2references = other.inode2references;
  inode2chunks = other.inode2chunks;
  handle2fd = other.handle2fd;
  handle2uniqino = other.handle2uniqino;
}


pthread_mutex_t *ChunkTables::Handle2Lock(const uint64_t handle) const {
  const uint32_t hash = hasher_uint64t(handle);
  const double bucket = static_cast<double>(hash)
                        * static_cast<double>(kNumHandleLocks)
                        / static_cast<double>((uint32_t)(-1));
  return handle_locks.At((uint32_t)bucket % kNumHandleLocks);
}


//------------------------------------------------------------------------------


SimpleChunkTables::SimpleChunkTables() {
  lock_ = reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


SimpleChunkTables::~SimpleChunkTables() {
  for (unsigned i = 0; i < fd_table_.size(); ++i) {
    delete fd_table_[i].chunk_reflist.list;
  }
  pthread_mutex_destroy(lock_);
  free(lock_);
}


int SimpleChunkTables::Add(FileChunkReflist chunks) {
  assert(chunks.list != NULL);
  OpenChunks new_entry;
  new_entry.chunk_reflist = chunks;
  new_entry.chunk_fd = new ChunkFd();
  unsigned i = 0;
  Lock();
  for (; i < fd_table_.size(); ++i) {
    if (fd_table_[i].chunk_reflist.list == NULL) {
      fd_table_[i] = new_entry;
      Unlock();
      return i;
    }
  }
  fd_table_.push_back(new_entry);
  Unlock();
  return i;
}


SimpleChunkTables::OpenChunks SimpleChunkTables::Get(int fd) {
  OpenChunks result;
  if (fd < 0)
    return result;

  unsigned idx = static_cast<unsigned>(fd);
  Lock();
  if (idx < fd_table_.size())
    result = fd_table_[idx];
  Unlock();
  return result;
}


void SimpleChunkTables::Release(int fd) {
  if (fd < 0)
    return;

  Lock();
  unsigned idx = static_cast<unsigned>(fd);
  if (idx >= fd_table_.size()) {
    Unlock();
    return;
  }

  delete fd_table_[idx].chunk_reflist.list;
  fd_table_[idx].chunk_reflist.list = NULL;
  fd_table_[idx].chunk_reflist.path.Assign("", 0);
  delete fd_table_[idx].chunk_fd;
  fd_table_[idx].chunk_fd = NULL;
  while (!fd_table_.empty() && (fd_table_.back().chunk_reflist.list == NULL)) {
    fd_table_.pop_back();
  }
  Unlock();
}
