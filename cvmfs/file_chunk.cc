/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "file_chunk.h"

#include <cassert>

#include "murmur.h"

using namespace std;  // NOLINT

static inline uint32_t hasher_uint64t(const uint64_t &value) {
  return MurmurHash2(&value, sizeof(value), 0x07387a4f);
}

void ChunkTables::InitLocks() {
  lock =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock, NULL);
  assert(retval == 0);

  for (unsigned i = 0; i < kNumHandleLocks; ++i) {
    pthread_mutex_t *m =
      reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
    int retval = pthread_mutex_init(m, NULL);
    assert(retval == 0);
    handle_locks.PushBack(m);
  }
}


void ChunkTables::InitHashmaps() {
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


ChunkTables &ChunkTables::operator= (const ChunkTables &other) {
  if (&other == this)
    return *this;

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
}


pthread_mutex_t *ChunkTables::Handle2Lock(const uint64_t handle) const {
  const uint32_t hash = hasher_uint64t(handle);
  const double bucket =
    static_cast<double>(hash) * static_cast<double>(kNumHandleLocks) /
    static_cast<double>((uint32_t)(-1));
  return handle_locks.At((uint32_t)bucket % kNumHandleLocks);
}
