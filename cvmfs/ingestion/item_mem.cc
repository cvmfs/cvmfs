/**
 * This file is part of the CernVM File System.
 */

#include "item_mem.h"

#include <cassert>
#include <cstdlib>

#include "util/concurrency.h"
#include "util/exception.h"

atomic_int64 ItemAllocator::total_allocated_ = 0;


void ItemAllocator::Free(void *ptr) {
  const MutexLockGuard guard(lock_);

  MallocArena *M = MallocArena::GetMallocArena(ptr, kArenaSize);
  M->Free(ptr);
  const unsigned N = malloc_arenas_.size();
  if ((N > 1) && M->IsEmpty()) {
    for (unsigned i = 0; i < N; ++i) {
      if (malloc_arenas_[i] == M) {
        delete malloc_arenas_[i];
        atomic_xadd64(&total_allocated_, -static_cast<int>(kArenaSize));
        malloc_arenas_.erase(malloc_arenas_.begin() + i);
        idx_last_arena_ = 0;
        return;
      }
    }
    PANIC(NULL);
  }
}


ItemAllocator::ItemAllocator() : idx_last_arena_(0) {
  const int retval = pthread_mutex_init(&lock_, NULL);
  assert(retval == 0);

  malloc_arenas_.push_back(new MallocArena(kArenaSize));
  atomic_xadd64(&total_allocated_, kArenaSize);
}


ItemAllocator::~ItemAllocator() {
  for (unsigned i = 0; i < malloc_arenas_.size(); ++i) {
    atomic_xadd64(&total_allocated_, -static_cast<int>(kArenaSize));
    delete malloc_arenas_[i];
  }
  pthread_mutex_destroy(&lock_);
}


void *ItemAllocator::Malloc(unsigned size) {
  const MutexLockGuard guard(lock_);

  void *p = malloc_arenas_[idx_last_arena_]->Malloc(size);
  if (p != NULL)
    return p;
  const unsigned N = malloc_arenas_.size();
  for (unsigned i = 0; i < N; ++i) {
    p = malloc_arenas_[i]->Malloc(size);
    if (p != NULL) {
      idx_last_arena_ = i;
      return p;
    }
  }
  idx_last_arena_ = N;
  MallocArena *M = new MallocArena(kArenaSize);
  atomic_xadd64(&total_allocated_, kArenaSize);
  malloc_arenas_.push_back(M);
  p = M->Malloc(size);
  assert(p != NULL);
  return p;
}
