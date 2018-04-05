/**
 * This file is part of the CernVM File System.
 */

#include "item_mem.h"

#include <cassert>
#include <cstdlib>

#include "util_concurrency.h"


ItemAllocator *ItemAllocator::instance_ = NULL;

void ItemAllocator::CleanupInstance() {
  delete instance_;
  instance_ = NULL;
}


void ItemAllocator::Free(void *ptr) {
  MutexLockGuard guard(lock_);

  MallocArena *M = MallocArena::GetMallocArena(ptr, kArenaSize);
  M->Free(ptr);
  unsigned N = malloc_arenas_.size();
  if ((N > 1) && M->IsEmpty()) {
    for (unsigned i = 0; i < N; ++i) {
      if (malloc_arenas_[i] == M) {
        delete malloc_arenas_[i];
        malloc_arenas_.erase(malloc_arenas_.begin() + i);
        return;
      }
    }
    assert(false);
  }
}


ItemAllocator::ItemAllocator() {
  int retval = pthread_mutex_init(&lock_, NULL);
  assert(retval == 0);

  malloc_arenas_.push_back(new MallocArena(kArenaSize));
}


ItemAllocator::~ItemAllocator() {
  for (unsigned i = 0; i < malloc_arenas_.size(); ++i)
    delete malloc_arenas_[i];
  pthread_mutex_destroy(&lock_);
}


void *ItemAllocator::Malloc(unsigned size) {
  MutexLockGuard guard(lock_);

  // We expect that blocks are allocated and free'd roughly in FIFO order
  unsigned N = malloc_arenas_.size();
  void *p = malloc_arenas_[N - 1]->Malloc(size);
  if (p == NULL) {
    MallocArena *M = new MallocArena(kArenaSize);
    malloc_arenas_.push_back(M);
    p = M->Malloc(size);
    assert(p != NULL);
  }
  return p;
}
