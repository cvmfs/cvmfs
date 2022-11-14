/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_ITEM_MEM_H_
#define CVMFS_INGESTION_ITEM_MEM_H_

#include <pthread.h>

#include <cassert>
#include <vector>

#include "malloc_arena.h"
#include "util/atomic.h"

/**
 * To avoid memory fragmentation, allocate the data buffer inside the BlockItem
 * with a separate allocator.
 */
class ItemAllocator {
 public:
  ItemAllocator();
  ~ItemAllocator();

  void *Malloc(unsigned size);
  void Free(void *ptr);

  static int64_t total_allocated() { return atomic_read64(&total_allocated_); }

 private:
  static const unsigned kArenaSize = 128 * 1024 * 1024;  // 128 MB
  static atomic_int64 total_allocated_;

  std::vector<MallocArena *> malloc_arenas_;
  /**
   * Where the last successful allocation took place.
   */
  unsigned idx_last_arena_;
  pthread_mutex_t lock_;
};  // class ItemAllocator

#endif  // CVMFS_INGESTION_ITEM_MEM_H_
