/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_ITEM_MEM_H_
#define CVMFS_INGESTION_ITEM_MEM_H_

#include <pthread.h>

#include <vector>

#include "malloc_arena.h"

/**
 * To avoid memory fragmentation, allocate the data buffer inside the BlockItem
 * with a separate allocator.
 */
class ItemAllocator {
 public:
  static ItemAllocator *GetInstance() {
    if (instance_ == NULL)
      instance_ = new ItemAllocator();
    return instance_;
  }
  static void CleanupInstance();
  static bool HasInstance() { return instance_ != NULL; }
  ~ItemAllocator();

  void *Malloc(unsigned size);
  void Free(void *ptr);

 private:
  static const unsigned kArenaSize = 128 * 1024 * 1024;  // 128 MB
  static ItemAllocator *instance_;

  ItemAllocator();

  std::vector<MallocArena *> malloc_arenas_;
  pthread_mutex_t lock_;
};  // class ItemAllocator

#endif  // CVMFS_INGESTION_ITEM_MEM_H_
