/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SQLITEMEM_H_
#define CVMFS_SQLITEMEM_H_

#include <inttypes.h>
#include <pthread.h>

#include <cstdlib>
#include <vector>

#include "gtest/gtest_prod.h"

namespace perf {
class Statistics;
}

namespace sqlite {

/**
 * Singleton.  GetInstance / CleanupInstance not thread-safe,
 * rest is thread-safe
 * has to be first instantiated before sqlite stuff runs
 * needs to be cleaned up after sqlite3_shutdown
 */
class MemoryManager {
  FRIEND_TEST(T_Sqlitemem, LookasideBufferArena);
  FRIEND_TEST(T_Sqlitemem, LookasideBuffer);

 public:
  /**
   * In practice, we hardly ever see scratch memory used.  Reserve 8kB for 16
   * slots.
   */
  const static unsigned kScratchSize = 8192 * 16;

  /**
   * Empricially, the largest page cache allocation is 1272B.  Reserve 4MB in
   * total.
   */
  const static unsigned kPageCacheSize = 1280 * 3275;

  /**
   * 32 bytes per slot is an empirically good value so that memory is not wasted
   * (too much) and many allocation fit within the boundary.
   */
  const static unsigned kLookasideSlotSize = 32;

  /**
   * 128 slots with 32bytes accumulate to 4kB.  See LookasideBufferArena below.
   */
  const static unsigned kLookasideSlotsPerDb = 128;

  static MemoryManager *GetInstance() {
    if (instance_ == NULL)
      instance_ = new MemoryManager();
    return instance_;
  }
  static void CleanupInstance();
  static bool HasInstance() { return instance_ != NULL; }
  ~MemoryManager();

  void *AssignLookasideBuffer(void *db);
  void ReleaseLookasideBuffer(void *buffer);

 private:
  static MemoryManager *instance_;

  /**
   * A continues chunk of memory from which fixed-sized chunks are given as
   * lookaside buffers to SQlite database connections.  A bitmap indicates used
   * and free buffers.
   * One arena serves 128 database connections and assuming less than 10000
   * concurrently open catalogs, the number of mmap'd regions stays under 100.
   */
  class LookasideBufferArena {
   public:
    /**
     * Accumulates to 4kB.
     */
    const static unsigned kBufferSize =
      kLookasideSlotSize * kLookasideSlotsPerDb;

    /**
     * Has to be a multiple of the number of bits in the int type.  One arena
     * can serve 128 concurrent sqlite database connections.
     */
    const static unsigned kBuffersPerArena = 128;
    /**
     * 512kB = 128 4kB buffers.
     */
    const static unsigned kArenaSize = kBuffersPerArena * kBufferSize;
    /**
     * Number of bitmap ints for 128 buffers.
     */
    const static unsigned kNoBitmaps = kBuffersPerArena / (sizeof(int) * 8);

    LookasideBufferArena();
    ~LookasideBufferArena();

    void *GetBuffer();
    void PutBuffer(void *buffer);
    bool IsEmpty();
    bool Contains(void *buffer);

   private:
    /**
     * An mmap'd memory block.
     */
    void *arena_;

    /**
     * Indicates free/used slots.  A used slot as its bit set to zero, an unused
     * slot has its bit set to 1.
     */
    int freemap_[kNoBitmaps];
  };

  MemoryManager();

  void *GetLookasideBuffer();
  void PutLookasideBuffer(void *buffer);

  pthread_mutex_t *lock_;

  void *scratch_memory_;
  void *page_cache_memory_;
  std::vector<LookasideBufferArena *> lookaside_buffer_arenas_;
};

}  // namespace sqlite

#endif  // CVMFS_SQLITEMEM_H_
