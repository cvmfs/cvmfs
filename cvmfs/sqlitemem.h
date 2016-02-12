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
 * The MemoryManager uses the sqlite hooks to optimize memory allocations.  It
 * is tuned for reading the cvmfs file catalogs.  It provides a page cache of
 * a few MB, a small scratch space and per-database lookaside buffers.
 *
 * It is implemented as a singleton.  GetInstance() will reserve memory blocks,
 * AssignGlobalArenas will set the global sqlite configuration and
 * CleanupInstance frees the memory blocks.  These three routines are not
 * thread-safe.  AssignGlobalArenas has to be called as first sqlite operation,
 * CleanupInstance has to be called after sqlite3_shutdown.  If the singleton
 * is alive, read-only sqlite databases will automatically use it for their
 * lookaside buffers (see sql.h).  Assignment of lookaside buffers is
 * thread-safe.
 */
class MemoryManager {
  FRIEND_TEST(T_Sqlitemem, LookasideBufferArena);
  FRIEND_TEST(T_Sqlitemem, LookasideBuffer);

 public:
  /**
   * In practice, we hardly ever see scratch memory used.  If it is used,
   * the allocation size is <8kB
   */
  const static unsigned kScratchSlotSize = 8192;
  /**
   * Sqlite ensures that never more slots than 2 times the number of threads are
   * used.  In practice, it's much lower.
   */
  const static unsigned kScratchNoSlots = 8;
  /**
   * In total: 64kB scratch memory
   */
  const static unsigned kScratchSize = kScratchSlotSize * kScratchNoSlots;

  /**
   * Empricially, the largest page cache allocation is 1296B.
   */
  const static unsigned kPageCacheSlotSize = 1300;
  /**
   * Number of pages that can be cached.
   */
  const static unsigned kPageCacheNoSlots = 4000;
  /**
   * In total: ~5MB
   */
  const static unsigned kPageCacheSize = kPageCacheSlotSize * kPageCacheNoSlots;

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

  void AssignGlobalArenas();
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
