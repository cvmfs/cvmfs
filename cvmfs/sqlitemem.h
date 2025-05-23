/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SQLITEMEM_H_
#define CVMFS_SQLITEMEM_H_

#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>

#include <cassert>
#include <cstdlib>
#include <vector>

#include "duplex_sqlite3.h"
#include "gtest/gtest_prod.h"

class MallocArena;


/**
 * The MemoryManager uses the sqlite hooks to optimize memory allocations.  It
 * is tuned for reading the cvmfs file catalogs.  It provides a page cache of
 * a few MB and per-database lookaside buffers.  It also contains a
 * "general purpose" malloc/free implementation tailored to the behavior of
 * sqlite memory allocation.
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
class SqliteMemoryManager {
  FRIEND_TEST(T_Sqlitemem, LookasideBuffer);
  FRIEND_TEST(T_Sqlitemem, Malloc);
  FRIEND_TEST(T_Sqlitemem, Realloc);
  FRIEND_TEST(T_Sqlitemem, ReallocStress);

 public:
  /**
   * In practice, we hardly ever see scratch memory used.  If it is used,
   * the allocation size is <8kB.
   * NOTE: The scratch memory allocator has been removed from sqlite as of
   * version 3.21.
   */
  static const unsigned kScratchSlotSize = 8192;
  /**
   * Sqlite ensures that never more slots than 2 times the number of threads are
   * used.  In practice, it's much lower.
   */
  static const unsigned kScratchNoSlots = 8;
  /**
   * In total: 64kB scratch memory
   */
  static const unsigned kScratchSize = kScratchSlotSize * kScratchNoSlots;

  /**
   * Empricially, the largest page cache allocation is 1296B.
   */
  static const unsigned kPageCacheSlotSize = 1300;
  /**
   * Number of pages that can be cached.
   */
  static const unsigned kPageCacheNoSlots = 4000;
  /**
   * In total: ~5MB
   */
  static const unsigned kPageCacheSize = kPageCacheSlotSize * kPageCacheNoSlots;

  /**
   * 32 bytes per slot is an empirically good value so that memory is not wasted
   * (too much) and many allocations fit within the boundary.
   */
  static const unsigned kLookasideSlotSize = 32;
  /**
   * 128 slots with 32bytes accumulate to 4kB.  See LookasideBufferArena below.
   */
  static const unsigned kLookasideSlotsPerDb = 128;


  /**
   * A continuous chunk of memory from which fixed-sized chunks are given as
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
    static const unsigned kBufferSize = kLookasideSlotSize
                                        * kLookasideSlotsPerDb;

    /**
     * Has to be a multiple of the number of bits in the int type.  One arena
     * can serve 128 concurrent sqlite database connections.
     */
    static const unsigned kBuffersPerArena = 128;
    /**
     * 512kB = 128 4kB buffers.
     */
    static const unsigned kArenaSize = kBuffersPerArena * kBufferSize;
    /**
     * Number of bitmap ints for 128 buffers.
     */
    static const unsigned kNoBitmaps = kBuffersPerArena / (sizeof(int) * 8);

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
     * Indicates free/used slots.  A used slot has its bit set to zero, an
     * unused slot has its bit set to 1.
     */
    int freemap_[kNoBitmaps];
  };


  static SqliteMemoryManager *GetInstance() {
    if (instance_ == NULL)
      instance_ = new SqliteMemoryManager();
    return instance_;
  }
  static void CleanupInstance();
  static bool HasInstance() { return instance_ != NULL; }
  ~SqliteMemoryManager();

  void AssignGlobalArenas();
  void *AssignLookasideBuffer(sqlite3 *db);
  void ReleaseLookasideBuffer(void *buffer);

 private:
  /**
   * Should be larger than 10 times the largest allocation, which for reading
   * sqlite file catalogs is 64kB.  An arena size of 8MB limits the total
   * number of arenas (mapped blocks) to <40, given typical storage needs for
   * 10,000 open catalogs.  Has to be a power of 2MB (see MallocArena).
   */
  static const unsigned kArenaSize = 8 * 1024 * 1024;
  static SqliteMemoryManager *instance_;

  /**
   * SQlite memory callbacks need to be static because they are referenced by
   * C function pointers.  See https://www.sqlite.org/c3ref/mem_methods.html
   */
  static void *xMalloc(int size);
  static void xFree(void *ptr);
  static void *xRealloc(void *ptr, int new_size);
  static int xSize(void *ptr);
  static int xRoundup(int size);
  static int xInit(void *app_data);
  static void xShutdown(void *app_data);


  SqliteMemoryManager();

  void *GetLookasideBuffer();
  void PutLookasideBuffer(void *buffer);

  void *GetMemory(int size);
  void PutMemory(void *ptr);
  int GetMemorySize(void *ptr);

  pthread_mutex_t lock_;

  /**
   * True if AssignGlobalArenas was called and the memory manager is used by
   * sqlite.
   */
  bool assigned_;

  /**
   * The standard memory allocator used by sqlite3.  Saved in order to reset on
   * destruction.
   */
  struct sqlite3_mem_methods sqlite3_mem_vanilla_;

  struct sqlite3_mem_methods mem_methods_;
  void *page_cache_memory_;
  std::vector<LookasideBufferArena *> lookaside_buffer_arenas_;
  std::vector<MallocArena *> malloc_arenas_;
  /**
   * Where the last successful allocation took place.
   */
  unsigned idx_last_arena_;
};  // class SqliteMemoryManager

#endif  // CVMFS_SQLITEMEM_H_
