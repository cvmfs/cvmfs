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

/**
 * The MemoryManager uses the sqlite hooks to optimize memory allocations.  It
 * is tuned for reading the cvmfs file catalogs.  It provides a page cache of
 * a few MB, a small scratch space and per-database lookaside buffers.  It also
 * contains a "general purpose" malloc/free implementation tailored to the
 * behavior of sqlite memory allocation.
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
   * the allocation size is <8kB
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
   * An mmap'd block of general purpose memory for malloc/free in sqlite.  Uses
   * the "boundary-tag system" as described in TAOCP vol 1 section 2.5.
   * Not thread-safe.
   *
   * Returned pointers are 8-byte aligned.  Since a reserved block has a 4 bytes
   * prefix, all reserved blocks must start addresses that end with binary 100.
   * We round up all allocation requests to a multiple of 8.  Reserved blocks
   * are always placed at the upper bound of a free block.  The first block of
   * the arena is appropriately padded and because the upper bound of the
   * initial large free block is at an address that ends on binary 100, all
   * reserved blocks will be aligned correctly.
   *
   * The tag layouts are as follows.  Size is a signed 4 byte integer, previous
   * and next are 4 byte offsets into the arena, linking to the previous and
   * next free blocks in the list of free blocks.
   *
   * Available (free) block:                         Reserved (used) block:
   *
   *       +--------+--------+                             +--------+--------+
   * upper |  size  |      00|                       upper |        |      01|
   *       +--------+--------+                             +--------+--------+
   *       |       ...       |                             |       ...       |
   *       +-----------------+                             +-----------------+
   *       |previous|        |                             |       ...       |
   *       +-----------------+                             +-----------------+
   * lower |  size  |  next  |                       lower |  -size |        |
   *       +--------+--------+                             +--------+--------+
   *        B0             B7                               B0             B7
   *
   *
   * First block of arena_:                          Last "block" of arena_:
   * in the free list but blocked for merging
   *
   *       +--------+#########
   * upper |      01| arena  #
   *       +--------+--------+
   *       |previous|        |
   *       +-----------------+                                multiple of 8
   *       | int(0) | next   | <- head_avail_                       |
   *       +-----------------+                             +--------+########
   * lower | this [+ pad@32] |                       lower |int(-1) |outside#
   *       +--------+--------+                             +--------+########
   *        B0             B7                               B0    B4
   */
  class MallocArena {
   public:
    /**
     * Should be larger than 10 times the largest allocation, which for reading
     * sqlite file catalogs is 64kB.  An arena size of 8MB limits the total
     * number of arenas (mapped blocks) to <40, given typical storage needs for
     * 10,000 open catalogs.
     * Has to be a power of 2MB.  (Has to be a multiple of 8 for alignment.)
     */
    static const unsigned kArenaSize = 8 * 1024 * 1024;

    /**
     * Avoid very small free blocks.  This must be a larger than
     * sizeof(AvailBlockCtl) + sizeof(AvailBlockTag) and a multiple of 8.
     */
    static const int kMinBlockSize = 24;

    /**
     * Returns the MallocArena that houses the destination of ptr.
     */
    static inline MallocArena *GetMallocArena(void *ptr) {
      void *arena = reinterpret_cast<void *>(
        uintptr_t(ptr) & ~(uintptr_t(kArenaSize) - uintptr_t(1)));
      return *reinterpret_cast<MallocArena **>(arena);
    }

    MallocArena();
    static MallocArena *CreateInitialized(unsigned char pattern);
    ~MallocArena();

    void *Malloc(const uint32_t size);
    void Free(void *ptr);
    inline bool Contains(void *ptr) const {
      return GetMallocArena(ptr) == this;
    }
    uint32_t GetSize(void *ptr) const;
    bool IsEmpty() const { return no_reserved_ == 0; }

    /**
     * Round up size to the next larger multiple of 8.  This is used
     * to force 8-byte alignment.
     */
    static inline uint32_t RoundUp8(const uint32_t size) {
      return (size + 7) & ~7;
    }

   private:
    static const char kTagAvail = 0;
    static const char kTagReserved = 1;

    /**
     * Lower boundary of a free block.  Note that the linking of blocks is not
     * necessarily in ascending order but random.
     */
    struct AvailBlockCtl {
      AvailBlockCtl *GetNextPtr(char *base) {
        return reinterpret_cast<AvailBlockCtl *>(base + link_next);
      }
      AvailBlockCtl *GetPrevPtr(char *base) {
        return reinterpret_cast<AvailBlockCtl *>(base + link_prev);
      }
      int32_t ConvertToLink(char *base) {
        return reinterpret_cast<char *>(this) - base;
      }
      void ShrinkTo(int32_t smaller_size) {
        size = smaller_size;
        new (AvailBlockTag::GetTagLocation(this)) AvailBlockTag(smaller_size);
      }
      int32_t size;  // always positive
      int32_t link_next;  // offset in the arena; saves 4 bytes on 64bit archs
      int32_t link_prev;  // offset in the arena; saves 4 bytes on 64bit archs
    };

    /**
     * 8 bytes upper boundary of a free block.
     */
    struct AvailBlockTag {
      explicit AvailBlockTag(int32_t s) : size(s), tag(kTagAvail) { }
      static void *GetTagLocation(AvailBlockCtl *block) {
        return
          reinterpret_cast<char *>(block) + block->size - sizeof(AvailBlockTag);
      }
      int32_t size;
      char padding[3];
      char tag;
    };

    /**
     * Lower boundary of a reserved block: a negative size to distinguish from
     * the lower boundary of a free block.
     */
    struct ReservedBlockCtl {
     public:
      explicit ReservedBlockCtl(int32_t s) : size_(-s) {
        char *base = reinterpret_cast<char *>(this);
        *(base + s - 1) = kTagReserved;
      }
      int32_t size() const { assert(size_ <= 0);  return -size_; }

     private:
      int32_t size_;  // always negative
    };

    void UnlinkAvailBlock(AvailBlockCtl *block);
    void EnqueueAvailBlock(AvailBlockCtl *block);
    AvailBlockCtl *FindAvailBlock(const int32_t block_size);
    void *ReserveBlock(AvailBlockCtl *block, int32_t block_size);

    /**
     * Starts with the address of the MallocArena object followed by a
     * head_avail_ block and ends with a -1 guard integer to mimic a reserved
     * end block.  The arena is aligned at a multiple of kArenaSize.  Therefore,
     * a pointer pointing into it can get the corresponding MallocArena object
     * in O(1).
     */
    char *arena_;  ///< The memory block
    /**
     * Head of the list of available blocks.  Located at the beginning of the
     * arena_.  The only "available" block with size 0 and with the upper tag
     * field set to "reserved", so that it is not merged with another free'd
     * block.
     */
    AvailBlockCtl *head_avail_;
    AvailBlockCtl *rover_;  ///< The free block where the next search starts.
    uint32_t no_reserved_;
  };


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
    static const unsigned kBufferSize =
      kLookasideSlotSize * kLookasideSlotsPerDb;

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
  void *scratch_memory_;
  void *page_cache_memory_;
  std::vector<LookasideBufferArena *> lookaside_buffer_arenas_;
  std::vector<MallocArena *> malloc_arenas_;
  /**
   * Where the last successful allocation took place.
   */
  unsigned idx_last_arena_;
};  // class SqliteMemoryManager

#endif  // CVMFS_SQLITEMEM_H_
