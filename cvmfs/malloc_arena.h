/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_MALLOC_ARENA_H_
#define CVMFS_MALLOC_ARENA_H_

#include <inttypes.h>
#include <stdint.h>

#include <cassert>
#include <new>

// TODO(jblomer): the arena size could be a template parameter.  In order to
// reduce code duplication, all functions not requiring the arena size could be
// moved to a base class.

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
 *
 * The arena size has to be a power of 2MB.  It also has to be a multiple of 8
 * for alignment reasons.  It needs to be <= 512MB.
 */
class MallocArena {
 public:
  /**
   * Avoid very small free blocks.  This must be a larger than
   * sizeof(AvailBlockCtl) + sizeof(AvailBlockTag) and a multiple of 8.
   */
  static const int kMinBlockSize = 24;

  /**
   * Returns the MallocArena that houses the destination of ptr.
   */
  static inline MallocArena *GetMallocArena(void *ptr, unsigned arena_size) {
    // NOLINTNEXTLINE(performance-no-int-to-ptr)
    void *arena = reinterpret_cast<void *>(
        uintptr_t(ptr) & ~(uintptr_t(arena_size) - uintptr_t(1)));
    return *reinterpret_cast<MallocArena **>(arena);
  }

  explicit MallocArena(unsigned arena_size);
  static MallocArena *CreateInitialized(unsigned arena_size,
                                        unsigned char pattern);
  ~MallocArena();

  void *Malloc(const uint32_t size);
  void Free(void *ptr);
  inline bool Contains(void *ptr) const {
    return GetMallocArena(ptr, arena_size_) == this;
  }
  uint32_t GetSize(void *ptr) const;
  bool IsEmpty() const { return no_reserved_ == 0; }

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
      return static_cast<int>(reinterpret_cast<char *>(this) - base);
    }
    void ShrinkTo(int32_t smaller_size) {
      size = smaller_size;
      new (AvailBlockTag::GetTagLocation(this)) AvailBlockTag(smaller_size);
    }
    int32_t size;       // always positive
    int32_t link_next;  // offset in the arena; saves 4 bytes on 64bit archs
    int32_t link_prev;  // offset in the arena; saves 4 bytes on 64bit archs
  };

  /**
   * 8 bytes upper boundary of a free block.
   */
  struct AvailBlockTag {
    explicit AvailBlockTag(int32_t s) : size(s), tag(kTagAvail) { }
    static void *GetTagLocation(AvailBlockCtl *block) {
      return reinterpret_cast<char *>(block) + block->size
             - sizeof(AvailBlockTag);
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
    int32_t size() const {
      assert(size_ <= 0);
      return -size_;
    }

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
   * end block.  The arena is aligned at a multiple of arena_size.  Therefore,
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
  unsigned arena_size_;
};  // class MallocArena

#endif  // CVMFS_MALLOC_ARENA_H_
