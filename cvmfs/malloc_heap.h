/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_MALLOC_HEAP_H_
#define CVMFS_MALLOC_HEAP_H_

#include <inttypes.h>
#include <stdint.h>

#include <cstdlib>

#include "util/async.h"

/**
 * Fills an ever-growing heap.  Free calls simply mark a block as free but
 * allocation continues to take place at the top of the heap.  Hence Realloc()
 * is inefficient.  For the common pattern of doubling a buffer with realloc,
 * twice the final buffer size is required.
 *
 * The allocator is copying and can collect garbage.  In order to react on the
 * move of a block, a callback can be registered that gets called with the new
 * pointer address.  The user of the MallocHeap has to make sure to identify
 * any block based on the first bytes.  Therefore, allocation requires these
 * first bytes, a user-defined "header" if you will.
 * Note that during a Compact() not even reading from any of the pointers is
 * allowed!
 *
 * MallocHeap is used by the in-memory object cache.  The header is the
 * object's content hash, so the cache manager can identify any block in its
 * hash table and move the pointers when MallocHeap runs a garbage collection.
 *
 * All memory blocks are 8-byte aligned and they have an 8-byte header
 * containing their size.  The size is negative for free blocks.
 */
class MallocHeap {
 public:
  struct BlockPtr {
    BlockPtr() : pointer(NULL) { }
    explicit BlockPtr(void *p) : pointer(p) { }
    void *pointer;
  };

  // Pointer to the callback method invoked for each memory block that gets
  // compacted.
  typedef Callbackable<BlockPtr>::CallbackTN *CallbackPtr;

  MallocHeap(uint64_t capacity, CallbackPtr callback_ptr);
  ~MallocHeap();

  void *Allocate(uint64_t size, void *header, unsigned header_size);
  void *Expand(void *block, uint64_t new_size);
  void MarkFree(void *block);
  uint64_t GetSize(void *block);
  void Compact();

  inline uint64_t num_blocks() { return num_blocks_; }
  inline uint64_t used_bytes() { return gauge_; }
  inline uint64_t stored_bytes() { return stored_; }
  inline uint64_t compacted_bytes() {
    return stored_ + num_blocks_ * sizeof(Tag);
  }
  inline uint64_t capacity() { return capacity_; }
  inline double utilization() {
    return static_cast<double>(stored_) / static_cast<double>(gauge_);
  }
  bool HasSpaceFor(uint64_t nbytes);

 private:
  /**
   * Minimum number of bytes of the heap.
   */
  static const unsigned kMinCapacity = 1024;

  /**
   * Prepends every block.  The size does not include the size of the tag.  The
   * size of the Tag structure has to be a multiple of 8 for alignment.
   */
  struct Tag {
    Tag() : size(0) { }
    explicit Tag(int64_t s) : size(s) { }
    inline uint64_t GetSize() {
      if (size < 0)
        return -size;
      return size;
    }
    inline bool IsFree() { return size < 0; }
    inline Tag *JumpToNext() {
      return reinterpret_cast<Tag *>(reinterpret_cast<unsigned char *>(this)
                                     + sizeof(Tag) + GetSize());
    }
    inline unsigned char *GetBlock() {
      return reinterpret_cast<unsigned char *>(this) + sizeof(Tag);
    }
    /**
     * Positive for reserved blocks, negative for free blocks.
     */
    int64_t size;
  };

  /**
   * Invoked when a block is moved during compact.
   */
  CallbackPtr callback_ptr_;
  /**
   * Total size of mmap'd arena.
   */
  uint64_t capacity_;
  /**
   * End of the area of reserved blocks, used for the next allocation.
   */
  uint64_t gauge_;
  /**
   * Sum of the non-free block sizes.
   */
  uint64_t stored_;
  /**
   * Number of reserved blocks
   */
  uint64_t num_blocks_;
  /**
   * The big mmap'd memory block used to serve allocation requests.
   */
  unsigned char *heap_;
};  // class MallocHeap

#endif  // CVMFS_MALLOC_HEAP_H_
