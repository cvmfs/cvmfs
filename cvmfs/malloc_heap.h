/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_MALLOC_HEAP_H_
#define CVMFS_MALLOC_HEAP_H_

#include <inttypes.h>
#include <stdint.h>

#include <cassert>

/**
 *
 */
class MallocHeap {
 public:
  explicit MallocHeap(uint64_t capacity);
  ~MallocHeap();

  void *Allocate(uint64_t size, unsigned char *header, unsigned header_size);
  void Free(void *block);
  uint64_t GetSize(void *block);
  uint64_t GetSizeInRam(void *block) { return RoundUp8(GetSize(block)); }
  void Compact();

 private:
  struct Tag {
    /**
     * Positive for reserved blocks, negative for free blocks
     */
    int64_t size;
  };

  /**
   * Round up size to the next larger multiple of 8.  This is used to enforce
   * 8-byte alignment.
   */
  static inline uint32_t RoundUp8(const uint32_t size) {
    return (size + 7) & ~7;
  }

  /**
   * Total size of mmap'd arena.
   */
  uint64_t capacity_;
  /**
   * End of the area of reserved blocks, used for the next allocation.
   */
  uint64_t gauge_;
};  // class MallocHeap

#endif  // CVMFS_MALLOC_HEAP_H_
