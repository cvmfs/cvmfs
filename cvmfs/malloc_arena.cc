/**
 * This file is part of the CernVM File System.
 */

#define __STDC_FORMAT_MACROS


#include "malloc_arena.h"

#include <cassert>
#include <cstddef>
#include <cstring>
#include <new>

#include "util/smalloc.h"

using namespace std;  // NOLINT


/**
 * Walks through the free list starting at rover_ and looks for the first block
 * larger than block_size.  Returns NULL if no such block exists.
 */
MallocArena::AvailBlockCtl *MallocArena::FindAvailBlock(
    const int32_t block_size) {
  bool wrapped = false;
  // Generally: p = LINK(q)
  AvailBlockCtl *q = rover_;
  AvailBlockCtl *p;
  do {
    p = q->GetNextPtr(arena_);
    if (p->size >= block_size) {
      rover_ = p->GetNextPtr(arena_);
      return p;
    }
    if (p == head_avail_) {
      if (wrapped)
        return NULL;
      wrapped = true;
    }
    q = p;
  } while (true);
}


/**
 * Creates a free block at the place of the reserved block ptr points into.
 * The free block might need to be merged with adjacent lower and/or upper
 * blocks.  In these cases, the corresponding blocks are removed from the list
 * of available blocks.  Every allocated block has a predecessor and a
 * successor in the arena.  The newly created free block is added to the end of
 * the list of available blocks.
 */
void MallocArena::Free(void *ptr) {
  assert(Contains(ptr));

  no_reserved_--;

  ReservedBlockCtl *block_ctl = reinterpret_cast<ReservedBlockCtl *>(
      reinterpret_cast<char *>(ptr) - sizeof(ReservedBlockCtl));
  const char prior_tag = *(reinterpret_cast<char *>(block_ctl) - 1);
  assert((prior_tag == kTagAvail) || (prior_tag == kTagReserved));

  int32_t new_size = block_ctl->size();
  assert(new_size > 0);
  AvailBlockCtl *new_avail = reinterpret_cast<AvailBlockCtl *>(block_ctl);

  if (prior_tag == kTagAvail) {
    // Merge with block before and remove the block from the list
    const int32_t prior_size = reinterpret_cast<AvailBlockTag *>(
                                   reinterpret_cast<char *>(block_ctl)
                                   - sizeof(AvailBlockTag))
                                   ->size;
    assert(prior_size > 0);
    new_size += prior_size;
    new_avail = reinterpret_cast<AvailBlockCtl *>(
        reinterpret_cast<char *>(block_ctl) - prior_size);
    // new_avail points now to the prior block
    UnlinkAvailBlock(new_avail);
    if (rover_ == new_avail)
      rover_ = head_avail_;
  }

  const int32_t succ_size = *reinterpret_cast<int32_t *>(
      reinterpret_cast<char *>(new_avail) + new_size);
  if (succ_size >= 0) {
    // Merge with succeeding block and remove the block from the list
    AvailBlockCtl *succ_avail = reinterpret_cast<AvailBlockCtl *>(
        reinterpret_cast<char *>(new_avail) + new_size);
    UnlinkAvailBlock(succ_avail);
    new_size += succ_size;
    if (rover_ == succ_avail)
      rover_ = head_avail_;
  }

  // Set new free block's boundaries
  new_avail->size = new_size;
  new (AvailBlockTag::GetTagLocation(new_avail)) AvailBlockTag(new_size);

  EnqueueAvailBlock(new_avail);
}


/**
 * Inserts an available block at the end of the free list.
 */
void MallocArena::EnqueueAvailBlock(AvailBlockCtl *block) {
  AvailBlockCtl *next = head_avail_;
  AvailBlockCtl *prev = head_avail_->GetPrevPtr(arena_);
  next->link_prev = block->ConvertToLink(arena_);
  prev->link_next = block->ConvertToLink(arena_);
  block->link_next = head_avail_->ConvertToLink(arena_);
  block->link_prev = prev->ConvertToLink(arena_);
}


/**
 * The ptr points to the result of Malloc().  The size of the area is stored
 * a few bytes before ptr.
 */
uint32_t MallocArena::GetSize(void *ptr) const {
  assert(Contains(ptr));

  ReservedBlockCtl *block_ctl = reinterpret_cast<ReservedBlockCtl *>(
      reinterpret_cast<char *>(ptr) - sizeof(ReservedBlockCtl));
  const int32_t size = block_ctl->size();
  assert(size > 1);
  return size - sizeof(ReservedBlockCtl) - 1;
}


/**
 * Walks the list of available blocks starting from rover and allocates the
 * first available spot that's large enough.  Puts the reserved block at the end
 * of the available one and, if necessary, removes the available one from the
 * list of free blocks.
 */
void *MallocArena::Malloc(const uint32_t size) {
  assert(size > 0);

  // Control word first, block type tag last
  int32_t total_size = sizeof(ReservedBlockCtl) + size + 1;
  total_size = RoundUp8(total_size);
  if (total_size < kMinBlockSize)
    total_size = kMinBlockSize;

  AvailBlockCtl *p = FindAvailBlock(total_size);
  if (p == NULL)
    return NULL;

  no_reserved_++;
  return ReserveBlock(p, total_size);
}


/**
 * The arena starts with a pointer to this followed by the AvailBlockCtl of
 * head_avail_, followed by a reserved tag to prevent it from being merged,
 * followed by a free block spanning the arena until the end tag.  The end tag
 * is a single negative int, which mimics another reserved block.
 */
MallocArena::MallocArena(unsigned arena_size)
    : arena_(reinterpret_cast<char *>(sxmmap_align(arena_size)))
    , head_avail_(reinterpret_cast<AvailBlockCtl *>(arena_ + sizeof(uint64_t)))
    , rover_(head_avail_)
    , no_reserved_(0)
    , arena_size_(arena_size) {
  assert(arena_size_ > 0);
  assert((arena_size_ % (2 * 1024 * 1024)) == 0);  // Multiple of 2MB
  assert(arena_size_ <= (512 * 1024 * 1024));      // <= 512MB

  const unsigned char padding = 7;
  // Size of the initial free block: everything minus arena boundaries
  const int32_t usable_size = arena_size_
                              - (sizeof(uint64_t) + sizeof(AvailBlockCtl)
                                 + padding + 1 + sizeof(int32_t));
  assert((usable_size % 8) == 0);

  // First 8 bytes of arena: this pointer (occupies only 4 bytes on 32bit
  // architectures, in which case the second 4 bytes are unused.)
  *reinterpret_cast<MallocArena **>(arena_) = this;

  // The initial large free block
  AvailBlockCtl *free_block = new (arena_ + sizeof(uint64_t)
                                   + sizeof(AvailBlockCtl) + padding + 1)
      AvailBlockCtl();
  free_block->size = usable_size;
  free_block->link_next = free_block->link_prev = head_avail_->ConvertToLink(
      arena_);
  new (AvailBlockTag::GetTagLocation(free_block)) AvailBlockTag(usable_size);

  head_avail_->size = 0;
  head_avail_->link_next = head_avail_->link_prev = free_block->ConvertToLink(
      arena_);

  // Prevent succeeding blocks from merging
  *(reinterpret_cast<char *>(free_block) - 1) = kTagReserved;
  // Final tag: reserved block marker
  *reinterpret_cast<int32_t *>(arena_ + arena_size_ - sizeof(int32_t)) = -1;
}


/**
 * Initializes the arena with repeated copies of the given pattern.  Used for
 * testing.
 */
MallocArena *MallocArena::CreateInitialized(unsigned arena_size,
                                            unsigned char pattern) {
  MallocArena *result = new MallocArena(arena_size);
  // At this point, there is one big free block linked to by head_avail_
  AvailBlockCtl *free_block = result->head_avail_->GetNextPtr(result->arena_);
  assert(free_block != result->head_avail_);
  assert(free_block->size > 0);
  // Strip control information at both ends of the block
  const int usable_size = free_block->size
                          - (sizeof(AvailBlockCtl) + sizeof(AvailBlockTag));
  assert(usable_size > 0);
  memset(free_block + 1, pattern, usable_size);
  return result;
}


MallocArena::~MallocArena() { sxunmap(arena_, arena_size_); }


/**
 * Given the free block "block", cuts out a new reserved block of size
 * block_size at the end of the free block.  Returns a pointer usable by the
 * application.
 */
void *MallocArena::ReserveBlock(AvailBlockCtl *block, int32_t block_size) {
  assert(block->size >= block_size);

  int32_t remaining_size = block->size - block_size;
  // Avoid creation of very small blocks
  if (remaining_size < kMinBlockSize) {
    block_size += remaining_size;
    remaining_size = 0;
  }

  // Update the list of available blocks
  if (remaining_size == 0) {
    // Remove free block p from the list of available blocks
    UnlinkAvailBlock(block);
  } else {
    block->ShrinkTo(remaining_size);
  }

  // Place the new allocation, which also sets the block type tag at the end
  char *new_block = reinterpret_cast<char *>(block) + remaining_size;
  new (new_block) ReservedBlockCtl(block_size);
  return new_block + sizeof(ReservedBlockCtl);
}


/**
 * Removes the given block from the doubly linked free block list.  This happens
 * when two adjacent free blocks are created in Free() and then merged.  Or if
 * a block gets fully used in Malloc().
 */
void MallocArena::UnlinkAvailBlock(AvailBlockCtl *block) {
  AvailBlockCtl *next = block->GetNextPtr(arena_);
  AvailBlockCtl *prev = block->GetPrevPtr(arena_);
  prev->link_next = block->link_next;
  next->link_prev = block->link_prev;
}
