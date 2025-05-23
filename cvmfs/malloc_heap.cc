/**
 * This file is part of the CernVM File System.
 */


#include "malloc_heap.h"

#include <cassert>
#include <cstring>
#include <new>

#include "util/smalloc.h"

using namespace std;  // NOLINT

void *MallocHeap::Allocate(uint64_t size, void *header, unsigned header_size) {
  assert(size > 0);
  assert(header_size <= size);
  uint64_t rounded_size = RoundUp8(size);
  int64_t real_size = rounded_size + sizeof(Tag);
  if (gauge_ + real_size > capacity_)
    return NULL;

  unsigned char *new_block = heap_ + gauge_;
  new (new_block) Tag(rounded_size);
  new_block += sizeof(Tag);
  memcpy(new_block, header, header_size);
  gauge_ += real_size;
  stored_ += rounded_size;
  num_blocks_++;
  return new_block;
}


void MallocHeap::Compact() {
  if (gauge_ == 0)
    return;

  // Not really a tag, just the top memory address
  Tag *heap_top = reinterpret_cast<Tag *>(heap_ + gauge_);
  Tag *current_tag = reinterpret_cast<Tag *>(heap_);
  Tag *next_tag = current_tag->JumpToNext();
  // Move a sliding window of two blocks over the heap and compact where
  // possible
  while (next_tag < heap_top) {
    if (current_tag->IsFree()) {
      if (next_tag->IsFree()) {
        // Adjacent free blocks, merge and try again
        current_tag->size -= sizeof(Tag) + next_tag->GetSize();
        next_tag = next_tag->JumpToNext();
      } else {
        // Free block followed by a reserved block, move memory and create a
        // new free tag at the end of the moved block
        int64_t free_space = current_tag->size;
        current_tag->size = next_tag->size;
        memmove(current_tag->GetBlock(), next_tag->GetBlock(),
                next_tag->GetSize());
        (*callback_ptr_)(BlockPtr(current_tag->GetBlock()));
        next_tag = current_tag->JumpToNext();
        next_tag->size = free_space;
      }
    } else {
      // Current block allocated, move on
      current_tag = next_tag;
      next_tag = next_tag->JumpToNext();
    }
  }

  gauge_ = (reinterpret_cast<unsigned char *>(current_tag) - heap_);
  if (!current_tag->IsFree())
    gauge_ += sizeof(Tag) + current_tag->GetSize();
}


void *MallocHeap::Expand(void *block, uint64_t new_size) {
  uint64_t old_size = GetSize(block);
  assert(old_size <= new_size);
  void *new_block = Allocate(new_size, block, old_size);
  if (new_block != NULL)
    MarkFree(block);
  return new_block;
}


bool MallocHeap::HasSpaceFor(uint64_t nbytes) {
  return RoundUp8(gauge_ + nbytes + sizeof(Tag)) <= capacity_;
}


void MallocHeap::MarkFree(void *block) {
  Tag *tag = reinterpret_cast<Tag *>(block) - 1;
  assert(tag->size > 0);
  tag->size = -(tag->size);
  stored_ -= tag->GetSize();
  num_blocks_--;
  // TODO(jblomer): if MarkFree() takes place at the top of the heap, one could
  // move back the gauge_ pointer.  If this is an optimization or unnecessary
  // extra work depends on how the MallocHeap is used.
}


uint64_t MallocHeap::GetSize(void *block) {
  Tag *tag = reinterpret_cast<Tag *>(block) - 1;
  assert(tag->size > 0);
  return tag->size;
}


MallocHeap::MallocHeap(uint64_t capacity, CallbackPtr callback_ptr)
    : callback_ptr_(callback_ptr)
    , capacity_(capacity)
    , gauge_(0)
    , stored_(0)
    , num_blocks_(0) {
  assert(capacity_ > kMinCapacity);
  // Ensure 8-byte alignment
  assert((capacity_ % 8) == 0);
  heap_ = reinterpret_cast<unsigned char *>(sxmmap(capacity));
  assert(uintptr_t(heap_) % 8 == 0);
}


MallocHeap::~MallocHeap() { sxunmap(heap_, capacity_); }
