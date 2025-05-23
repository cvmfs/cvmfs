/**
 * This file is part of the CernVM File System.
 *
 * Similar to bigvector, but queue semantics. Used by the negative entry
 * tracker. Allocates with mmap in order to avoid memory fragmentation.
 */

#ifndef CVMFS_BIGQUEUE_H_
#define CVMFS_BIGQUEUE_H_

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <new>

#include "util/smalloc.h"

template<class Item>
class BigQueue {
 public:
  BigQueue() {
    Alloc(kNumInit);
    size_ = 0;
  }

  explicit BigQueue(const size_t num_items) {
    size_t min_items = kNumInit;
    Alloc(std::max(num_items, min_items));
    size_ = 0;
  }

  BigQueue(const BigQueue<Item> &other) { CopyFrom(other); }

  BigQueue<Item> &operator=(const BigQueue<Item> &other) {
    if (&other == this)
      return *this;

    Dealloc();
    CopyFrom(other);
    return *this;
  }

  ~BigQueue() { Dealloc(); }

  void PushBack(const Item &item) {
    if (GetAvailableSpace() == 0) {
      Migrate(1.9 * static_cast<float>(capacity_));
      assert(GetAvailableSpace() > 0);
    }
    new (head_ + size_) Item(item);
    size_++;
  }

  void PopFront() {
    assert(!IsEmpty());
    head_++;
    size_--;
    if ((size_ > kCompactThreshold) && (size_ < (capacity_ / 2)))
      Migrate(static_cast<int>(static_cast<float>(capacity_ * 0.6)));
  }

  bool Peek(Item **item) {
    if (IsEmpty())
      return false;
    *item = head_;
    return true;
  }

  bool IsEmpty() const { return size_ == 0; }

  void Clear() {
    Dealloc();
    Alloc(kNumInit);
  }

  size_t size() const { return size_; }
  size_t capacity() const { return capacity_; }

 private:
  static const size_t kNumInit = 64;
  static const size_t kCompactThreshold = 64;

  size_t GetHeadOffset() const { return head_ - buffer_; }
  size_t GetAvailableSpace() const {
    return capacity_ - (size_ + GetHeadOffset());
  }

  void Alloc(const size_t num_elements) {
    size_t num_bytes = sizeof(Item) * num_elements;
    buffer_ = static_cast<Item *>(smmap(num_bytes));
    capacity_ = num_elements;
    head_ = buffer_;
  }

  void Dealloc() {
    FreeBuffer(buffer_, GetHeadOffset() + size_);
    buffer_ = NULL;
    head_ = NULL;
    capacity_ = 0;
    size_ = 0;
  }

  void Migrate(size_t new_capacity) {
    assert(new_capacity > 0);
    assert(new_capacity >= size_);

    size_t head_offset = GetHeadOffset();
    Item *old_buffer = buffer_;

    Alloc(new_capacity);
    for (size_t i = 0; i < size_; ++i)
      new (buffer_ + i) Item(old_buffer[head_offset + i]);

    FreeBuffer(old_buffer, head_offset + size_);
  }

  void FreeBuffer(Item *buf, const size_t nitems) {
    for (size_t i = 0; i < nitems; ++i)
      buf[i].~Item();

    if (buf)
      smunmap(buf);
  }

  void CopyFrom(const BigQueue<Item> &other) {
    size_t min_items = kNumInit;
    Alloc(std::max(other.size_, min_items));
    for (size_t i = 0; i < other.size_; ++i) {
      new (buffer_ + i) Item(*(other.buffer_ + other.GetHeadOffset() + i));
    }
    size_ = other.size_;
  }

  Item *buffer_;
  Item *head_;
  size_t size_;
  size_t capacity_;
};

#endif  // CVMFS_BIGQUEUE_H_
