/**
 * This file is part of the CernVM File System.
 *
 * Dynamic array, allocate with mmap for large arrays.
 */

#ifndef CVMFS_BIGVECTOR_H_
#define CVMFS_BIGVECTOR_H_

#include <cassert>
#include <cstdlib>

#include "util/smalloc.h"

template<class Item>
class BigVector {
 public:
  BigVector() {
    buffer_ = Alloc(kNumInit);
    size_ = 0;
    shared_buffer_ = false;
  }

  explicit BigVector(const size_t num_items) {
    assert(num_items > 0);
    buffer_ = Alloc(num_items);
    size_ = 0;
    shared_buffer_ = false;
  }

  BigVector(const BigVector<Item> &other) { CopyFrom(other); }

  BigVector<Item> &operator=(const BigVector<Item> &other) {
    if (&other == this)
      return *this;

    if (!shared_buffer_)
      Dealloc();
    CopyFrom(other);
    return *this;
  }

  ~BigVector() {
    if (!shared_buffer_)
      Dealloc();
  }

  Item At(const size_t index) const {
    assert(index < size_);
    return buffer_[index];
  }

  const Item *AtPtr(const size_t index) const {
    assert(index < size_);
    return &buffer_[index];
  }

  void PushBack(const Item &item) {
    if (size_ == capacity_)
      DoubleCapacity();
    new (buffer_ + size_) Item(item);
    size_++;
  }

  void Replace(size_t index, const Item &item) {
    assert(index < size_);
    buffer_[index] = item;
  }

  bool IsEmpty() const { return size_ == 0; }

  void Clear() {
    Dealloc();
    buffer_ = Alloc(kNumInit);
  }

  void ShareBuffer(Item **duplicate, bool *large_alloc) {
    *duplicate = buffer_;
    *large_alloc = large_alloc_;
    shared_buffer_ = true;
  }

  void DoubleCapacity() {
    Item *old_buffer = buffer_;
    bool old_large_alloc = large_alloc_;

    assert(capacity_ > 0);
    buffer_ = Alloc(capacity_ * 2);
    for (size_t i = 0; i < size_; ++i)
      new (buffer_ + i) Item(old_buffer[i]);

    FreeBuffer(old_buffer, size_, old_large_alloc);
  }

  void ShrinkIfOversized() {
    assert(!shared_buffer_);

    if (size_ <= kNumInit)
      return;
    if (static_cast<float>(size_) >= (0.25 * static_cast<float>(capacity_)))
      return;

    bool old_large_alloc = large_alloc_;
    Item *new_buffer = Alloc(0.5 * static_cast<float>(capacity_));
    for (size_t i = 0; i < size_; ++i)
      new (new_buffer + i) Item(buffer_[i]);
    FreeBuffer(buffer_, size_, old_large_alloc);
    buffer_ = new_buffer;
  }

  // Careful!  Only for externally modified buffer.
  void SetSize(const size_t new_size) {
    assert(new_size <= capacity_);
    size_ = new_size;
  }

  size_t size() const { return size_; }
  size_t capacity() const { return capacity_; }

 private:
  static const size_t kNumInit = 16;
  static const size_t kMmapThreshold = 128 * 1024;

  Item *Alloc(const size_t num_elements) {
    Item *result;
    size_t num_bytes = sizeof(Item) * num_elements;
    if (num_bytes >= kMmapThreshold) {
      result = static_cast<Item *>(smmap(num_bytes));
      large_alloc_ = true;
    } else {
      result = static_cast<Item *>(smalloc(num_bytes));
      large_alloc_ = false;
    }
    capacity_ = num_elements;
    return result;
  }

  void Dealloc() {
    FreeBuffer(buffer_, size_, large_alloc_);
    buffer_ = NULL;
    capacity_ = 0;
    size_ = 0;
  }

  void FreeBuffer(Item *buf, const size_t size, const bool large) {
    for (size_t i = 0; i < size; ++i)
      buf[i].~Item();

    if (buf) {
      if (large) {
        smunmap(buf);
      } else {
        free(buf);
      }
    }
  }

  void CopyFrom(const BigVector<Item> &other) {
    buffer_ = Alloc(other.capacity_);
    for (size_t i = 0; i < other.size_; ++i) {
      new (buffer_ + i) Item(*other.AtPtr(i));
    }
    size_ = other.size_;
    shared_buffer_ = false;
  }

  Item *buffer_;
  size_t size_;
  size_t capacity_;
  bool large_alloc_;
  bool shared_buffer_;
};

#endif  // CVMFS_BIGVECTOR_H_
