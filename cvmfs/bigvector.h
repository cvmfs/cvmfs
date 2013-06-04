/**
 * This file is part of the CernVM File System.
 *
 * Dynamic array, allocate with mmap for large arrays.
 */

#ifndef CVMFS_BIGVECTOR_H_
#define CVMFS_BIGVECTOR_H_

#include <cstdlib>
#include <cassert>

#include "smalloc.h"

template<class Item>
class BigVector {
 public:
  BigVector() {
    Alloc(kNumInit);
    size_ = 0;
    shared_buffer_ = false;
  }

  ~BigVector() {
    if (!shared_buffer_)
      Dealloc();
  }

  Item At(const size_t index) const {
    assert(index < size_);
    return buffer_[index];
  }

  void PushBack(const Item &item) {
    if (size_ == capacity_) {
      Item *old_buffer = buffer_;
      bool old_large_alloc = large_alloc_;

      assert(capacity_ > 0);
      Alloc(capacity_ * 2);
      for (size_t i = 0; i < size_; ++i)
        buffer_[i] = old_buffer[i];

      FreeBuffer(old_buffer, old_large_alloc);
    }
    buffer_[size_] = item;
    size_++;
  }

  void Clear() {
    Dealloc();
    Alloc(kNumInit);
  }

  void ShareBuffer(Item **duplicate, bool *large_alloc) {
    *duplicate = buffer_;
    *large_alloc = large_alloc_;
    shared_buffer_ = true;
  }

  size_t size() const { return size_; }
  size_t capacity() const { return capacity_; }
 private:
  static const size_t kNumInit = 4;
  static const size_t kMmapThreshold = 128*1024;

  void Alloc(const size_t num_elements) {
    size_t num_bytes = sizeof(Item) * num_elements;
    if (num_bytes >= kMmapThreshold) {
      buffer_ = static_cast<Item *>(smmap(num_bytes));
      large_alloc_ = true;
    } else {
      buffer_ = static_cast<Item *>(smalloc(num_bytes));
      large_alloc_ = false;
    }
    capacity_ = num_elements;
  }

  void Dealloc() {
    for (size_t i = 0; i < size_; ++i)
      buffer_[i].~Item();

    FreeBuffer(buffer_, large_alloc_);
    buffer_ = NULL;
    capacity_ = 0;
    size_ = 0;
  }

  void FreeBuffer(Item *buf, const bool large) {
    if (buf) {
      if (large)
        smunmap(buf);
      else
        free(buf);
    }
  }

  Item *buffer_;
  size_t size_;
  size_t capacity_;
  bool large_alloc_;
  bool shared_buffer_;
};

#endif  // CVMFS_BIGVECTOR_H_
