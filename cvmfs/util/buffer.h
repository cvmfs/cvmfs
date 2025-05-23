/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_BUFFER_H_
#define CVMFS_UTIL_BUFFER_H_

#include <cassert>
#include <cstddef>
#include <memory>


#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif


template<typename T, class A = std::allocator<T> >
class Buffer {
 public:
  typedef typename A::pointer pointer_t;

  Buffer() : used_(0), size_(0), buffer_(NULL), initialized_(false) { }

  explicit Buffer(const size_t size)
      : used_(0), size_(0), buffer_(NULL), initialized_(false) {
    Allocate(size);
  }

  virtual ~Buffer() { Deallocate(); }

  void Allocate(const size_t size) {
    assert(!IsInitialized());
    size_ = size;
    buffer_ = allocator_.allocate(size_bytes());
    initialized_ = true;
  }

  bool IsInitialized() const { return initialized_; }

  typename A::pointer ptr() {
    assert(IsInitialized());
    return buffer_;
  }
  const typename A::pointer ptr() const {
    assert(IsInitialized());
    return buffer_;
  }

  typename A::pointer free_space_ptr() {
    assert(IsInitialized());
    return buffer_ + used();
  }

  const typename A::pointer free_space_ptr() const {
    assert(IsInitialized());
    return buffer_ + used();
  }

  void SetUsed(const size_t items) {
    assert(items <= size());
    used_ = items;
  }

  void SetUsedBytes(const size_t bytes) {
    assert(bytes <= size_bytes());
    assert(bytes % sizeof(T) == 0);
    used_ = bytes / sizeof(T);
  }

  size_t size() const { return size_; }
  size_t size_bytes() const { return size_ * sizeof(T); }
  size_t used() const { return used_; }
  size_t used_bytes() const { return used_ * sizeof(T); }
  size_t free() const { return size_ - used_; }
  size_t free_bytes() const { return free() * sizeof(T); }

 private:
  Buffer(const Buffer &other) { assert(false); }  // no copy!
  Buffer &operator=(const Buffer &other) { assert(false); }

  void Deallocate() {
    if (size_ == 0) {
      return;
    }
    allocator_.deallocate(buffer_, size_bytes());
    buffer_ = NULL;
    size_ = 0;
    used_ = 0;
    initialized_ = false;
  }

 private:
  A allocator_;
  size_t used_;
  size_t size_;
  typename A::pointer buffer_;
  bool initialized_;
};

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_BUFFER_H_
