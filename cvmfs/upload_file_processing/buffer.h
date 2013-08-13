/**
 * This file is part of the CernVM File System.
 */

#ifndef ULOAD_FILE_PROCESSING_BUFFER_H
#define ULOAD_FILE_PROCESSING_BUFFER_H

#include <tbb/atomic.h>
#include <tbb/scalable_allocator.h>
#include <tbb/tick_count.h>

#include <cassert>
#include <vector>
#include <sys/types.h>


namespace upload {

static const uint64_t kTimeResolution = 1000000000;

template<typename T, class A = std::allocator<T> >
class Buffer {
 public:
  Buffer() : base_offset_(0), size_(0), used_bytes_(0), buffer_(NULL) {}

  Buffer(const size_t size) : base_offset_(0),
                              size_(0),
                              used_bytes_(0),
                              buffer_(NULL) {
    Allocate(size);
  }

  ~Buffer() {
    Deallocate();
  }

  void Allocate(const size_t size) {
    assert (!IsInitialized());
    size_ = size;
    buffer_ = A().allocate(size_);
  }

  bool IsInitialized() const { return size_ > 0; }

  typename A::pointer ptr() {
    assert (IsInitialized());
    return buffer_;
  }
  const typename A::pointer ptr() const {
    assert (IsInitialized());
    return buffer_;
  }

  void SetUsedBytes(const size_t bytes)  { used_bytes_ = bytes; }
  void SetBaseOffset(const off_t offset) { base_offset_ = offset; }

  size_t size()        const { return size_;        }
  size_t used_bytes()  const { return used_bytes_;  }
  off_t  base_offset() const { return base_offset_; }

 private:
  Buffer(const Buffer &other) { assert (false); } // no copy!
  Buffer& operator=(const Buffer& other) { assert (false); }

  void Deallocate() {
    if (size_ == 0) {
      return;
    }
    A().deallocate(buffer_, size_);
    buffer_     = NULL;
    size_       = 0;
    used_bytes_ = 0;
  }

 private:
  off_t                        base_offset_;
  size_t                       used_bytes_;
  size_t                       size_;
  typename A::pointer          buffer_;
};
typedef Buffer<unsigned char, tbb::scalable_allocator<unsigned char> > CharBuffer;

typedef std::vector<CharBuffer*> CharBufferVector;

} // namespace upload

#endif /* ULOAD_FILE_PROCESSING_BUFFER_H */
