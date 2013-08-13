#ifndef BUFFER_H
#define BUFFER_H

#include <tbb/atomic.h>
#include <tbb/scalable_allocator.h>
#include <tbb/tick_count.h>

#include <cassert>
#include <vector>
#include <sys/types.h>

#include <sstream> // TODO: remove
#include "util.h"  // TODO: remove

//#define MEASURE_ALLOCATION_TIME
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
#ifdef MEASURE_ALLOCATION_TIME
    tbb::tick_count start = tbb::tick_count::now();
#endif
    size_ = size;
    buffer_ = A().allocate(size_);
#ifdef MEASURE_ALLOCATION_TIME
    tbb::tick_count end = tbb::tick_count::now();
    allocation_time_ += (uint64_t)((end - start).seconds() * kTimeResolution);
    ++active_instances_;
#endif
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
#ifdef MEASURE_ALLOCATION_TIME
    tbb::tick_count start = tbb::tick_count::now();
#endif
    if (size_ == 0) {
      return;
    }
    A().deallocate(buffer_, size_);
    buffer_     = NULL;
    size_       = 0;
    used_bytes_ = 0;
#ifdef MEASURE_ALLOCATION_TIME
    tbb::tick_count end = tbb::tick_count::now();
    deallocation_time_ += (uint64_t)((end - start).seconds() * kTimeResolution);
    --active_instances_;
#endif
  }

 private:
  off_t                        base_offset_;
  size_t                       used_bytes_;
  size_t                       size_;
  typename A::pointer          buffer_;

 public:
  static tbb::atomic<uint64_t> allocation_time_;
  static tbb::atomic<uint64_t> deallocation_time_;
  static tbb::atomic<uint64_t> active_instances_;
};
typedef Buffer<unsigned char, tbb::scalable_allocator<unsigned char> > CharBuffer;

template<typename T, class A>
tbb::atomic<uint64_t> Buffer<T, A>::allocation_time_;

template<typename T, class A>
tbb::atomic<uint64_t> Buffer<T, A>::deallocation_time_;

template<typename T, class A>
tbb::atomic<uint64_t> Buffer<T, A>::active_instances_;

typedef std::vector<CharBuffer*> CharBufferVector;

#endif /* BUFFER_H */
