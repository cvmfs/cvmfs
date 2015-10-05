/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FILE_PROCESSING_CHAR_BUFFER_H_
#define CVMFS_FILE_PROCESSING_CHAR_BUFFER_H_

#include <sys/types.h>
#include <tbb/scalable_allocator.h>

#include <cassert>
#include <vector>

#include "../util.h"

namespace upload {

/**
 * Specialization of a Buffer object for an ordinary unsigned char buffer.
 * We are using the scalable_allocator of TBB which brings measurable advantage
 * compared to the std::allocator in our use case!
 * Additionally this buffer allows to store a Base Offset, which describes the
 * offset of a specific buffer in a partly read file.
 */
class CharBuffer : public Buffer<unsigned char,
                                 tbb::scalable_allocator<unsigned char> > {
 public:
  CharBuffer() : base_offset_(0) {}
  explicit CharBuffer(const size_t size) :
    Buffer<unsigned char, tbb::scalable_allocator<unsigned char> >(size),
    base_offset_(0) {}

  CharBuffer* Clone() const {
    assert(IsInitialized());
    CharBuffer* new_buffer = new CharBuffer(size());
    assert(new_buffer->IsInitialized());
    new_buffer->SetUsedBytes(used_bytes());
    new_buffer->SetBaseOffset(base_offset());
    memcpy(new_buffer->ptr(), ptr(), size_bytes());
    return new_buffer;
  }

  void SetBaseOffset(const off_t offset) { base_offset_ = offset; }
  off_t base_offset() const { return base_offset_; }

 private:
  off_t base_offset_;
};

typedef std::vector<CharBuffer*> CharBufferVector;

}  // namespace upload

#endif  // CVMFS_FILE_PROCESSING_CHAR_BUFFER_H_
