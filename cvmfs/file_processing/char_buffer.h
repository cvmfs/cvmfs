/**
 * This file is part of the CernVM File System.
 */

#ifndef UPLOAD_FILE_PROCESSING_BUFFER_H
#define UPLOAD_FILE_PROCESSING_BUFFER_H

#include <tbb/scalable_allocator.h>

#include <cassert>
#include <vector>
#include <sys/types.h>

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
  CharBuffer(const size_t size) :
    Buffer<unsigned char, tbb::scalable_allocator<unsigned char> >(size),
    base_offset_(0) {}

  void SetBaseOffset(const off_t offset) { base_offset_ = offset; }
  off_t base_offset() const { return base_offset_; }

 private:
  off_t base_offset_;
};

typedef std::vector<CharBuffer*> CharBufferVector;

} // namespace upload

#endif /* UPLOAD_FILE_PROCESSING_BUFFER_H */
