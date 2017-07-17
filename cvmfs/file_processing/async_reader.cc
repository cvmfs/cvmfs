/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "async_reader.h"

namespace upload {

CharBuffer* AbstractReader::CreateBuffer(const size_t size) {
  CharBuffer *buffer = new CharBuffer(size);
  return buffer;
}


void AbstractReader::ReleaseBuffer(CharBuffer *buffer) {
  delete buffer;
}

}  // namespace upload
