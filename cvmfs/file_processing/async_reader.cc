/**
 * This file is part of the CernVM File System.
 */

#include "async_reader.h"

using namespace upload;


CharBuffer* AbstractReader::CreateBuffer(const size_t size) {
  ++buffers_in_flight_counter_;
  CharBuffer *buffer = new CharBuffer(size);
  return buffer;
}


void AbstractReader::ReleaseBuffer(CharBuffer *buffer) {
  delete buffer;
  --buffers_in_flight_counter_;
}

