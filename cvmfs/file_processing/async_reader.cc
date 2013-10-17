/**
 * This file is part of the CernVM File System.
 */

#include "async_reader.h"

using namespace upload;


CharBuffer* AbstractReader::CreateBuffer(const size_t size) {
  pthread_mutex_lock(&free_slot_mutex_);
  while (buffers_in_flight_ > max_buffers_in_flight_) {
    pthread_cond_wait(&free_slot_condition_, &free_slot_mutex_);
  }
  ++buffers_in_flight_;
  pthread_mutex_unlock(&free_slot_mutex_);

  CharBuffer *buffer = new CharBuffer(size);
  return buffer;
}


void AbstractReader::ReleaseBuffer(CharBuffer *buffer) {
  delete buffer;
  pthread_mutex_lock(&free_slot_mutex_);
  if (--buffers_in_flight_ < max_buffers_in_flight_ / 2) {
    pthread_cond_signal(&free_slot_condition_);
  }
  pthread_mutex_unlock(&free_slot_mutex_);
}

