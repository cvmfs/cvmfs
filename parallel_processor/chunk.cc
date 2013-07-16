#include "chunk.h"

#include "io_dispatcher.h"

void Chunk::ScheduleWrite(CharBuffer *buffer) {
  // if (deferred_write_) {
  //   assert (!HasFileDescriptor());
  //   Print("Deferring write...");
  //   deferred_buffers_.push_back(buffer);
  //   return;
  // }

  io_dispatcher_->ScheduleWrite(this, buffer);
}
