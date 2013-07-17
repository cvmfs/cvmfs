#include "chunk.h"

#include <sstream>

#include "io_dispatcher.h"

void Chunk::ScheduleWrite(CharBuffer *buffer) {
  if (deferred_write_) {
    assert (! HasFileDescriptor());
    deferred_buffers_.push_back(buffer);
    return;
  }

  IoDispatcher::Instance()->ScheduleWrite(this, buffer);
}


void Chunk::Done() {
  assert (! done_);

  if (deferred_write_) {
    assert (deferred_buffers_.size() > 0);
    std::vector<CharBuffer*>::const_iterator i    = deferred_buffers_.begin();
    std::vector<CharBuffer*>::const_iterator iend = deferred_buffers_.end();
    for (; i != iend; ++i) {
      IoDispatcher::Instance()->ScheduleWrite(this, *i);
    }
    deferred_write_ = false;
  }

  done_ = true;
}
