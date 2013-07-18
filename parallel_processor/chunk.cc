#include "chunk.h"

#include <sstream>
#include <cstdio>

#include "io_dispatcher.h"
#include "file.h"

void Chunk::ScheduleWrite(CharBuffer *buffer) {
  assert (buffer->used_bytes() > 0);

  if (deferred_write_) {
    assert (! HasFileDescriptor());
    deferred_buffers_.push_back(buffer);
    return;
  }

  IoDispatcher::Instance()->ScheduleWrite(this, buffer);
}


void Chunk::FlushDeferredWrites(const bool delete_buffers) {
  assert (deferred_write_);

  std::vector<CharBuffer*>::const_iterator i    = deferred_buffers_.begin();
  std::vector<CharBuffer*>::const_iterator iend = deferred_buffers_.end();
  for (; i != iend; ++i) {
    IoDispatcher::Instance()->ScheduleWrite(this, *i, delete_buffers);
  }
  deferred_buffers_.clear();
  deferred_write_ = false;
}


void Chunk::Done() {
  assert (! done_);

  if (deferred_write_) {
    FlushDeferredWrites();
  }

  assert (! deferred_write_);
  done_ = true;
}


Chunk::Chunk(const Chunk &other) :
  owning_file_(other.owning_file_),
  file_offset_(other.file_offset_),
  chunk_size_(other.chunk_size_),
  done_(other.done_),
  is_bulk_chunk_(other.is_bulk_chunk_),
  deferred_write_(other.deferred_write_),
  deferred_buffers_(other.deferred_buffers_),
  zlib_initialized_(false),
  sha1_context_(other.sha1_context_),
  sha1_digest_(""),
  sha1_initialized_(other.sha1_initialized_),
  file_descriptor_(0),
  tmp_file_path_(other.tmp_file_path_),
  bytes_written_(other.bytes_written_),
  compressed_size_(other.compressed_size_)
{
  assert (other.done_ == false);
  assert (! other.HasFileDescriptor());
  assert (other.tmp_file_path_.empty());
  assert (other.bytes_written_ == 0);
  assert (other.zlib_context_.avail_in == 0);

  const int retval = deflateCopy(&zlib_context_,
                                 const_cast<z_streamp>(&other.zlib_context_));
  assert (retval == Z_OK);
  zlib_initialized_ = true;
}


Chunk* Chunk::CopyAsBulkChunk(const size_t file_size) {
  assert (! done_);
  assert (deferred_write_);
  assert (! HasFileDescriptor());
  assert (file_offset_ == 0);

  Chunk *new_bulk_chunk = new Chunk(*this);
  new_bulk_chunk->set_size(file_size);
  const bool should_delete_buffers = false;
  new_bulk_chunk->FlushDeferredWrites(should_delete_buffers);
  new_bulk_chunk->SetAsBulkChunk();

  // upload all previously generated buffers _without_ deleting them, they will
  // be needed for the copied bulk chunk as well
  FlushDeferredWrites();
  assert (! deferred_write_);

  return new_bulk_chunk;
}
