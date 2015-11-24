/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "chunk.h"

#include "../file_chunk.h"
#include "../smalloc.h"
#include "file.h"
#include "io_dispatcher.h"

namespace upload {

CharBuffer* Chunk::GetDeflateBuffer(const size_t bytes) {
  if (current_deflate_buffer_              == NULL ||
      current_deflate_buffer_->free_bytes() <  64) {
    if (current_deflate_buffer_ != NULL) {
      ScheduleWrite(current_deflate_buffer_);
    }
    current_deflate_buffer_ = new CharBuffer(bytes);
  }

  return current_deflate_buffer_;
}


void Chunk::ScheduleWrite(CharBuffer *buffer) {
  buffer->SetBaseOffset(compressed_size_);
  compressed_size_ += buffer->used_bytes();

  if (deferred_write_) {
    assert(!HasUploadStreamHandle());
    deferred_buffers_.push_back(buffer);
  } else {
    file_->io_dispatcher()->ScheduleWrite(this, buffer);
  }
}


void Chunk::FlushDeferredWrites(const bool delete_buffers) {
  assert(deferred_write_);

  std::vector<CharBuffer*>::const_iterator i    = deferred_buffers_.begin();
  std::vector<CharBuffer*>::const_iterator iend = deferred_buffers_.end();
  for (; i != iend; ++i) {
    file_->io_dispatcher()->ScheduleWrite(this, *i, delete_buffers);
  }
  deferred_buffers_.clear();
  deferred_write_ = false;
}


void Chunk::Initialize() {
  done_            = false;
  compressed_size_ = 0;

  content_hash_context_.buffer = smalloc(content_hash_context_.size);
  shash::Init(content_hash_context_);

  // Uses PolymorphicConstruction class from util.h, which
  // has a Construct function to create the appropriate object
  // from a parameter, a zlib::Algorithms in this case
  compressor_ = zlib::Compressor::Construct(compression_algorithm_);

  zlib_initialized_         = true;
  content_hash_initialized_ = true;
}


void Chunk::Finalize() {
  assert(!done_);

  shash::Final(content_hash_context_, &content_hash_);
  free(content_hash_context_.buffer);
  content_hash_context_.buffer = NULL;

  if (current_deflate_buffer_ != NULL) {
    ScheduleWrite(current_deflate_buffer_);
    current_deflate_buffer_ = NULL;
  }

  if (deferred_write_) {
    FlushDeferredWrites();
  }

  done_ = true;
}


void Chunk::ScheduleCommit() {
  file_->io_dispatcher()->ScheduleCommit(this);
}


Chunk::Chunk(const Chunk &other) :
  file_(other.file_),
  file_offset_(other.file_offset_),
  chunk_size_(other.chunk_size_),
  done_(other.done_),
  is_bulk_chunk_(other.is_bulk_chunk_),
  is_fully_defined_(other.is_fully_defined_),
  deferred_write_(other.deferred_write_),
  deferred_buffers_(other.deferred_buffers_),
  zlib_initialized_(false),
  content_hash_context_(other.content_hash_context_),
  content_hash_(other.content_hash_),
  content_hash_initialized_(other.content_hash_initialized_),
  upload_stream_handle_(NULL),
  bytes_written_(other.bytes_written_),
  compressed_size_(other.compressed_size_)
{
  assert(!other.done_);
  assert(!other.HasUploadStreamHandle());
  assert(other.bytes_written_ == 0);

  current_deflate_buffer_ = other.current_deflate_buffer_->Clone();

  content_hash_context_.buffer = smalloc(content_hash_context_.size);
  memcpy(content_hash_context_.buffer,
         other.content_hash_context_.buffer,
         content_hash_context_.size);

  compressor_ = other.compressor_->Clone();
  zlib_initialized_ = true;
}


Chunk* Chunk::CopyAsBulkChunk(const size_t file_size) {
  assert(!done_);
  assert(deferred_write_);
  assert(!HasUploadStreamHandle());
  assert(file_offset_ == 0);

  // create a new bulk chunk and upload the (copied) data buffers that have been
  // hold back (deferred write)
  // Note: The buffer data is _not_ copied when copying the chunk
  //       we therefore do not delete the uploaded data
  Chunk *new_bulk_chunk = new Chunk(*this);
  new_bulk_chunk->set_size(file_size);
  const bool should_delete_buffers = false;
  new_bulk_chunk->FlushDeferredWrites(should_delete_buffers);
  new_bulk_chunk->SetAsBulkChunk();

  // upload the deferred buffers and delete them after the upload is complete
  FlushDeferredWrites();
  assert(!deferred_write_);

  return new_bulk_chunk;
}


void Chunk::SetAsBulkChunk() {
  is_bulk_chunk_       = true;
  content_hash_.suffix = file_->hash_suffix();  // bulk chunks inherit file_'s
                                                // content hash suffix
}

}  // namespace upload
