/**
 * This file is part of the CernVM File System.
 */

#include "chunk.h"

#include <sstream>
#include <cstdio>

#include "io_dispatcher.h"
#include "file.h"

using namespace upload;

void Chunk::ScheduleWrite(CharBuffer *buffer) {
  assert (buffer->used_bytes() > 0);

  if (deferred_write_) {
    assert (! HasUploadStreamHandle());
    deferred_buffers_.push_back(buffer);
  } else {
    file_->io_dispatcher()->ScheduleWrite(this, buffer);
  }
}


void Chunk::FlushDeferredWrites(const bool delete_buffers) {
  assert (deferred_write_);

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

  const int sha1_retval = SHA1_Init(&sha1_context_);
  assert (sha1_retval == 1);

  zlib_context_.zalloc   = Z_NULL;
  zlib_context_.zfree    = Z_NULL;
  zlib_context_.opaque   = Z_NULL;
  zlib_context_.next_in  = Z_NULL;
  zlib_context_.avail_in = 0;
  const int zlib_retval = deflateInit(&zlib_context_, Z_DEFAULT_COMPRESSION);
  assert (zlib_retval == 0);

  zlib_initialized_ = true;
  sha1_initialized_ = true;
}


void Chunk::Finalize() {
  assert (! done_);

  const int fin_rc = SHA1_Final(sha1_digest_, &sha1_context_);
  assert (fin_rc == 1);

  assert (zlib_context_.avail_in == 0);
  const int retcode = deflateEnd(&zlib_context_);
  assert (retcode == Z_OK);

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
  sha1_context_(other.sha1_context_),
  sha1_initialized_(other.sha1_initialized_),
  upload_stream_handle_(NULL),
  tmp_file_path_(other.tmp_file_path_),
  bytes_written_(other.bytes_written_),
  compressed_size_(other.compressed_size_)
{
  assert (! other.done_);
  assert (! other.HasUploadStreamHandle());
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
  assert (! HasUploadStreamHandle());
  assert (file_offset_ == 0);

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
  assert (! deferred_write_);

  return new_bulk_chunk;
}
