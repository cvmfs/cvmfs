/**
 * This file is part of the CernVM File System.
 */

#include "io_dispatcher.h"

#include <cassert>
#include <sstream>
#include <cstdio>
#include <cerrno>

#include "file.h"
#include "processor.h"
#include "chunk.h"
#include "file_processor.h"
#include "../util_concurrency.h"

using namespace upload;


void IoDispatcher::WriteThread() {
  bool running = true;

  while (running) {
    WriteJob write_job;
    write_queue_.pop(write_job);

    switch (write_job.type) {
      case WriteJob::TearDown:
        running = false;
        break;
      case WriteJob::CommitChunk:
        CommitChunk(write_job.chunk);
        break;
      case WriteJob::UploadChunk:
        WriteBufferToChunk(write_job.chunk,
                           write_job.buffer,
                           write_job.delete_buffer);
        break;
      default:
        assert (false);
    }
  }
}


void IoDispatcher::WriteBufferToChunk(Chunk       *chunk,
                                      CharBuffer  *buffer,
                                      const bool   delete_buffer) {
  assert (chunk != NULL);
  assert (buffer != NULL);
  assert (chunk->IsInitialized());
  assert (buffer->IsInitialized());

  const size_t bytes_to_write = buffer->used_bytes();
  assert (bytes_to_write > 0u);
  assert (chunk->bytes_written() == static_cast<size_t>(buffer->base_offset()));

  // Initialize a streamed upload in the AbstractUploader implementation if it
  // has not been done before for this Chunk.
  if (! chunk->HasUploadStreamHandle()) {
    UploadStreamHandle *handle = uploader_->InitStreamedUpload(
      AbstractUploader::MakeClosure(&IoDispatcher::ChunkUploadCompleteCallback,
                                    this,
                                    chunk));
    assert (handle != NULL);
    chunk->set_upload_stream_handle(handle);
  }

  // Upload the provided data Block into the chunk in a streamed fashion
  uploader_->Upload(chunk->upload_stream_handle(), buffer,
    AbstractUploader::MakeClosure(&IoDispatcher::BufferUploadCompleteCallback,
                                  this,
                                  BufferUploadCompleteParam(chunk,
                                                            buffer,
                                                            delete_buffer)));
}


void IoDispatcher::CommitChunk(Chunk* chunk) {
  assert (chunk->IsFullyProcessed());
  assert (chunk->HasUploadStreamHandle());

  // Finalize the streamed upload for the committed Chunk
  uploader_->FinalizeStreamedUpload(chunk->upload_stream_handle(),
                                    chunk->sha1(),
                                    chunk->hash_suffix());
}


void IoDispatcher::BufferUploadCompleteCallback(
                                    const UploaderResults        &results,
                                    BufferUploadCompleteParam     buffer_info) {
  Chunk      *chunk         = buffer_info.chunk;
  CharBuffer *buffer        = buffer_info.buffer;
  const bool  delete_buffer = buffer_info.delete_buffer;

  assert (results.return_code == 0);

  chunk->add_bytes_written(buffer->used_bytes());
  if (delete_buffer) {
    delete buffer;
  }
}


void IoDispatcher::ChunkUploadCompleteCallback(const UploaderResults &results,
                                               Chunk* chunk) {
  assert (chunk->IsFullyProcessed());
  assert (chunk->HasUploadStreamHandle());
  assert (chunk->bytes_written() == chunk->compressed_size());

  assert (results.return_code == 0);

  chunk->file()->ChunkCommitted(chunk);

  pthread_mutex_lock(&processing_done_mutex_);
  if (--chunks_in_flight_ == 0 && files_in_flight_ == 0) {
      pthread_cond_signal(&processing_done_condition_);
  }
  pthread_mutex_unlock(&processing_done_mutex_);
}


void IoDispatcher::CommitFile(File *file) {
  pthread_mutex_lock(&files_in_flight_mutex_);
  if (--files_in_flight_ < max_files_in_flight_ / 2) {
    pthread_cond_signal(&free_slot_condition_);
  }
  pthread_mutex_unlock(&files_in_flight_mutex_);

  file_processor_->FileDone(file);
  delete file;
}
