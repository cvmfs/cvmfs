/**
 * This file is part of the CernVM File System.
 */

#include "io_dispatcher.h"

#include <cassert>
#include <cerrno>

#include "file.h"
#include "processor.h"
#include "chunk.h"
#include "file_processor.h"
#include "../util_concurrency.h"

using namespace upload;

void IoDispatcher::ScheduleWrite(Chunk       *chunk,
                                 CharBuffer  *buffer,
                                 const bool   delete_buffer) {
  assert (chunk != NULL);
  assert (chunk->IsInitialized());

  assert (buffer != NULL);
  assert (buffer->IsInitialized());

  // Initialize a streamed upload in the AbstractUploader implementation if it
  // has not been done before for this Chunk.
  if (! chunk->HasUploadStreamHandle()) {
    UploadStreamHandle *handle = uploader_->InitStreamedUpload(
      // the closure passed here, is called by the AbstractUploader as soon as
      // it successfully committed the complete chunk
      AbstractUploader::MakeClosure(&IoDispatcher::ChunkUploadCompleteCallback,
                                    this,
                                    chunk));
    if (handle == NULL) {
      LogCvmfs(kLogSpooler, kLogStderr, "initiating streamed upload failed");
      abort();
    }

    chunk->set_upload_stream_handle(handle);
  }

  // Schedule the upload of the provided data Block into the chunk
  uploader_->ScheduleUpload(chunk->upload_stream_handle(), buffer,
    AbstractUploader::MakeClosure(&IoDispatcher::BufferUploadCompleteCallback,
                                  this,
                                  BufferUploadCompleteParam(chunk,
                                                            buffer,
                                                            delete_buffer)));
}


void IoDispatcher::ScheduleCommit(Chunk* chunk) {
  assert (chunk->IsFullyProcessed());
  assert (chunk->HasUploadStreamHandle());

  // Finalize the streamed upload for the committed Chunk
  uploader_->ScheduleCommit(chunk->upload_stream_handle(),
                            chunk->content_hash(),
                            chunk->hash_suffix());
}


void IoDispatcher::BufferUploadCompleteCallback(
                                      const UploaderResults      &results,
                                      BufferUploadCompleteParam   buffer_info) {
  Chunk      *chunk         = buffer_info.chunk;
  CharBuffer *buffer        = buffer_info.buffer;
  const bool  delete_buffer = buffer_info.delete_buffer;

  if (results.return_code != 0) {
    LogCvmfs(kLogSpooler, kLogStderr, "buffer upload failed (code: %d)",
      results.return_code);
    abort();
  }

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

  if (results.return_code != 0) {
    LogCvmfs(kLogSpooler, kLogStderr, "chunk upload failed (code: %d)",
      results.return_code);
    abort();
  }

  chunk->file()->ChunkCommitted(chunk);

  pthread_mutex_lock(&processing_done_mutex_);
  if (--chunks_in_flight_ == 0) {
      pthread_cond_signal(&processing_done_condition_);
  }
  pthread_mutex_unlock(&processing_done_mutex_);
}


void IoDispatcher::CommitFile(File *file) {
  file_processor_->FileDone(file);
  delete file;
}
