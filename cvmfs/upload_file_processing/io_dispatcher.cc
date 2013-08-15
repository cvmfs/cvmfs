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
#include "../util_concurrency.h"
#include "../file_chunk.h"

using namespace upload;

void Reader::Read() {
  while (HasData()) {
    File *file = NULL;
    const bool popped_new_job = TryToAcquireNewJob(file);
    if (popped_new_job) {
      if (file != NULL) {
        OpenNewFile(file);
      } else {
        EnableDraining();
      }
    }

    OpenFileList::iterator       i    = open_files_.begin();
    OpenFileList::const_iterator iend = open_files_.end();
    for (; i != iend; ++i) {
      const bool finished_reading = ReadAndScheduleNextBuffer(*i);
      if (finished_reading) {
        CloseFile(*i);
        i = open_files_.erase(i);
      }
    }
  }
}


bool Reader::TryToAcquireNewJob(File *&new_file_job) {
  // in draining mode we only process files that are already open
  if (draining_) {
    return false;
  }

  File *file  = NULL;
  bool popped = false;

  // if we have no more work, we allow the thread to block otherwise we just
  // acquire what we can get
  if (open_files_.empty()) {
    queue_.pop(file);
    popped = true;
  } else {
    popped = queue_.try_pop(file);
  }

  new_file_job = file;
  return popped;
}


void Reader::OpenNewFile(File *file) {
  const int fd = open(file->path().c_str(), O_RDONLY, 0);
  assert (fd > 0);

  OpenFile open_file;
  open_file.file            = file;
  open_file.file_descriptor = fd;

  open_files_.push_back(open_file);
}


void Reader::CloseFile(OpenFile &file) {
  const int retval = close(file.file_descriptor);
  assert (retval == 0);
}


bool Reader::ReadAndScheduleNextBuffer(OpenFile &open_file) {
  assert (open_file.file != NULL);
  assert (open_file.file_descriptor > 0);

  const size_t file_size     = open_file.file->size();
  const size_t bytes_to_read =
    std::min(file_size - static_cast<size_t>(open_file.file_marker),
             max_buffer_size_);
  CharBuffer *buffer = CreateBuffer(bytes_to_read);
  buffer->SetBaseOffset(open_file.file_marker);

  const size_t bytes_read = read(open_file.file_descriptor,
                                 buffer->ptr(),
                                 bytes_to_read);
  assert (bytes_to_read == bytes_read);
  buffer->SetUsedBytes(bytes_read);
  open_file.file_marker += bytes_read;

  // create an asynchronous task to process the data chunk, together with
  // a synchronisation task that ensures the correct execution order of the
  // processing tasks
  FileScrubbingTask  *new_task =
    new(tbb::task::allocate_root()) FileScrubbingTask(open_file.file,
                                                      buffer,
                                                      this);
  new_task->increment_ref_count();
  tbb::task *sync_task = new(new_task->allocate_child()) tbb::empty_task();

  // decorate the predecessor task (i-1) with it's successor (i) and allow
  // it to be scheduled by TBB
  // Note: all asynchronous tasks for a single file need to be processed in
  //       the correct order, since they depend on each other
  //       (This inherently kills parallelism in the first stage but every
  //        asynchronous task spawned here may spawn additional independent
  //        sub-tasks)
  if (open_file.previous_task != NULL) {
    open_file.previous_task->SetNext(new_task);
    tbb::task::enqueue(*open_file.previous_sync_task);
  }

  open_file.previous_task      = new_task;
  open_file.previous_sync_task = sync_task;

  const bool finished_reading =
    (static_cast<size_t>(open_file.file_marker) == file_size);
  if (finished_reading) {
    // make sure that the last chunk is processed
    tbb::task::enqueue(*open_file.previous_sync_task);
  }

  return finished_reading;
}


void IoDispatcher::ReadThread() {
  tbb::task_scheduler_init sched(tbb_workers_ + 1);
  reader_.Read();
}


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

  if (! chunk->HasUploadStreamHandle()) {
    UploadStreamHandle *handle = uploader_->InitStreamedUpload(
      AbstractUploader::MakeClosure(&IoDispatcher::ChunkUploadCompleteCallback,
                                    this,
                                    chunk));
    assert (handle != NULL);
    chunk->set_upload_stream_handle(handle);
  }

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

  const std::string hash_suffix =
    (chunk->IsBulkChunk()) ? "" : FileChunk::kCasSuffix;

  uploader_->FinalizeStreamedUpload(chunk->upload_stream_handle(),
                                    chunk->sha1(),
                                    hash_suffix);
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
  delete file;
}
