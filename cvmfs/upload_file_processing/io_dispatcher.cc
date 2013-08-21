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
#include "../file_chunk.h"

using namespace upload;

void Reader::Read() {
  while (HasData()) {
    // acquire a new job from the job queue:
    // -> if the queue is empty, just continue on the work currently in flight
    // -> if the popped value is NULL, the drainout is initiated
    File *file = NULL;
    const bool popped_new_job = TryToAcquireNewJob(file);
    if (popped_new_job) {
      if (file != NULL) {
        OpenNewFile(file);
      } else {
        EnableDraining();
      }
    }

    // read File Blocks in a round robin fashion and schedule these blocks for
    // processing
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
  // acquire what we can get and continue working
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


  // All asynchronous tasks for a single File need to be processed sequentially,
  // since they depend on each other. We use the following TBB task graph to
  // produce this behaviour.
  //
  // => Task graph for one specific File with multipe data Blocks:
  //      FileScrubbingTask -> FST
  //      tbb::empty_task   -> SyncTask
  //
  // +----------+     +----------+     +----------+     +----------+
  // | SyncTask |     | SyncTask |     | SyncTask |     | SyncTask |
  // +----------+     +----------+     +----------+     +----------+
  //      |                |                |                |
  //     `|´              `|´              `|´              `|´
  //   +-----+          +-----+          +-----+          +-----+
  //   | FST |--------->| FST |--------->| FST |--------->| FST |
  //   +-----+          +-----+          +-----+          +-----+
  //
  // Each FST is associated with a SyncTask as well as it's successing FST. FSTs
  // depend on their SyncTasks, thus they only run after this SyncTask has been
  // executed.
  // An FST must not be executed by TBB before the association to it's successor
  // has been established and thus cannot be simply spawned directly after
  // creation (race condition - TBB against Reader thread).
  //
  // Given a big File that needs to be read in multiple data Blocks we will see
  // the following synchronization pattern after the first data Block has been
  // loaded into memory:
  // We create FST-0 together with SyncTask-0 and store both without spawning
  // them in TBB (thus they will not run yet). As soon as the second data Block
  // of the File arrives, FST-1 together with SyncTask-1 is created and FST-0 is
  // given a pointer to FST-1. We then enqueue SyncTask-0 to TBB and thus allow
  // the associated FST-0 to run and process the first data Block.
  // When FST-0 finishes, it will return FST-1 to TBB for execution (forcing the
  // sequential order of FSTs).
  // Now FST-1 needs to wait for SyncTask-1 to run before it can execute. Given
  // that the third data Block was already loaded in the mean time (associating
  // FST-1 with it's successor FST-2 and enqueuing SyncTask-1) TBB can run FST-1
  // immediately. Otherwise TBB needs to suspend the processing of this file and
  // wait until more data arrives.



  // figure out how many bytes need to be read in this step and create a
  // CharBuffer to accomodate these bytes
  const size_t file_size     = open_file.file->size();
  const size_t bytes_to_read =
    std::min(file_size - static_cast<size_t>(open_file.file_marker),
             max_buffer_size_);
  CharBuffer *buffer = CreateBuffer(bytes_to_read);
  buffer->SetBaseOffset(open_file.file_marker);

  // read the next data Block into the just created CharBuffer and check if
  // everything worked as expected
  const size_t bytes_read = read(open_file.file_descriptor,
                                 buffer->ptr(),
                                 bytes_to_read);
  assert (bytes_to_read == bytes_read);
  buffer->SetUsedBytes(bytes_read);
  open_file.file_marker += bytes_read;

  // create an asynchronous task (FileScrubbingTask) to process the data chunk,
  // together with a synchronization task that ensures the correct execution
  // order of the FileScrubbingTasks
  FileScrubbingTask  *new_task =
    new(tbb::task::allocate_root()) FileScrubbingTask(open_file.file,
                                                      buffer,
                                                      this);
  new_task->increment_ref_count();
  tbb::task *sync_task = new(new_task->allocate_child()) tbb::empty_task();

  // decorate the predecessor task (i-1) with it's successor (i) and allow the
  // predecessor to be scheduled by TBB (task::enqueue)
  if (open_file.previous_task != NULL) {
    open_file.previous_task->SetNext(new_task);
    tbb::task::enqueue(*open_file.previous_sync_task);
  }

  open_file.previous_task      = new_task;
  open_file.previous_sync_task = sync_task;

  // make sure that the last chunk is processed
  const bool finished_reading =
    (static_cast<size_t>(open_file.file_marker) == file_size);
  if (finished_reading) {
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

  const std::string hash_suffix =
    (chunk->IsBulkChunk()) ? "" : FileChunk::kCasSuffix;

  // Finalize the streamed upload for the comitted Chunk
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

  file_processor_->FileDone(file);
  delete file;
}
