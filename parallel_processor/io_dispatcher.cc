#include "io_dispatcher.h"

#include <cassert>
#include <sstream>
#include <cstdio>

#include "file.h"
#include "processor.h"
#include "chunk.h"
#include "util.h"


const size_t IoDispatcher::kMaxBufferSize = 512 * 1024;
IoDispatcher* IoDispatcher::instance_ = NULL;

void IoDispatcher::ReadThread() {
  tbb::task_scheduler_init sched(tbb_workers_ + 1);

  while (true) {
    File *file;
    read_queue_.pop(file);
    if (file == NULL) {
      break;
    }

#ifdef MEASURE_IO_TIME
    tbb::tick_count start = tbb::tick_count::now();
#endif
    ReadFileAndSpawnTasks(file);
#ifdef MEASURE_IO_TIME
    tbb::tick_count end = tbb::tick_count::now();
    read_time_ += (end - start).seconds();
#endif
  }
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
      {
    #ifdef MEASURE_IO_TIME
        tbb::tick_count start = tbb::tick_count::now();
    #endif
          WriteBufferToChunk(write_job.chunk,
                             write_job.buffer,
                             write_job.delete_buffer);
    #ifdef MEASURE_IO_TIME
        tbb::tick_count end = tbb::tick_count::now();
        write_time_ += (end - start).seconds();
    #endif
      }
        break;
      default:
        assert (false);
    }
  }
}


bool IoDispatcher::ReadFileAndSpawnTasks(File *file) {
  const std::string &path = file->path();
  const size_t       size = file->size();

  // open the file
  int fd;
  if ((fd = open(path.c_str(), O_RDONLY, 0)) == -1) {
    std::cerr << "cannot open file: " << path << std::endl;
    return false;
  }

  // read the file chunk-wise
  size_t       file_marker    = 0;
  unsigned int chunks_to_read = (size / kMaxBufferSize) +
                                std::min(size_t(1), size % kMaxBufferSize);

  FileScrubbingTask  *previous_task      = NULL;
  tbb::task          *previous_sync_task = NULL;

  for (unsigned int i = 0; i < chunks_to_read; ++i) {
    const size_t bytes_to_read_now = std::min(kMaxBufferSize,
                                              size - file_marker);
    CharBuffer *buffer = new CharBuffer(bytes_to_read_now);
    buffer->SetBaseOffset(file_marker);

    const size_t bytes_read = read(fd, buffer->ptr(), bytes_to_read_now);
    if (bytes_read != bytes_to_read_now) {
      std::cerr << "failed to read " << bytes_to_read_now << " from " << path
                << std::endl;
      close(fd);
      return false;
    }

    buffer->SetUsedBytes(bytes_read);

    // create an asynchronous task to process the data chunk, together with
    // a synchronisation task that ensures the correct execution order of the
    // processing tasks
    // Note: the last task for any given file does not need to wait for a
    //       synchronisation task since it has no successor.
    FileScrubbingTask  *new_task =
      new(tbb::task::allocate_root()) FileScrubbingTask(file, buffer);
    new_task->increment_ref_count();
    tbb::task *sync_task = new(new_task->allocate_child()) tbb::empty_task();

    // decorate the predecessor task (i-1) with it's successor (i) and allow
    // it to be scheduled by TBB
    // Note: all asynchronous tasks for a single file need to be processed in
    //       the correct order, since they depend on each other
    //       (This inherently kills parallelism in the first stage but every
    //        asynchronous task spawned here may spawn additional independent
    //        sub-tasks)
    if (previous_task != NULL) {
      previous_task->SetNext(new_task);
      tbb::task::enqueue(*previous_sync_task);
    }

    previous_task      = new_task;
    previous_sync_task = sync_task;
    file_marker       += bytes_read;
  }

  // make sure that the last chunk is processed
  tbb::task::enqueue(*previous_sync_task);

  // close the file
  assert (file_marker == size);
  close(fd);

  return true;
}


bool IoDispatcher::WriteBufferToChunk(Chunk       *chunk,
                                      CharBuffer  *buffer,
                                      const bool   delete_buffer) {
  assert (chunk != NULL);
  assert (buffer != NULL);
  assert (chunk->IsInitialized());
  assert (buffer->IsInitialized());

  if (! chunk->HasFileDescriptor()) {
    const std::string file_path = output_path + "/" + "chunk.XXXXXXX";
    char *tmp_file = strdupa(file_path.c_str());
    const int tmp_fd = mkstemp(tmp_file);
    if (tmp_fd < 0) {
      std::stringstream ss;
      ss << "Failed to create temporary output file (Errno: " << errno << ")";
      PrintErr(ss.str());
      return false;
    }
    chunk->set_file_descriptor(tmp_fd);
    chunk->set_temporary_path(tmp_file);
  }

  const int fd = chunk->file_descriptor();

  // write to file
  const size_t bytes_to_write = buffer->used_bytes();
  assert (bytes_to_write > 0);
  assert (chunk->bytes_written() == buffer->base_offset());
  const size_t bytes_written = write(fd, buffer->ptr(), bytes_to_write);
  if (bytes_written != bytes_to_write) {
    std::stringstream ss;
    ss << "Failed to write to file (Errno: " << errno << ")";
    PrintErr(ss.str());
    return false;
  }
  chunk->add_bytes_written(bytes_written);

  if (delete_buffer) {
    delete buffer;
  }

  return true;
}


void IoDispatcher::CommitChunk(Chunk* chunk) {
  assert (chunk->IsFullyProcessed());
  assert (chunk->HasFileDescriptor());
  assert (chunk->bytes_written() == chunk->compressed_size());

  int retval = close(chunk->file_descriptor());
  assert (retval == 0);

  const std::string final_path = output_path + "/" + chunk->sha1_string() +
                                 ((! chunk->IsBulkChunk()) ? "P" : "");

  retval = rename(chunk->temporary_path().c_str(), final_path.c_str());
  assert (retval == 0);

  chunk->owning_file()->ChunkCommitted(chunk);

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
