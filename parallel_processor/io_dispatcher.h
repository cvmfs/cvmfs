#ifndef IO_DISPATCHER_H
#define IO_DISPATCHER_H

#include <sys/types.h>
#include <list>

#include <tbb/atomic.h>
#include <tbb/concurrent_queue.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/tbb_thread.h>

#include <pthread.h>

#include <iostream> // TODO: remove
#include <sstream>  // TODO:    "

#include "buffer.h"

//#define MEASURE_IO_TIME

class File;
class Chunk;
class FileScrubbingTask;

namespace tbb {
  class task;
}

typedef tbb::concurrent_bounded_queue<File*> FileQueue;


class Reader {
 protected:
  struct OpenFile {
    OpenFile() :
      file(NULL), file_marker(0), previous_task(NULL),
      previous_sync_task(NULL) {}

    File               *file;
    int                 file_descriptor;
    off_t               file_marker;

    FileScrubbingTask  *previous_task;
    tbb::task          *previous_sync_task;
  };

  typedef std::list<OpenFile> OpenFileList;

 public:
  Reader(FileQueue          &queue,
         const size_t        max_buffer_size,
         const unsigned int  max_buffers_in_flight) :
    queue_(queue),
    max_buffer_size_(max_buffer_size), draining_(false),
    max_buffers_in_flight_(max_buffers_in_flight)
  {
    buffers_in_flight_ = 0;
    pthread_mutex_init(&free_slot_mutex_,    NULL);
    pthread_cond_init(&free_slot_condition_, NULL);
  }

  ~Reader() {
    pthread_mutex_destroy(&free_slot_mutex_);
    pthread_cond_destroy(&free_slot_condition_);
  }

  void Read();
  void ReleaseBuffer(CharBuffer *buffer) {
    delete buffer;
    pthread_mutex_lock(&free_slot_mutex_);
    if (--buffers_in_flight_ < max_buffers_in_flight_ / 2) {
      pthread_cond_signal(&free_slot_condition_);
    }
    pthread_mutex_unlock(&free_slot_mutex_);
  }

 protected:
  bool HasData() const { return ! draining_ || open_files_.size() > 0; }

  bool TryToAcquireNewJob(File *&new_file_job);
  void OpenNewFile(File *file);
  void CloseFile(OpenFile &file);
  bool ReadAndScheduleNextBuffer(OpenFile &open_file);

  void EnableDraining() { draining_ = true; }

  CharBuffer *CreateBuffer(const size_t size) {
    pthread_mutex_lock(&free_slot_mutex_);
    while (buffers_in_flight_ > max_buffers_in_flight_) {
      pthread_cond_wait(&free_slot_condition_, &free_slot_mutex_);
    }
    ++buffers_in_flight_;
    pthread_mutex_unlock(&free_slot_mutex_);

    CharBuffer *buffer = new CharBuffer(size);
    return buffer;
  }

 private:
  FileQueue                 &queue_;
  const size_t               max_buffer_size_;
  tbb::atomic<unsigned int>  buffers_in_flight_;
  const unsigned int         max_buffers_in_flight_;

  bool                       draining_;
  OpenFileList               open_files_;

  pthread_mutex_t            free_slot_mutex_;
  pthread_cond_t             free_slot_condition_;
};




class IoDispatcher {
 protected:
  typedef void (IoDispatcher:: *MethodPtr)();

  struct WriteJob {
    enum JobType {
      UploadChunk,
      CommitChunk,
      TearDown
    };

    WriteJob() :
      chunk(NULL), buffer(NULL), delete_buffer(true),
      type(TearDown) {}

    WriteJob(Chunk      *chunk,
             CharBuffer *buffer,
             const bool  delete_buffer) :
      chunk(chunk), buffer(buffer), delete_buffer(delete_buffer),
      type(UploadChunk) {}

    WriteJob(Chunk *chunk) :
      chunk(chunk), buffer(NULL), delete_buffer(false),
      type(CommitChunk) {}

    Chunk       *chunk;
    CharBuffer  *buffer;
    bool         delete_buffer;
    JobType      type;
  };

 public:
  typedef tbb::concurrent_bounded_queue<WriteJob> WriteJobQueue;

 public:
  IoDispatcher(const size_t max_read_buffer_size = 512 * 1024) :
    tbb_workers_(tbb::task_scheduler_init::default_num_threads()),
    max_read_buffer_size_(max_read_buffer_size),
    max_files_in_flight_(tbb_workers_ * 10),
    read_thread_(&IoDispatcher::ThreadEntry, this, &IoDispatcher::ReadThread),
    write_thread_(&IoDispatcher::ThreadEntry, this, &IoDispatcher::WriteThread),
    reader_(read_queue_, max_read_buffer_size_, max_files_in_flight_ * 5)
  {
    files_in_flight_   = 0;
    chunks_in_flight_  = 0;
    file_count_        = 0;
    pthread_mutex_init(&processing_done_mutex_,    NULL);
    pthread_mutex_init(&files_in_flight_mutex_,    NULL);
    pthread_cond_init(&processing_done_condition_, NULL);
    pthread_cond_init(&free_slot_condition_,       NULL);
  }

  ~IoDispatcher() {
    Wait();
    TearDown();

     pthread_mutex_destroy(&processing_done_mutex_);
     pthread_mutex_destroy(&files_in_flight_mutex_);
     pthread_cond_destroy(&processing_done_condition_);
     pthread_cond_destroy(&free_slot_condition_);

#ifdef MEASURE_IO_TIME
    std::cout << "Reads took:  " << read_time_ << std::endl
              << "  average:   " << (read_time_ / file_count_) << std::endl
              << "Writes took: " << write_time_ << std::endl
              << "  average:   " << (write_time_ / file_count_) << std::endl;
#endif
  }

  static void ThreadEntry(IoDispatcher *delegate,
                          MethodPtr     method) {
    (*delegate.*method)();
  }

  void Wait() {
    pthread_mutex_lock(&processing_done_mutex_);
    while (files_in_flight_ > 0 || chunks_in_flight_ > 0) {
      pthread_cond_wait(&processing_done_condition_, &processing_done_mutex_);
    }
    pthread_mutex_unlock(&processing_done_mutex_);
  }

  void RegisterChunk(Chunk *chunk) {
    ++chunks_in_flight_;
  }

  void ScheduleRead(File *file) {
    pthread_mutex_lock(&files_in_flight_mutex_);
    while (files_in_flight_ > max_files_in_flight_) {
      pthread_cond_wait(&free_slot_condition_, &files_in_flight_mutex_);
    }
    ++files_in_flight_;
    pthread_mutex_unlock(&files_in_flight_mutex_);
    ++file_count_;
    read_queue_.push(file);
  }

  void ScheduleWrite(Chunk       *chunk,
                     CharBuffer  *buffer,
                     const bool   delete_buffer = true) {
    assert (buffer != NULL && chunk != NULL);
    assert (buffer->used_bytes() > 0);
    write_queue_.push(WriteJob(chunk, buffer, delete_buffer));
  }

  void ScheduleCommit(Chunk *chunk) {
    assert (chunk != NULL);
    write_queue_.push(WriteJob(chunk));
  }

  void CommitFile(File *file);

 protected:
  void TearDown() {
    assert(read_thread_.joinable());
    assert(write_thread_.joinable());

    read_queue_.push(NULL);
    write_queue_.push(WriteJob());

    read_thread_.join();
    write_thread_.join();
  }

  void ReadThread();
  void WriteThread();

  bool ReadFileAndSpawnTasks(File *file);
  bool WriteBufferToChunk(Chunk       *chunk,
                          CharBuffer  *buffer,
                          const bool   delete_buffer);
  void CommitChunk(Chunk* chunk);

 private:
  const unsigned int        tbb_workers_;
  const size_t              max_read_buffer_size_;

  tbb::atomic<unsigned int> files_in_flight_;
  tbb::atomic<unsigned int> chunks_in_flight_;
  tbb::atomic<unsigned int> file_count_;

  double read_time_;
  double write_time_;

  FileQueue     read_queue_;
  WriteJobQueue write_queue_;

  pthread_mutex_t    files_in_flight_mutex_;
  pthread_cond_t     free_slot_condition_;
  const unsigned int max_files_in_flight_;

  pthread_mutex_t processing_done_mutex_;
  pthread_cond_t  processing_done_condition_;

  tbb::tbb_thread read_thread_;
  tbb::tbb_thread write_thread_;
  Reader          reader_;
};


#endif /* IO_DISPATCHER_H */
