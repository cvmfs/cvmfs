#ifndef IO_DISPATCHER_H
#define IO_DISPATCHER_H

#include <sys/types.h>

#include <tbb/atomic.h>
#include <tbb/concurrent_queue.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/tbb_thread.h>

#include <pthread.h>

#include <iostream> // TODO: remove

#include "buffer.h"

#define MEASURE_IO_TIME



class File;
class Chunk;

class IoDispatcher {
 protected:
  static const size_t kMaxBufferSize;
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

 private:
  static IoDispatcher* instance_;

 public:
  /**
   * Singleton
   */
  static IoDispatcher* Instance() {
    if (instance_ == NULL) {
      instance_ = new IoDispatcher();
    }
    return instance_;
  }

  static void Destroy() {
    if (instance_ != NULL) {
      delete instance_;
      instance_ = NULL;
    }
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
  IoDispatcher() :
    tbb_workers_(8),
    read_thread_(&IoDispatcher::ThreadEntry, this, &IoDispatcher::ReadThread),
    write_thread_(&IoDispatcher::ThreadEntry, this, &IoDispatcher::WriteThread),
    max_files_in_flight_(tbb_workers_ * 10)
  {
    files_in_flight_  = 0;
    chunks_in_flight_ = 0;
    file_count_       = 0;
    pthread_mutex_init(&processing_done_mutex_, NULL);
    pthread_mutex_init(&files_in_flight_mutex_, NULL);
    pthread_cond_init(&processing_done_condition_, NULL);
    pthread_cond_init(&free_slot_condition_, NULL);
  }

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

  tbb::atomic<unsigned int> files_in_flight_;
  tbb::atomic<unsigned int> chunks_in_flight_;
  tbb::atomic<unsigned int> file_count_;

  double read_time_;
  double write_time_;

  tbb::concurrent_bounded_queue<File*>     read_queue_;
  tbb::concurrent_bounded_queue<WriteJob>  write_queue_;

  tbb::tbb_thread read_thread_;
  tbb::tbb_thread write_thread_;

  pthread_mutex_t files_in_flight_mutex_;
  pthread_cond_t  free_slot_condition_;
  unsigned int    max_files_in_flight_;

  pthread_mutex_t processing_done_mutex_;
  pthread_cond_t  processing_done_condition_;
};


#endif /* IO_DISPATCHER_H */
