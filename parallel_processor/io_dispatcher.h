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
      chunk(NULL), buffer(NULL), delete_buffer(true), job_type(TearDown) {}

    WriteJob(Chunk      *chunk,
             CharBuffer *buffer,
             const bool  delete_buffer) :
      chunk(chunk), buffer(buffer), delete_buffer(delete_buffer),
      job_type(UploadChunk) {}

    WriteJob(Chunk *chunk) :
      chunk(chunk), buffer(NULL), delete_buffer(false), job_type(CommitChunk) {}

    bool IsUploadJob() const { return job_type == UploadChunk; }
    bool IsCommitJob() const { return job_type == CommitChunk; }
    bool IsTearDownJob() const { return job_type == TearDown; }

    Chunk       *chunk;
    CharBuffer  *buffer;
    bool         delete_buffer;
    JobType      job_type;
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
    // Here we go tomorrow!
  }

  void ScheduleRead(File *file) {
    ++files_in_flight_;
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

  void CommitFile(File *file) {
    --files_in_flight_;
  }

 protected:
  IoDispatcher() :
    read_thread_(&IoDispatcher::ThreadEntry, this, &IoDispatcher::ReadThread),
    write_thread_(&IoDispatcher::ThreadEntry, this, &IoDispatcher::WriteThread)
  {
    file_count_      = 0;
    files_in_flight_ = 0;
    all_enqueued_    = false;
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
  bool CommitChunk(Chunk* chunk);

 private:
  tbb::atomic<unsigned int> files_in_flight_;
  tbb::atomic<bool>         all_enqueued_;
  tbb::atomic<unsigned int> file_count_;

  double read_time_;
  double write_time_;

  tbb::concurrent_bounded_queue<File*>     read_queue_;
  tbb::concurrent_bounded_queue<WriteJob>  write_queue_;

  tbb::tbb_thread read_thread_;
  tbb::tbb_thread write_thread_;
};


#endif /* IO_DISPATCHER_H */
