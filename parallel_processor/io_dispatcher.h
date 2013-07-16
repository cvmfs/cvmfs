#ifndef IO_DISPATCHER_H
#define IO_DISPATCHER_H

#include <sys/types.h>

#include <tbb/atomic.h>
#include <tbb/concurrent_queue.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/tbb_thread.h>

#include <iostream> // TODO: remove

#include "buffer.h"

#define MEASURE_IO_TIME



class File;
class Chunk;

class IoDispatcher {
 protected:
  static const size_t kMaxBufferSize;

  typedef void (IoDispatcher:: *MethodPtr)();

 public:
  IoDispatcher() :
    read_thread_(&IoDispatcher::ThreadEntry, this, &IoDispatcher::ReadThread),
    write_thread_(&IoDispatcher::ThreadEntry, this, &IoDispatcher::WriteThread)
  {
    file_count_      = 0;
    files_in_flight_ = 0;
    all_enqueued_    = false;
  }

  ~IoDispatcher() {
    Wait();

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
    all_enqueued_ = true;

    read_queue_.push(NULL);

    if (read_thread_.joinable()) {
      read_thread_.join();
    }

    if (write_thread_.joinable()) {
      write_thread_.join();
    }
  }

  void ScheduleRead(File *file) {
    read_queue_.push(file);
    ++file_count_;
  }

  void ScheduleWrite(Chunk *chunk, CharBuffer *buffer) {
    write_queue_.push(std::make_pair(chunk, buffer));
  }

  void ScheduleCommit(Chunk *chunk) {
    write_queue_.push(std::make_pair(chunk, static_cast<CharBuffer*>(NULL)));
  }

 protected:
  void ReadThread();
  void WriteThread();

  bool ReadFileAndSpawnTasks(File *file);
  bool WriteBufferToChunk(Chunk* chunk, CharBuffer *buffer);
  bool CommitChunk(Chunk* chunk);

 private:
  tbb::atomic<unsigned int> files_in_flight_;
  tbb::atomic<bool>         all_enqueued_;
  tbb::atomic<unsigned int> file_count_;

  double read_time_;
  double write_time_;

  tbb::concurrent_bounded_queue<File*> read_queue_;
  tbb::concurrent_bounded_queue<std::pair<Chunk*, CharBuffer*> > write_queue_;

  tbb::tbb_thread read_thread_;
  tbb::tbb_thread write_thread_;
};


#endif /* IO_DISPATCHER_H */
