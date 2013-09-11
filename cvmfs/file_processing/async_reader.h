#ifndef CVMFS_ASYNC_READER_H
#define CVMFS_ASYNC_READER_H


/**
 * This file is part of the CernVM File System.
 */

#include <tbb/concurrent_queue.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/task.h>
#include <tbb/tbb_thread.h>

#include <list>

#include "char_buffer.h"

namespace upload { // TODO: remove this... wrong namespace (for testing)

/**
 * Wrapper function to bind an arbitrary this* to a method call in a C-style
 * spawned thread function
 */
template <class DelegateT>
void ThreadProxy(DelegateT        *delegate,
                 void (DelegateT::*method)()) {
  (*delegate.*method)();
}


class AbstractReader {

 public:
  AbstractReader(const unsigned int  max_buffers_in_flight) :
    max_buffers_in_flight_(max_buffers_in_flight)
  {
    buffers_in_flight_ = 0;
    pthread_mutex_init(&free_slot_mutex_,    NULL);
    pthread_cond_init(&free_slot_condition_, NULL);
  }

  ~AbstractReader() {
    pthread_mutex_destroy(&free_slot_mutex_);
    pthread_cond_destroy(&free_slot_condition_);
  }

  /**
   * Releases CharBuffers that were previously allocated by the Reader. This
   * needs to be called for each Buffer that was created using CreateBuffer.
   */
  void ReleaseBuffer(CharBuffer *buffer);

 protected:
  /**
   * Creates a new Buffer object with the provided size. If too many buffers
   * are currently processed, this method blocks until a Buffer slot gets free.
   */
  CharBuffer *CreateBuffer(const size_t size);


 private:
  tbb::atomic<unsigned int>  buffers_in_flight_;     ///< number of data Blocks currently in memory
  const unsigned int         max_buffers_in_flight_; ///< maximal number of Buffers in memory

  pthread_mutex_t            free_slot_mutex_;
  pthread_cond_t             free_slot_condition_;
};



/**
 * The Reader takes care of the efficient read-in of files. It (asynchronously)
 * pops read jobs from a queue and handles them. Reading of multiple big Files
 * is done concurrently in a sense that new data Blocks for a number of Files
 * are read in a round robin fashion and scheduled for processing.
 * This allows for better parallelism in the processing pipeline since each
 * individual File needs to be processed sequentially. Though data Blocks of
 * different Files are independent of each other and allow for higher throughput.
 *
 * Note: The Reader produces CharBuffers that are passed into the processing
 *       pipeline. The Reader claims ownership of these specific CharBuffers,
 *       thus they need to be released using the Reader::ReleaseBuffer() method!
 */
template <class FileScrubbingTaskT, class FileT>
class Reader : public AbstractReader {
 protected:
  /**
   * Internal structure to keep information about each currently open File.
   */
  struct OpenFile {
    OpenFile() :
      file(NULL), file_marker(0), previous_task(NULL),
      previous_sync_task(NULL) {}

    FileT               *file;               ///< reference to the associated File structure
    int                  file_descriptor;    ///< open file descriptor of the file
    off_t                file_marker;        ///< current position of file read-in

    FileScrubbingTaskT  *previous_task;      ///< previously scheduled TBB task (for synchronization)
    tbb::task           *previous_sync_task; ///< previously scheduled TBB task (for synchronization)
  };
  typedef std::list<OpenFile> OpenFileList;

  struct FileJob {
    FileJob(FileT *file) : file(file), terminate(false) {}
    FileJob() : file(NULL), terminate(true) {}

    FileT *file;
    bool   terminate;
  };
  typedef tbb::concurrent_bounded_queue<FileJob> JobQueue;

 public:
  Reader(const size_t       max_buffer_size,
         const unsigned int max_buffers_in_flight,
         const unsigned int number_of_tbb_threads =
                          tbb::task_scheduler_init::default_num_threads() + 1) :
    max_buffer_size_(max_buffer_size),
    draining_(false),
    tbb_worker_count_(number_of_tbb_threads),
    read_thread_(&ThreadProxy<Reader>,
                 this,
                 &Reader<FileScrubbingTaskT, FileT>::ReadThread)
  {}

  ~Reader() {
    if (read_thread_.joinable()) {
      // send a termination signal through the queue and wait for the read
      // thread to terminate
      queue_.push(FileJob());
      read_thread_.join();
    }
  }

  void ScheduleRead(FileT *file) {
    queue_.push(FileJob(file));
  }

 protected:
  void ReadThread();

  /** Checks if there is still data to be worked on */
  bool HasData() const { return ! draining_ || open_files_.size() > 0; }

  /**
   * When all Files have been scheduled from the outside, we enable the draining
   * mode to bring all reading to an end and terminate eventually.
   */
  void EnableDraining() { draining_ = true; }

  bool TryToAcquireNewJob(FileJob &next_job);
  void OpenNewFile(FileT          *file);
  void CloseFile(OpenFile         &file);

  bool ReadAndScheduleNextBuffer(OpenFile &open_file);

 private:
  JobQueue            queue_;           ///< reference to the JobQueue (see IoDispatcher)
  const size_t        max_buffer_size_; ///< size of data Blocks to read-in

  bool                draining_;
  OpenFileList        open_files_;

  const unsigned int  tbb_worker_count_;
  tbb::tbb_thread     read_thread_;
};

}

#include "async_reader_impl.h"

#endif /* CVMFS_ASYNC_READER_H */
