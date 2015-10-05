/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FILE_PROCESSING_ASYNC_READER_H_
#define CVMFS_FILE_PROCESSING_ASYNC_READER_H_

#include <tbb/concurrent_queue.h>
#include <tbb/task.h>
#include <tbb/tbb_thread.h>

#include <list>
#include <string>

#include "../util_concurrency.h"
#include "char_buffer.h"

// TODO(rmeusel): remove this... wrong namespace (for testing)
namespace upload {

class AbstractFile;

class AbstractReader {
 public:
  explicit AbstractReader(const unsigned int max_buffers_in_flight) :
    buffers_in_flight_counter_(max_buffers_in_flight)
  {}

  virtual ~AbstractReader() {}

  /**
   * Releases CharBuffers that were previously allocated by the Reader. This
   * needs to be called for each Buffer that was created using CreateBuffer.
   */
  void ReleaseBuffer(CharBuffer *buffer);

  /**
   * Gets called by the AbstractFileScrubbingTask for each fully read file
   */
  virtual void FinalizedFile(AbstractFile *file) = 0;

 protected:
  /**
   * Creates a new Buffer object with the provided size. If too many buffers
   * are currently processed, this method blocks until a Buffer slot gets free.
   */
  CharBuffer *CreateBuffer(const size_t size);


 private:
  SynchronizingCounter<uint32_t> buffers_in_flight_counter_;
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
class Reader : public AbstractReader,
               public Observable<FileT*> {
 protected:
  /**
   * Internal structure to keep information about each currently open File.
   */
  struct OpenFile {
    OpenFile() :
      file(NULL), file_descriptor(0), file_marker(0), previous_task(NULL),
      previous_sync_task(NULL) {}

    FileT *file;  ///< reference to the associated File structure
    int file_descriptor;  ///< open file descriptor of the file
    off_t file_marker;  ///< current position of file read-in

    /**
     * Previously scheduled TBB task (for synchronization)
     */
    FileScrubbingTaskT *previous_task;
    /**
     * Previously scheduled TBB task (for synchronization)
     */
    tbb::task *previous_sync_task;
  };
  typedef std::list<OpenFile> OpenFileList;

  struct FileJob {
    explicit FileJob(FileT *file) : file(file), terminate(false) {}
    FileJob() : file(NULL), terminate(true) {}

    FileT *file;
    bool   terminate;
  };
  typedef tbb::concurrent_bounded_queue<FileJob> JobQueue;

 public:
  Reader(const size_t       max_buffer_size,
         const unsigned int max_files_in_flight) :
    AbstractReader(max_files_in_flight * 5),
    max_buffer_size_(max_buffer_size),
    draining_(false),
    files_in_flight_counter_(max_files_in_flight),
    running_(false) {}

  virtual ~Reader() {
    assert(!running_);
  }

  bool Initialize();
  void TearDown() { Terminate(); }

  void ScheduleRead(FileT *file) {
    assert(running_);
    ++files_in_flight_counter_;
    queue_.push(FileJob(file));
  }

  void Wait() {
    files_in_flight_counter_.WaitForZero();
  }

  void Terminate() {
    Wait();

    if (running_ && read_thread_.joinable()) {
      // send a termination signal through the queue and wait for the read
      // thread to terminate
      queue_.push(FileJob());
      read_thread_.join();
      running_ = false;
    }
  }

 protected:
  void ReadThread();

  /** Checks if there is still data to be worked on */
  bool HasData() const { return !draining_ || open_files_.size() > 0; }

  /**
   * When all Files have been scheduled from the outside, we enable the draining
   * mode to bring all reading to an end and terminate eventually.
   */
  void EnableDraining() { draining_ = true; }

  bool TryToAcquireNewJob(FileJob *next_job);
  void OpenNewFile(FileT          *file);
  void CloseFile(OpenFile         *file);

  bool ReadAndScheduleNextBuffer(OpenFile *open_file);

  void FinalizedFile(AbstractFile *file);

 private:
  JobQueue queue_;  ///< reference to the JobQueue (see IoDispatcher)
  const size_t max_buffer_size_;  ///< size of data Blocks to read-in

  bool                            draining_;
  OpenFileList                    open_files_;

  SynchronizingCounter<uint32_t>  files_in_flight_counter_;

  tbb::tbb_thread                 read_thread_;
  Future<bool>                    thread_started_executing_;
  bool                            running_;
};

}  // namespace upload

#include "async_reader_impl.h"

#endif  // CVMFS_FILE_PROCESSING_ASYNC_READER_H_
