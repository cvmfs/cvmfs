/**
 * This file is part of the CernVM File System.
 */

#ifndef UPLOAD_FILE_PROCESSING_IO_DISPATCHER_H
#define UPLOAD_FILE_PROCESSING_IO_DISPATCHER_H

#include <sys/types.h>
#include <list>

#include <tbb/atomic.h>
#include <tbb/concurrent_queue.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/tbb_thread.h>
#include <tbb/task.h>

#include <pthread.h>

#include "char_buffer.h"
#include "../upload_facility.h"

namespace upload {

class File;
class Chunk;
class FileScrubbingTask;
class FileProcessor;

typedef tbb::concurrent_bounded_queue<File*> FileQueue;

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
class Reader {
 protected:
  /**
   * Internal structure to keep information about each currently open File.
   */
  struct OpenFile {
    OpenFile() :
      file(NULL), file_marker(0), previous_task(NULL),
      previous_sync_task(NULL) {}

    File               *file;               ///< reference to the associated File structure
    int                 file_descriptor;    ///< open file descriptor of the file
    off_t               file_marker;        ///< current position of file read-in

    FileScrubbingTask  *previous_task;      ///< previously scheduled TBB task (for synchronization)
    tbb::task          *previous_sync_task; ///< previously scheduled TBB task (for synchronization)
  };

  typedef std::list<OpenFile> OpenFileList;

 public:
  Reader(FileQueue          &queue,
         const size_t        max_buffer_size,
         const unsigned int  max_buffers_in_flight) :
    queue_(queue),
    max_buffer_size_(max_buffer_size),
    max_buffers_in_flight_(max_buffers_in_flight),
    draining_(false)
  {
    buffers_in_flight_ = 0;
    pthread_mutex_init(&free_slot_mutex_,    NULL);
    pthread_cond_init(&free_slot_condition_, NULL);
  }

  ~Reader() {
    pthread_mutex_destroy(&free_slot_mutex_);
    pthread_cond_destroy(&free_slot_condition_);
  }

  /**
   * Starts the reading loop of the Reader.
   */
  void Read();

  /**
   * Releases CharBuffers that were previously allocated by the Reader. This
   * needs to be called by the processing pipeline for finished Buffers.
   */
  void ReleaseBuffer(CharBuffer *buffer) {
    delete buffer;
    pthread_mutex_lock(&free_slot_mutex_);
    if (--buffers_in_flight_ < max_buffers_in_flight_ / 2) {
      pthread_cond_signal(&free_slot_condition_);
    }
    pthread_mutex_unlock(&free_slot_mutex_);
  }

 protected:
  /** Checks if there is still data to be worked on */
  bool HasData() const { return ! draining_ || open_files_.size() > 0; }

  bool TryToAcquireNewJob(File *&new_file_job);
  void OpenNewFile(File *file);
  void CloseFile(OpenFile &file);
  bool ReadAndScheduleNextBuffer(OpenFile &open_file);

  /**
   * When all Files have been scheduled from the outside, we enable the draining
   * mode to bring all reading to an end and terminate eventually.
   */
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
  FileQueue                 &queue_;                 ///< reference to the JobQueue (see IoDispatcher)
  const size_t               max_buffer_size_;       ///< size of data Blocks to read-in
  tbb::atomic<unsigned int>  buffers_in_flight_;     ///< number of data Blocks currently in memory
  const unsigned int         max_buffers_in_flight_; ///< maximal number of Buffers in memory

  bool                       draining_;
  OpenFileList               open_files_;

  pthread_mutex_t            free_slot_mutex_;
  pthread_cond_t             free_slot_condition_;
};



/**
 * The IoDispatcher is responsible for reading Files from disk and schedule them
 * for processing. Additionally it dispatches the write back of processed data
 * Blocks. Both reading and writing are asynchronous operations and run in sep-
 * arate threads.
 * Note: For files of a certain size the IoDispatcher will not read the complete
 *       File in memory. Instead small data Blocks get loaded and scheduled in
 *       a TBB task for processing. This allows for processing of huge files w/o
 *       fully storing them in memory at any point in time.
 *
 * Writing blocks of Chunk data into the backend storage need to happen in the
 * right order! WriteJobs are scheduled in correct order and the IoDispatcher is
 * not allowed to change this order during asynchronous dispatching.
 *
 * Note: The concrete implementations of AbstractUploader will receive upload
 *       requests from the IoDispatcher in a special write thread managed by the
 *       IoDispatcher itself. If this thread gets blocked by the Uploader for an
 *       extended period of time it might affect the performance of the file
 *       processing as well.
 */
class IoDispatcher {
 protected:
  typedef void (IoDispatcher:: *MethodPtr)();

  /**
   * This Job description wraps information for the writing compressed data
   * Blocks back to disk. There are currently three different events to be pro-
   * cessed by the writing part of the IoDispatcher:
   *  -> UploadChunk
   *        Job description containing a single CharBuffer that contains a block
   *        of compressed data to be written for a specific (provided) Chunk
   *        Note: The IoDispatcher must not change the order of these Jobs,
   *              since this would lead to data corruption. Thus, WriteJobs for
   *              each individual Chunk must be scheduled in the right order!
   *
   *  -> CommitChunk
   *        After all data Blocks for a specific Chunk are successfully written
   *        and the processing of the Chunk finished, a CommitChunk Job is sent.
   *        This job contains a fully defined Chunk (finalized content hash).
   *
   *  -> TearDown
   *        This job is sent at the end of the processing run, when no more work
   *        has to be done and the write thread should terminate. Jobs scheduled
   *        after a TearDown job will not be processed anymore.
   */
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

    Chunk       *chunk;         ///< Chunk to be (partially) uploaded
    CharBuffer  *buffer;        ///< Data to be uploaded (or appended)
    bool         delete_buffer; ///< Delete the buffer after upload?
    JobType      type;          ///< Type of the WriteJob (see general info)
  };

  /**
   * This is a wrapper struct for the upload callback closure.
   * The implementation of AbstractUploader asynchronously informs the
   * IoDispatcher about a finished upload. Thus we need to associate the initial
   * management information with these asynchronous callbacks.
   */
  struct BufferUploadCompleteParam {
    BufferUploadCompleteParam(Chunk       *chunk,
                              CharBuffer  *buffer,
                              bool         delete_buffer) :
      chunk(chunk), buffer(buffer), delete_buffer(delete_buffer) {}
    Chunk       *chunk;
    CharBuffer  *buffer;
    bool         delete_buffer;
  };

 public:
  typedef tbb::concurrent_bounded_queue<WriteJob> WriteJobQueue;

 public:
  IoDispatcher(AbstractUploader  *uploader,
               FileProcessor     *file_processor,
               const size_t       max_read_buffer_size = 512 * 1024) :
    tbb_workers_(tbb::task_scheduler_init::default_num_threads()),
    max_read_buffer_size_(max_read_buffer_size),
    max_files_in_flight_(tbb_workers_ * 10),
    reader_(read_queue_, max_read_buffer_size_, max_files_in_flight_ * 5),
    read_thread_(&IoDispatcher::ThreadEntry, this, &IoDispatcher::ReadThread),
    write_thread_(&IoDispatcher::ThreadEntry, this, &IoDispatcher::WriteThread),
    uploader_(uploader),
    file_processor_(file_processor)
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
  }

  /**
   * Wrapper function to bind the IoDispatcher* to its worker thread spawning
   */
  static void ThreadEntry(IoDispatcher *delegate,
                          MethodPtr     method) {
    (*delegate.*method)();
  }

  /**
   * Waits until all scheduled files are processed
   * Note: scheduling new files for reading after calling Wait() might cause
   *       undefined behaviour
   */
  void Wait() {
    pthread_mutex_lock(&processing_done_mutex_);
    while (files_in_flight_ > 0 || chunks_in_flight_ > 0) {
      pthread_cond_wait(&processing_done_condition_, &processing_done_mutex_);
    }
    pthread_mutex_unlock(&processing_done_mutex_);
  }

  /**
   * Schedule a File object for reading by the IoDispatcher
   * Note: the IoDispatcher will read the file block-wise and schedule these
   *       Blocks for processing asynchronously.
   */
  void ScheduleRead(File *file) {
    pthread_mutex_lock(&files_in_flight_mutex_);
    while (files_in_flight_ > max_files_in_flight_) {
      pthread_cond_wait(&free_slot_condition_, &files_in_flight_mutex_);
    }
    ++files_in_flight_;
    ++file_count_;
    read_queue_.push(file);
    pthread_mutex_unlock(&files_in_flight_mutex_);
  }

  void CommitFile(File *file);

 protected:
  friend class Chunk;
  friend class File;

  /**
   * Internally called by the processing pipeline to write back processed data
   * blocks. Writing will be done by an implementation of AbstractUploader and
   * happens asynchronously.
   */
  void ScheduleWrite(Chunk       *chunk,
                     CharBuffer  *buffer,
                     const bool   delete_buffer = true) {
    assert (buffer != NULL && chunk != NULL);
    assert (buffer->used_bytes() > 0);
    write_queue_.push(WriteJob(chunk, buffer, delete_buffer));
  }

  /**
   * This is called by the processing pipeline to a schedule the commit of a
   * specific Chunk. A Chunk must be committed only after all data blocks have
   * been processed and uploaded.
   */
  void ScheduleCommit(Chunk *chunk) {
    assert (chunk != NULL);
    write_queue_.push(WriteJob(chunk));
  }

  /**
   * Newly generated Chunks need to be registered to keep track of the number
   * of Chunks currently being processed
   */
  void RegisterChunk(Chunk *chunk) {
    ++chunks_in_flight_;
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
  void WriteBufferToChunk(Chunk       *chunk,
                          CharBuffer  *buffer,
                          const bool   delete_buffer);
  void CommitChunk(Chunk* chunk);

  void ChunkUploadCompleteCallback(const UploaderResults &results, Chunk* chunk);
  void BufferUploadCompleteCallback(const UploaderResults      &results,
                                    BufferUploadCompleteParam   buffer_info);

 private:
  const unsigned int        tbb_workers_;               ///< number of TBB worker threads to be used
  const size_t              max_read_buffer_size_;      ///< maximal data block size for file read-in

  tbb::atomic<unsigned int> files_in_flight_;           ///< number of Files currently in processing
  tbb::atomic<unsigned int> chunks_in_flight_;          ///< number of Chunks currently in processing
  tbb::atomic<unsigned int> file_count_;                ///< overall number of processed files

  FileQueue                 read_queue_;                ///< JobQueue for reading (see Reader)
  WriteJobQueue             write_queue_;               ///< JobQueue for writing

  pthread_mutex_t           files_in_flight_mutex_;
  pthread_cond_t            free_slot_condition_;
  const unsigned int        max_files_in_flight_;

  pthread_mutex_t           processing_done_mutex_;
  pthread_cond_t            processing_done_condition_;

  Reader                    reader_;                    ///< dedicated File Reader object
  tbb::tbb_thread           read_thread_;
  tbb::tbb_thread           write_thread_;

  AbstractUploader         *uploader_;                  ///< (weak) reference to the used AbstractUploaer
  FileProcessor            *file_processor_;            ///< (weak) reference to the FileProcesser in command
};

} // namespace upload


#endif /* UPLOAD_FILE_PROCESSING_IO_DISPATCHER_H */
