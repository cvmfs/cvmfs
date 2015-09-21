/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FILE_PROCESSING_IO_DISPATCHER_H_
#define CVMFS_FILE_PROCESSING_IO_DISPATCHER_H_

#include <pthread.h>
#include <sys/types.h>
#include <tbb/atomic.h>
#include <tbb/concurrent_queue.h>
#include <tbb/task.h>
#include <tbb/task_scheduler_init.h>
#include <tbb/tbb_thread.h>

#include <list>

#include "../upload_facility.h"
#include "async_reader.h"
#include "char_buffer.h"
#include "file.h"
#include "processor.h"

namespace upload {

class Chunk;
class FileProcessor;


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

    explicit WriteJob(Chunk *chunk) :
      chunk(chunk), buffer(NULL), delete_buffer(false),
      type(CommitChunk) {}

    Chunk       *chunk;          ///< Chunk to be (partially) uploaded
    CharBuffer  *buffer;         ///< Data to be uploaded (or appended)
    bool         delete_buffer;  ///< Delete the buffer after upload?
    JobType      type;           ///< Type of the WriteJob (see general info)
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
  IoDispatcher(AbstractUploader    *uploader,
               FileProcessor       *file_processor,
               const unsigned int   number_of_threads,
               const size_t         max_read_buffer_size = 512 * 1024) :
    max_read_buffer_size_(max_read_buffer_size),
    reader_(max_read_buffer_size_, number_of_threads * 10),
    uploader_(uploader),
    file_processor_(file_processor)
  {
    chunks_in_flight_  = 0;
    file_count_        = 0;
    reader_.Initialize();
    const bool mutex_inits_successful = (
      pthread_mutex_init(&processing_done_mutex_,    NULL) == 0 &&
      pthread_cond_init(&processing_done_condition_, NULL) == 0);
    assert(mutex_inits_successful);
  }

  ~IoDispatcher() {
    Wait();

    reader_.TearDown();

    pthread_mutex_destroy(&processing_done_mutex_);
    pthread_cond_destroy(&processing_done_condition_);
  }

  /**
   * Waits until all scheduled files are processed
   * Note: scheduling new files for reading after calling Wait() might cause
   *       undefined behaviour
   */
  void Wait() {
    reader_.Wait();

    pthread_mutex_lock(&processing_done_mutex_);
    while (chunks_in_flight_ > 0) {
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
    ++file_count_;
    reader_.ScheduleRead(file);
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
                     const bool   delete_buffer = true);

  /**
   * This is called by the processing pipeline to a schedule the commit of a
   * specific Chunk. A Chunk must be committed only after all data blocks have
   * been processed and uploaded.
   */
  void ScheduleCommit(Chunk *chunk);

  /**
   * Newly generated Chunks need to be registered to keep track of the number
   * of Chunks currently being processed
   */
  void RegisterChunk(Chunk *chunk) {
    ++chunks_in_flight_;
  }

  void ChunkUploadCompleteCallback(const UploaderResults &results,
                                   Chunk* chunk);
  void BufferUploadCompleteCallback(const UploaderResults      &results,
                                    BufferUploadCompleteParam   buffer_info);

 private:
  /**
   * Maximal data block size for file read-in
   */
  const size_t max_read_buffer_size_;

  /**
   * Number of Chunks currently in processing
   */
  tbb::atomic<unsigned int> chunks_in_flight_;
  tbb::atomic<unsigned int> file_count_;  ///< overall number of processed files

  pthread_mutex_t processing_done_mutex_;
  pthread_cond_t processing_done_condition_;

  Reader<FileScrubbingTask, File> reader_;  ///< dedicated File Reader object

  /**
   * (weak) reference to the used AbstractUploaer.
   */
  AbstractUploader *uploader_;
  /**
   * (weak) reference to the FileProcesser in command
   */
  FileProcessor *file_processor_;
};

}  // namespace upload

#endif  // CVMFS_FILE_PROCESSING_IO_DISPATCHER_H_
