/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FILE_PROCESSING_PROCESSOR_H_
#define CVMFS_FILE_PROCESSING_PROCESSOR_H_

#include <tbb/task.h>

#include <vector>

#include "char_buffer.h"
#include "file_scrubbing_task.h"

namespace upload {

class Chunk;
class IoDispatcher;
class File;

/**
 * This processes a data Block provided by the IoDispatcher in the context of a
 * specific Chunk. For a specific CharBuffer there might be more than one Chunk-
 * ProcessingTask in flight concurrently, for example to process a bulk Chunk
 * and two Chunks partially owning data inside the bounds of the CharBuffer.
 *
 * The ChunkProcessingTask is compressing the File data read by the IoDispatcher
 * in a streamed fashion. Additionally it computes the content hash of the com-
 * pressed File data. Furthermore it schedules the writing of processed data
 * Blocks.
 */
class ChunkProcessingTask : public tbb::task {
 public:
  ChunkProcessingTask(Chunk        *chunk,
                      CharBuffer   *buffer) :
    chunk_(chunk), buffer_(buffer) {}

  tbb::task* execute();

 protected:
  /**
   * Does the compression and hashing of the data provided by the IoDispatcher
   * and the FileScrubbingTasks
   *
   * @param data      raw pointer to the data to be processed
   *                  Note: this is not necessarily the beginning of a specific
   *                        CharBuffer!
   * @param bytes     the number of bytes to be processed starting from *data
   * @param finalize  flag to trigger the finalization of the streamed data
   *                  processing when the last block of data has been provided
   */
  void Crunch(const unsigned char  *data,
              const size_t          bytes,
              const bool            finalize);

 private:
  Chunk        *chunk_;   ///< the associated Chunk object (will be updated)
  CharBuffer   *buffer_;  ///< the CharBuffer containing data to be processed
                          ///< Note: not necessarily all data in this buffer is
                          ///<       applicable for the associated Chunk
                          ///<       (see: ChunkProcessingTask::execute)
};


/**
 * TBB task that processes a single data Block of a specific file. For each data
 * Block a new FileScrubbingTask is created. FileScrubbingTasks are associated
 * with their successor (if applicable) and return it for continuous and sequen-
 * tial processing of particular Files by the TBB runtime.
 *
 * The FileScrubbingTask runs over the complete file and handles the creation
 * of Chunks on the way. For each incoming data Block and each associated Chunk
 * it will spawn ChunkProcessingTasks that take care of the actual crunching of
 * Chunk contents.
 */
class FileScrubbingTask : public AbstractFileScrubbingTask<File> {
 protected:
  typedef std::vector<off_t> CutMarks;

 public:
  FileScrubbingTask(File            *file,
                    CharBuffer      *buffer,
                    const bool       is_last_piece,
                    AbstractReader  *reader) :
    AbstractFileScrubbingTask<File>(file, buffer, is_last_piece, reader) {}

  tbb::task* execute();

 protected:
  bool IsLastBuffer() const;

  /**
   * Scrubs the data in buffer_ and looks for Chunk cut marks using the Chunk-
   * Detector provided in the associated File object
   */
  CutMarks FindNextChunkCutMarks();

  void QueueForDeferredProcessing(Chunk *chunk) {
    assert(chunk != NULL);
    chunks_to_process_.push_back(chunk);
  }
  void SpawnTasksAndWaitForProcessing();
  void CommitFinishedChunks() const;

 private:
  /**
   * Filled on runtime of FileScrubbingTask with all Chunks that need to "see"
   * the data in buffer_
   */
  std::vector<Chunk*>  chunks_to_process_;
};

}  // namespace upload

#endif  // CVMFS_FILE_PROCESSING_PROCESSOR_H_
