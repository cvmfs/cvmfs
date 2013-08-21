#ifndef UPLOAD_FILE_PROCESSING_PROCESSOR_H
#define UPLOAD_FILE_PROCESSING_PROCESSOR_H

#include <tbb/task.h>

#include "char_buffer.h"

namespace upload {

class Chunk;
class IoDispatcher;
class Reader;
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
              const bool            finalize) const;

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
class FileScrubbingTask : public tbb::task {
 protected:
  typedef std::vector<off_t> CutMarks;

 public:
  FileScrubbingTask(File *file, CharBuffer *buffer, Reader *reader) :
    file_(file), buffer_(buffer), reader_(reader), next_(NULL) {}

  /** Associate the FileScrubbingTask with its successor */
  void SetNext(FileScrubbingTask *next) {
    next->increment_ref_count();
    next_ = next;
  }

  tbb::task* execute();

 protected:
  bool IsLastBuffer() const;

  /**
   * Scrubs the data in buffer_ and looks for Chunk cut marks using the Chunk-
   * Detector provided in the associated File object
   */
  CutMarks FindNextChunkCutMarks();

  /**
   * Decide if the next FileScrubbingTask can be directly returned for pro-
   * cessing in TBB
   */
  tbb::task* Next() {
    return (next_ != NULL && next_->decrement_ref_count() == 0)
      ? next_
      : NULL;
  }

  void QueueForDeferredProcessing(Chunk *chunk) {
    assert (chunk != NULL);
    chunks_to_process_.push_back(chunk);
  }
  void SpawnTasksAndWaitForProcessing();
  void CommitFinishedChunks() const;

 private:
  File                *file_;   ///< the associated file that is to be processed
  CharBuffer          *buffer_; ///< the CharBuffer containing the current data Block
  Reader              *reader_; ///< the Reader that is responsible for the given data Block
  FileScrubbingTask   *next_;   ///< the next FileScrubbingTask
                                ///< (if NULL, no more data will come after this FileScrubbingTask)

  std::vector<Chunk*>  chunks_to_process_; ///< Filled on runtime of FileScrubbingTask with all
                                           ///< Chunks that need to "see" the data in buffer_
};

} // namespace upload

#endif /* UPLOAD_FILE_PROCESSING_PROCESSOR_H */
