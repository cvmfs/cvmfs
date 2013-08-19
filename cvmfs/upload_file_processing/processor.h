#ifndef UPLOAD_FILE_PROCESSING_PROCESSOR_H
#define UPLOAD_FILE_PROCESSING_PROCESSOR_H

#include <tbb/task.h>

#include "char_buffer.h"

namespace upload {

class Chunk;
class IoDispatcher;
class Reader;
class File;

class ChunkProcessingTask : public tbb::task {
 public:
  ChunkProcessingTask(Chunk        *chunk,
                      CharBuffer   *buffer) :
    chunk_(chunk), buffer_(buffer) {}

  tbb::task* execute();

 protected:
  void Crunch(const unsigned char  *data,
              const size_t          bytes,
              const bool            finalize) const;

 private:
  Chunk        *chunk_;
  CharBuffer   *buffer_;
};


class FileScrubbingTask : public tbb::task {
 protected:
  typedef std::vector<off_t> CutMarks;

 public:
  FileScrubbingTask(File *file, CharBuffer *buffer, Reader *reader) :
    file_(file), buffer_(buffer), reader_(reader), next_(NULL) {}

  void SetNext(FileScrubbingTask *next) {
    next->increment_ref_count();
    next_ = next;
  }

  tbb::task* execute();

 protected:
  bool IsLastBuffer() const;
  CutMarks FindNextChunkCutMarks();

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
  File                *file_;
  CharBuffer          *buffer_;
  Reader              *reader_;
  FileScrubbingTask   *next_;

  std::vector<Chunk*>  chunks_to_process_;
};

} // namespace upload

#endif /* UPLOAD_FILE_PROCESSING_PROCESSOR_H */
