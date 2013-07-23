#ifndef PROCESSOR_H
#define PROCESSOR_H

#include <tbb/task.h>

#include "buffer.h"

class Chunk;
class IoDispatcher;
class Reader;
class File;

template <class CruncherT>
class ChunkCruncher {
 public:
  ChunkCruncher(Chunk        *chunk,
                CharBuffer   *buffer,
                const off_t   internal_offset,
                const size_t  byte_count,
                const bool    finalize) :
    chunk_(chunk),
    buffer_(buffer),
    internal_offset_(internal_offset),
    byte_count_(byte_count),
    finalize_(finalize)
  {
    assert (internal_offset_ < buffer->used_bytes());
    assert (internal_offset_ + byte_count_ <= buffer->used_bytes());
  }

  void operator()() const {
    // Hack: parallel_invoke expects a const callable operator... wtf
    ChunkCruncher  *self     = const_cast<ChunkCruncher*>(this);
    Chunk          *chunk    = self->chunk();
    CharBuffer     *buffer   = self->buffer();

    CruncherT::Crunch(chunk,
                      buffer_->ptr() + internal_offset_,
                      byte_count_,
                      finalize_);
  }

 protected:
  Chunk*      chunk()  { return chunk_;  }
  CharBuffer* buffer() { return buffer_; }

 private:
  ChunkCruncher(const ChunkCruncher &other) { assert (false); } // no copy!
  ChunkCruncher& operator=(const ChunkCruncher& other) { assert (false); }

 private:
  Chunk        *chunk_;
  CharBuffer   *buffer_;
  const off_t   internal_offset_;
  const size_t  byte_count_;
  const bool    finalize_;
};


class ChunkHasher {
 public:
  static void Crunch(Chunk                *chunk,
                     const unsigned char  *data,
                     const size_t          bytes,
                     const bool            finalize);
};


class ChunkCompressor {
 public:
  static void Crunch(Chunk                *chunk,
                     const unsigned char  *data,
                     const size_t          bytes,
                     const bool            finalize);
};












class ChunkProcessingTask : public tbb::task {
 public:
  ChunkProcessingTask(Chunk        *chunk,
                      CharBuffer   *buffer) :
    chunk_(chunk), buffer_(buffer) {}

  tbb::task* execute();

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

  void WaitForProcessing() {
    increment_ref_count(); // for the wait itself
    wait_for_all();
  }

  CutMarks FindNextChunkCutMarks();

  tbb::task* Next() {
    return (next_ != NULL && next_->decrement_ref_count() == 0)
      ? next_
      : NULL;
  }

  void Process(Chunk *chunk);

 public:
  File               *file_;
  CharBuffer         *buffer_;
  Reader             *reader_;
  FileScrubbingTask  *next_;
};



#endif /* PROCESSOR_H */
