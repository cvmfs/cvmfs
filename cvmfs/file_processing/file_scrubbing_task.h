/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FILE_PROCESSING_FILE_SCRUBBING_TASK_H_
#define CVMFS_FILE_PROCESSING_FILE_SCRUBBING_TASK_H_

#include <tbb/task.h>

#include <string>

#include "async_reader.h"

namespace upload {  // TODO(rmeusel): remove or replace

class CharBuffer;


class AbstractFile {
 public:
  AbstractFile(const std::string &path,
               const size_t       size) :
    path_(path), size_(size) {}

  const std::string& path() const { return path_; }
        size_t       size() const { return size_; }

 private:
  const std::string  path_;
  const size_t       size_;
};


template <class FileT>
class AbstractFileScrubbingTask : public tbb::task {
 public:
  AbstractFileScrubbingTask(FileT           *file,
                            CharBuffer      *buffer,
                            const bool       is_last_piece,
                            AbstractReader  *reader = NULL) :
    file_(file), buffer_(buffer), reader_(reader), is_last_(is_last_piece),
    next_(NULL) {}

        FileT*      file()         { return file_;    }
        CharBuffer* buffer()       { return buffer_;  }
  const FileT*      file()   const { return file_;    }
  const CharBuffer* buffer() const { return buffer_;  }
  bool              IsLast() const { return is_last_; }

  /** Associate the FileScrubbingTask with its successor */
  void SetNext(tbb::task *next) {
    next->increment_ref_count();
    next_ = next;
  }

 protected:
  tbb::task* Finalize() {
    reader_->ReleaseBuffer(buffer_);
    if (is_last_) {
      reader_->FinalizedFile(file_);
    }

    return Next();
  }

  /**
   * Decide if the next FileScrubbingTask can be directly returned for pro-
   * cessing in TBB
   */
  tbb::task* Next() {
    return (next_ != NULL && next_->decrement_ref_count() == 0)
      ? next_
      : NULL;
  }

 private:
  FileT *file_;  ///< the associated file that is to be processed
  CharBuffer *buffer_;  ///< the CharBuffer containing the current data Block
  /**
   * The Reader that is responsible for the given data Block
   */
  AbstractReader *reader_;
  const bool is_last_;  ///< defines if we have the last piece
  /**
   * The next FileScrubbingTask (if NULL, no more data will come after this
   * FileScrubbingTask)
   */
  tbb::task *next_;
};

}  // namespace upload

#endif  // CVMFS_FILE_PROCESSING_FILE_SCRUBBING_TASK_H_
