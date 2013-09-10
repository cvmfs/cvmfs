#ifndef UPLOAD_FILE_PROCESSING_FILE_SCRUBBING_TASK_H
#define UPLOAD_FILE_PROCESSING_FILE_SCRUBBING_TASK_H

#include <tbb/task.h>

#include "async_reader.h"

namespace upload { // TODO: remove or replace

class CharBuffer;

template <class FileT>
class AbstractFileScrubbingTask : public tbb::task {
 public:
  AbstractFileScrubbingTask(FileT           *file,
                            CharBuffer      *buffer,
                            AbstractReader  *reader) :
    file_(file), buffer_(buffer), reader_(reader), next_(NULL)  {}

  ~AbstractFileScrubbingTask() {
    reader_->ReleaseBuffer(buffer_);
  }

  /** Associate the FileScrubbingTask with its successor */
  void SetNext(tbb::task *next) {
    next->increment_ref_count();
    next_ = next;
  }

 protected:
  /**
   * Decide if the next FileScrubbingTask can be directly returned for pro-
   * cessing in TBB
   */
  tbb::task* Next() {
    return (next_ != NULL && next_->decrement_ref_count() == 0)
      ? next_
      : NULL;
  }

 protected:
  FileT               *file_;   ///< the associated file that is to be processed
  CharBuffer          *buffer_; ///< the CharBuffer containing the current data Block
  AbstractReader      *reader_; ///< the Reader that is responsible for the given data Block
  tbb::task           *next_;   ///< the next FileScrubbingTask
                                ///< (if NULL, no more data will come after this FileScrubbingTask)
};

}

#endif /* UPLOAD_FILE_PROCESSING_FILE_SCRUBBING_TASK_H */
