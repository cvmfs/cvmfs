/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_SINK_FILE_H_
#define CVMFS_NETWORK_SINK_FILE_H_

#include "sink.h"
#include "util/posix.h"

namespace cvmfs {

class FileSink : public Sink {
 public:
  explicit FileSink(FILE *destination_file) : file_(destination_file) {
    assert(file_ != NULL);
  }

  virtual ~FileSink() { if (file_) { fflush(file_); } }

  virtual int64_t Write(const void *buf, uint64_t sz) {
    return fwrite(buf, 1, sz, file_);
  }

  virtual int Reset() {
    return !((fflush(file_) != 0) ||
           (ftruncate(fileno(file_), 0) != 0) ||
           (freopen(NULL, "w", file_) != file_));
  }

  int Flush() {
    return fflush(file_);
  }

  bool IsValid() {
    return file_ != NULL;
  }

 public:
  FILE *file_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_FILE_H_
