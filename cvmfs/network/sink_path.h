/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_SINK_PATH_H_
#define CVMFS_NETWORK_SINK_PATH_H_

#include <string>

#include "sink.h"
#include "util/posix.h"

namespace cvmfs {

class PathSink : public Sink {
 public:
  explicit PathSink(const std::string &destination_path) :
                                                       path_(destination_path),
                                                       is_owner_(true) {
    file_ = fopen(destination_path.c_str(), "w");
    assert(file_ != NULL);
  }

  virtual ~PathSink() { if (is_owner_ && file_) { fclose(file_); } }

  virtual int64_t Write(const void *buf, uint64_t sz) {
    return fwrite(buf, 1, sz, file_);
  }

  virtual int Reset() {
    return !((fflush(file_) != 0) ||
           (ftruncate(fileno(file_), 0) != 0) ||
           (freopen(NULL, "w", file_) != file_));
  }

  int Close() {
    if (!is_owner_) {
      return 0;
    }

    int ret = fclose(file_);
    file_ = NULL;

    return ret;
  }

  void Release() {
    is_owner_ = false;
  }

  bool IsValid() {
    return file_ != NULL;
  }

 public:
  FILE *file_;
  const std::string &path_;
  bool is_owner_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_PATH_H_
