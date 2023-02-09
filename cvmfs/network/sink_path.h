
#ifndef CVMFS_NETWORK_SINK_PATH_H_
#define CVMFS_NETWORK_SINK_PATH_H_

#include "sink.h"
#include "util/posix.h"

namespace cvmfs {

class PathSink : public Sink {
 public:
  PathSink(const std::string &destination_path) : path_(destination_path) {
    file_ = fopen(destination_path.c_str(), "w");
  }

  virtual ~PathSink() { if (file_) { fclose(file_); unlink(path_.c_str()); } }

  virtual int64_t Write(const void *buf, uint64_t sz) {
    return fwrite(buf, 1, sz, file_);
  }

  virtual int Reset() {
    return (fflush(file_) != 0) ||
           (ftruncate(fileno(file_), 0) != 0) ||
           (freopen(NULL, "w", file_) != file_);
  }

  int Close() {
    int ret = fclose(file_);
    unlink(path_.c_str());

    return ret;
  }

  bool IsValid() {
    return file_ != NULL;
  }

 public:
  FILE *file_;
  const std::string &path_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_PATH_H_