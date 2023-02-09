
#ifndef CVMFS_NETWORK_SINK_MEM_H_
#define CVMFS_NETWORK_SINK_MEM_H_

#include <cstring>

#include "sink.h"
#include "util/posix.h"
#include "util/smalloc.h"

namespace cvmfs {

class MemSink : public Sink {
 public:
  MemSink() : size_(0), pos_(0), data_(NULL), is_owner_(true) {};
  MemSink(size_t size) : size_(size), pos_(0) {
    data_ = static_cast<char *>(smalloc(size));
    is_owner_ = true;
  }

  virtual ~MemSink() { if (is_owner_ && data_) { free(data_); } }

  virtual int64_t Write(const void *buf, uint64_t sz) {
    if (pos_ + sz > size_) {
      return pos_ + sz - size_;
    }

    memcpy(data_ + pos_, buf, sz);
    pos_ += sz;
    return 0;
  }

  virtual int Reset() {
    int ret = 1;
    if (is_owner_ && data_) {
      free(data_);
      ret = 0;
    }

    data_ = NULL;
    size_ = 0;
    pos_ = 0;
    
    return ret;
  }

  virtual bool IsValid() {
    return (size_ == 0 && pos_ == 0 && data_ == NULL) ||
           (size_ > 0 && pos_ >= 0 && data_ != NULL);
  }

  void AllocData(size_t size) {
    if (is_owner_ && data_) {
      free(data_);
    }

    is_owner_ = true;
    size_ = size;
    pos_ = 0;
    if (size == 0) {
      data_ = NULL;
    } else {
      data_ = static_cast<char *>(smalloc(size));
    }
  }

  void Release() {
    is_owner_ = false;
  }

  void Set(size_t size, size_t pos, char *data, bool is_owner=true) {
    assert(size >= pos);

    if (is_owner_ && data_) {
      free(data_);
    }

    is_owner_ = is_owner;
    size_ = size;
    pos_ = pos;
    data_ = data;
  }

 public:
  size_t size_;
  size_t pos_;
  char *data_;
  bool is_owner_;
};

}  // namespace cvmfs

#endif  // CVMFS_NETWORK_SINK_MEM_H_