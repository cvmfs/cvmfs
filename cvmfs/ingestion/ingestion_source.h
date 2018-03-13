/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_INGESTION_SOURCE_H_
#define CVMFS_INGESTION_INGESTION_SOURCE_H_

#include <stdio.h>
#include <string>

#include "platform.h"
#include "util/posix.h"
#include "util/single_copy.h"

class IngestionSource : SingleCopy {
 public:
  virtual ~IngestionSource() {}
  virtual bool Open() = 0;
  virtual ssize_t Read(void* buffer, size_t nbyte) = 0;
  virtual bool Close() = 0;
  virtual bool GetSize(uint64_t* size) = 0;
};

class FileIngestionSource : public IngestionSource {
 public:
  explicit FileIngestionSource(const std::string& path) : path_(path) {}
  ~FileIngestionSource() {  // Close();
  }

  bool Open() {
    fd_ = open(path_.c_str(), O_RDONLY);
    if (fd_ < 0) {
      printf("Err: Impossible to open the file. fd = %d -> errno = %d => %s\n",
             fd_, errno, strerror(errno));
      return false;
    }
    return true;
  }

  ssize_t Read(void* buffer, size_t nbyte) {
    assert(fd_ >= 0);
    ssize_t read = SafeRead(fd_, buffer, nbyte);
    if (read < 0) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "failed to read fd = %d | path = %s (%d) %s", fd_, path_.c_str(),
               errno, strerror(errno));
    }
    return read;
  }

  bool Close() {
    if (fd_ == -1) return true;
    int ret = close(fd_);
    fd_ = -1;
    if (ret == 0) {
      return true;
    }
    return false;
  }

  bool GetSize(uint64_t* size) {
    platform_stat64 info;
    int ret = platform_fstat(fd_, &info);
    *size = info.st_size;
    return (ret == 0);
  }

 private:
  const std::string path_;
  int fd_;
};

class TarIngestionSource : public IngestionSource {
 public:
  TarIngestionSource(char* buffer, uint64_t size, std::string filename)
      : buffer_(buffer),
        buffer_seek_ptr_(buffer),
        size_(size),
        offset_read_(0),
        filename_(filename) {}
  bool Open() {
    assert(buffer_);
    assert(size_);
    printf("TarIngestionSource::Open | Opening tar of file %s\n",
           filename_.c_str());
    return true;
  }

  ssize_t Read(void* external_buffer, size_t nbytes) {
    printf("TarIngestionSource::Read | Reading from tar of file %s\n",
           filename_.c_str());
    uint64_t max_to_copy =
        std::min(nbytes, size_ - (buffer_seek_ptr_ - buffer_));

    memcpy(external_buffer, buffer_seek_ptr_, max_to_copy);
    buffer_seek_ptr_ += max_to_copy;

    assert(buffer_seek_ptr_ - buffer_ >= 0);
    assert((buffer_seek_ptr_ - buffer_) - size_ <= 0);

    return max_to_copy;
  }

  bool Close() {
    printf("TarIngestionSource::Close | Closing tar file %s\n",
           filename_.c_str());

    return true;
  }

  bool GetSize(uint64_t* size) {
    *size = size_;
    return true;
  }

 private:
  char* buffer_;
  char* buffer_seek_ptr_;
  uint64_t size_;
  uint64_t offset_read_;
  std::string filename_;
};

#endif  // CVMFS_INGESTION_INGESTION_SOURCE_H_
