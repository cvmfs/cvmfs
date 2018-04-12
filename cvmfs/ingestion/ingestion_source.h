/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_INGESTION_SOURCE_H_
#define CVMFS_INGESTION_INGESTION_SOURCE_H_

#include <pthread.h>

#include <cerrno>
#include <cstdio>
#include <string>

#include "duplex_libarchive.h"
#include "platform.h"
#include "util/posix.h"
#include "util/single_copy.h"
#include "util_concurrency.h"

class IngestionSource : SingleCopy {
 public:
  virtual ~IngestionSource() {}
  virtual std::string GetPath() const = 0;
  virtual bool Open() = 0;
  virtual ssize_t Read(void* buffer, size_t nbyte) = 0;
  virtual bool Close() = 0;
  virtual bool GetSize(uint64_t* size) = 0;
};

class FileIngestionSource : public IngestionSource {
 public:
  explicit FileIngestionSource(const std::string& path)
      : path_(path), fd_(-1) {}
  ~FileIngestionSource() {  // Close();
  }

  std::string GetPath() const { return path_; }

  bool Open() {
    fd_ = open(path_.c_str(), O_RDONLY);
    if (fd_ < 0) {
      LogCvmfs(
          kLogCvmfs, kLogStderr,
          "Err: Impossible to open the file. path => %s fd = %d -> errno = %d "
          "=> %s\n",
          path_.c_str(), fd_, errno, strerror(errno));
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
  TarIngestionSource(std::string path, struct archive* archive,
                     struct archive_entry* entry, Signal* read_archive_signal)
      : path_(path),
        archive_(archive),
        read_archive_signal_(read_archive_signal) {
    const struct stat* stat_ = archive_entry_stat(entry);
    size_ = stat_->st_size;
  }

  std::string GetPath() const { return path_; }

  bool Open() {
    assert(size_ >= 0);
    return true;
  }

  ssize_t Read(void* external_buffer, size_t nbytes) {
    return archive_read_data(archive_, external_buffer, nbytes);
  }

  bool Close() {
    read_archive_signal_->Wakeup();
    return true;
  }

  bool GetSize(uint64_t* size) {
    *size = size_;
    return true;
  }

 private:
  std::string path_;
  struct archive* archive_;
  uint64_t size_;
  Signal* read_archive_signal_;
};

#endif  // CVMFS_INGESTION_INGESTION_SOURCE_H_
