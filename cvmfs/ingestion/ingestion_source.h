/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_INGESTION_SOURCE_H_
#define CVMFS_INGESTION_INGESTION_SOURCE_H_

#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <string>

#include "duplex_libarchive.h"
#include "logging.h"
#include "platform.h"
#include "util/posix.h"
#include "util/single_copy.h"
#include "util_concurrency.h"

/*
 * The purpose of this class is to add a common interface for object that are
 * ingested by the pipeline. Hence the pipeline is able to ingest everything
 * that implements this interface.
 * The ownership of new IngestionSource objects is transfered from their creator
 * directly to the pipeline itself that will take care of deallocating
 * everything.
 * The pipeline is multithreaded so it is very likely that the code implement in
 * this interface will be called inside a different thread from the one that
 * originally
 * allocated the object, hence is necessary to take extra care in the use of
 * locks, prefer conditional variables.
 */
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
      : path_(path), fd_(-1), stat_obtained_(false) {}
  ~FileIngestionSource() {}

  std::string GetPath() const { return path_; }

  bool Open() {
    fd_ = open(path_.c_str(), O_RDONLY);
    if (fd_ < 0) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Err: Impossible to open the file: %s (%d)\n %s", path_.c_str(),
               errno, strerror(errno));
      return false;
    }
    return true;
  }

  ssize_t Read(void* buffer, size_t nbyte) {
    assert(fd_ >= 0);
    ssize_t read = SafeRead(fd_, buffer, nbyte);
    if (read < 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to read the file: %s (%d)\n %s",
               fd_, path_.c_str(), errno, strerror(errno));
    }
    return read;
  }

  bool Close() {
    if (fd_ == -1) return true;
    int ret = close(fd_);
    fd_ = -1;
    return (ret == 0);
  }

  bool GetSize(uint64_t* size) {
    if (stat_obtained_) {
      *size = stat_.st_size;
      return true;
    }
    int ret = platform_fstat(fd_, &stat_);
    if (ret == 0) {
      *size = stat_.st_size;
      stat_obtained_ = true;
      return true;
    }
    return false;
  }

 private:
  const std::string path_;
  int fd_;
  platform_stat64 stat_;
  bool stat_obtained_;
};

class TarIngestionSource : public IngestionSource {
 public:
  TarIngestionSource(const std::string path, struct archive* archive,
                     struct archive_entry* entry, Signal* read_archive_signal)
      : path_(path),
        archive_(archive),
        read_archive_signal_(read_archive_signal) {
    assert(read_archive_signal_->IsSleeping());
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
