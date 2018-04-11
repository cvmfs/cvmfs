/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_INGESTION_SOURCE_H_
#define CVMFS_INGESTION_INGESTION_SOURCE_H_

#include <pthread.h>
#include <stdio.h>

#include <cerrno>
#include <string>

#include "duplex_libarchive.h"
#include "platform.h"
#include "sync_item.h"
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
                     struct archive_entry* entry, pthread_mutex_t* archive_lock,
                     pthread_cond_t* read_archive_cond, bool* can_read_archive, Signal* read_archive_signal)
      : path_(path),
        archive_(archive),
        archive_lock_(archive_lock),
        read_archive_cond_(read_archive_cond),
        can_read_archive_(can_read_archive),
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
    pthread_mutex_lock(archive_lock_);
    *can_read_archive_ = true;
    pthread_cond_broadcast(read_archive_cond_);
    pthread_mutex_unlock(archive_lock_);
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
  pthread_mutex_t* archive_lock_;
  pthread_cond_t* read_archive_cond_;
  bool* can_read_archive_;
  Signal* read_archive_signal_;
};

#endif  // CVMFS_INGESTION_INGESTION_SOURCE_H_
