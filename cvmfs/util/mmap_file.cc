/**
 * This file is part of the CernVM File System.
 *
 * Some common functions.
 */

#ifndef __STDC_FORMAT_MACROS
// NOLINTNEXTLINE
#define __STDC_FORMAT_MACROS
#endif


#include "mmap_file.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cassert>

#include "util/logging.h"
#include "util/platform.h"

using namespace std;  // NOLINT

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif


MemoryMappedFile::MemoryMappedFile(const std::string &file_path)
    : file_path_(file_path)
    , file_descriptor_(-1)
    , mapped_file_(NULL)
    , mapped_size_(0)
    , mapped_(false) { }

MemoryMappedFile::~MemoryMappedFile() {
  if (IsMapped()) {
    Unmap();
  }
}

bool MemoryMappedFile::Map() {
  assert(!mapped_);

  // open the file
  int fd;
  if ((fd = open(file_path_.c_str(), O_RDONLY, 0)) == -1) {
    LogCvmfs(kLogUtility, kLogStderr, "failed to open %s (%d)",
             file_path_.c_str(), errno);
    return false;
  }

  // get file size
  platform_stat64 filesize;
  if (platform_fstat(fd, &filesize) != 0) {
    LogCvmfs(kLogUtility, kLogStderr, "failed to fstat %s (%d)",
             file_path_.c_str(), errno);
    close(fd);
    return false;
  }

  // check if the file is empty and 'pretend' that the file is mapped
  // --> buffer will then look like a buffer without any size...
  void *mapping = NULL;
  if (filesize.st_size > 0) {
    // map the given file into memory
    mapping = mmap(NULL, filesize.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapping == MAP_FAILED) {  // NOLINT(performance-no-int-to-ptr)
      LogCvmfs(kLogUtility, kLogStderr,
               "failed to mmap %s (file size: %ld) "
               "(errno: %d)",
               file_path_.c_str(), filesize.st_size, errno);
      close(fd);
      return false;
    }
  }

  // save results
  mapped_file_ = static_cast<unsigned char *>(mapping);
  file_descriptor_ = fd;
  mapped_size_ = filesize.st_size;
  mapped_ = true;
  LogCvmfs(kLogUtility, kLogVerboseMsg, "mmap'ed %s", file_path_.c_str());
  return true;
}

void MemoryMappedFile::Unmap() {
  assert(mapped_);

  if (mapped_file_ == NULL) {
    return;
  }

  // unmap the previously mapped file
  if ((munmap(static_cast<void *>(mapped_file_), mapped_size_) != 0)
      || (close(file_descriptor_) != 0)) {
    LogCvmfs(kLogUtility, kLogStderr, "failed to unmap %s", file_path_.c_str());
    const bool munmap_failed = false;
    assert(munmap_failed);
  }

  // reset (resettable) data
  mapped_file_ = NULL;
  file_descriptor_ = -1;
  mapped_size_ = 0;
  mapped_ = false;
  LogCvmfs(kLogUtility, kLogVerboseMsg, "munmap'ed %s", file_path_.c_str());
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
