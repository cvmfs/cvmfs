/**
 * This file is part of the CernVM File System.
 */

#include "sink_file.h"

#include <cerrno>
#include <cstdio>
#include <string>
#include <unistd.h>

namespace cvmfs {

/**
 * Appends data to the sink
 *
 * @returns on success: number of bytes written
 *          on failure: -errno.
 */
int64_t FileSink::Write(const void *buf, uint64_t sz) {
  const size_t ret = fwrite(buf, 1ul, sz, file_);

  if (ferror(file_) != 0) {
    // ferror does not tell us what exactly the error is
    // and errno is also not set
    // so just return generic I/O error flag
    return -EIO;
  }

  return static_cast<int64_t>(ret);
}

/**
 * Truncate all written data and start over at position zero.
 *
 * @returns Success = 0
 *          Failure = -1
 */
int FileSink::Reset() {
  return ((fflush(file_) == 0) && (ftruncate(fileno(file_), 0) == 0)
          && (freopen(NULL, "w", file_) == file_))
             ? 0
             : -errno;
}

/**
 * Purges all resources leaving the sink in an invalid state.
 * More aggressive version of Reset().
 * For some sinks and depending on owner status it might do
 * the same as Reset().
 *
 * @returns Success = 0
 *          Failure = -errno
 */
int FileSink::Purge() {
  if (is_owner_ && file_) {
    const int ret = fclose(file_);
    file_ = NULL;
    if (ret != 0) {
      return -errno;
    }

    return 0;
  } else {
    return Reset();
  }
}

/**
 * Return a string representation describing the type of sink and its status
 */
std::string FileSink::Describe() {
  std::string result = "File sink with ";
  result += IsValid() ? " valid file pointer" : " invalid file pointer";
  return result;
}
void FileSink::Adopt(FILE *file, bool is_owner) {
  if (is_owner_ && file_) {
    (void)fclose(file_);
  }

  is_owner_ = is_owner;
  file_ = file;
}
}  // namespace cvmfs
