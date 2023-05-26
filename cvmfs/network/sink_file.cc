/**
 * This file is part of the CernVM File System.
 */

#include <cerrno>
#include <cstdio>
#include <string>

#include "sink_file.h"

namespace cvmfs {

/**
 * Appends data to the sink
 * 
 * @returns on success: number of bytes written 
 *          on failure: -errno.
 */
int64_t FileSink::Write(const void *buf, uint64_t sz) {
  fwrite(buf, 1ul, sz, file_);

  if (ferror(file_) != 0) {
    // ferror does not tell us what exactly the error is
    // and errno is also not set
    // so just return generic I/O error flag
    return -EIO;
  }

  return sz;
}

/**
 * Truncate all written data and start over at position zero.
 * 
 * @returns Success = 0
 *          Failure = -1
 */
int FileSink::Reset() {
  return ((fflush(file_) == 0) &&
          (ftruncate(fileno(file_), 0) == 0) &&
          (freopen(NULL, "w", file_) == file_)) ? 0 : -errno;
}

/**
 * Return a string representation of the sink
*/
std::string FileSink::Describe() {
  std::string result = "File sink with ";
  result += IsValid() ? " valid file pointer" : " invalid file pointer";
  return result;
}


}  // namespace cvmfs