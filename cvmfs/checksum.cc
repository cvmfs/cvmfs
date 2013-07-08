
#include "checksum.h"

#include "cache.h"
#include "bulk_crc32.h"
#include "logging.h"

#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <stdint.h>

#include <vector>

#define likely(x)    __builtin_expect (!!(x), 1)
#define unlikely(x)  __builtin_expect (!!(x), 0)

namespace cache {
std::string
__attribute__((weak))
GetPathInCache(const hash::Any &) {return "";}
}

static int
pread_full(int fd, void *in_buf, size_t len, off_t off) {
  if (len == 0) return 0;

  char *buf = static_cast<char *>(in_buf);
  ssize_t counter = 0;
  ssize_t res;
  errno = 0;
  while (((res = pread(fd, buf, len, off)) > 0) && (errno != EINTR)) {
    if (res > 0) {
      buf += res;
      len -= res;
      off += res;
      counter += res;
    }
    if (len == 0) {return counter;}
  }
  if (res < 0) return counter;
  return counter;
}

static int
write_full(int fd, void *in_buf, size_t len) {
  if (len == 0) return 0;

  char *buf = static_cast<char *>(in_buf);
  ssize_t counter = 0;
  ssize_t res;
  errno = 0;
  while (((res = write(fd, buf, len)) >= 0) && (errno != EINTR)) {
    if (res > 0) {
      buf += res;
      len -= res;
      counter += res;
    }
    if (len == 0) {return counter;}
  }
  if (res < 0) return counter;
  return counter;
}

ChecksumFileWriter::ChecksumFileWriter(const hash::Any &hash)
  : filename(cache::GetPathInCache(hash) + CHECKSUM_SUFFIX), buffer_offset(0), fd(-1), error(true)
{
  memset(partial_buffer, '\0', CHECKSUM_BLOCKSIZE);
  fd = open(filename.c_str(), O_RDWR|O_TRUNC|O_CREAT);
  if (fd < 0) {
    LogCvmfs(kLogChecksum, kLogDebug, "could not open checksum file %s (errno=%d, %s)",
             filename.c_str(), errno, strerror(errno));
  }
  else {
    char header[CHECKSUM_HEADERSIZE];
    memset(header, '\0', CHECKSUM_HEADERSIZE);
    header[0] = '\1';
    if (write_full(fd, header, CHECKSUM_HEADERSIZE) != CHECKSUM_HEADERSIZE) {
      LogCvmfs(kLogChecksum, kLogDebug, "could not write header to checksum file %s (errno=%d, %s)",
               filename.c_str(), errno, strerror(errno));
    } else {
      error = false;
    }
  }
}

ChecksumFileWriter::~ChecksumFileWriter() {
  if (buffer_offset) {
    //LogCvmfs(kLogChecksum, kLogDebug, "checksumming %d bytes on close", buffer_offset);
    memset(partial_buffer+buffer_offset, '\0', CHECKSUM_BLOCKSIZE - buffer_offset);
    checksum(partial_buffer, CHECKSUM_BLOCKSIZE);
  }
  close(fd);
}

// Note -- len here is assumed to be aligned to CHECKSUM_BLOCKSIZE by stream below.
void
ChecksumFileWriter::checksum(const uint8_t * buf, size_t len) {

  if (unlikely(error)) {return;}

  ssize_t c_count = len / CHECKSUM_BLOCKSIZE;
  std::vector<uint32_t> sums; sums.reserve(c_count);
  if (bulk_calculate_crc(buf, len,
                    &sums[0], CRC32C_POLYNOMIAL,
                    CHECKSUM_BLOCKSIZE)) {
    LogCvmfs(kLogChecksum, kLogDebug, "could not calculate checksum for %s", filename.c_str());
    error = true;
  }
  if (write_full(fd, &sums[0], c_count*CHECKSUM_SIZE) != c_count*CHECKSUM_SIZE) {
    LogCvmfs(kLogChecksum, kLogDebug, "could not write to checksum file %s (errno=%d, %s)", filename.c_str(), errno, strerror(errno));
    error = true;
  }
}

int
ChecksumFileWriter::stream(const unsigned char *buf, size_t len) {

  if (unlikely(error)) {return -1;}

  if (buffer_offset) {
    int to_copy = CHECKSUM_BLOCKSIZE - buffer_offset;
    if (static_cast<size_t>(to_copy) > len) {to_copy = len;}

    memcpy(partial_buffer+buffer_offset, buf, to_copy);
    buffer_offset += to_copy;
    buf += to_copy;
    len -= to_copy;

    if (buffer_offset == CHECKSUM_BLOCKSIZE) {
      checksum(partial_buffer, CHECKSUM_BLOCKSIZE);
      buffer_offset = 0;
    }
    // ELSE: we copied buf completely into partial_buffer and
    // len = 0
  }
  size_t data_len = (len / CHECKSUM_BLOCKSIZE) * CHECKSUM_BLOCKSIZE;
  if (data_len) {
    checksum(buf, data_len);
    len -= data_len;
  }
  if (len) {
    buf += data_len;
    memcpy(partial_buffer, buf, len);
    buffer_offset = len;
  }
  return !error ? 0 : -1;
}

int
ChecksumFileReader::open(const hash::Any &hash)
{
  std::string filename = cache::GetPathInCache(hash)+CHECKSUM_SUFFIX;
  int cfd;
  if ((cfd = ::open(filename.c_str(), O_RDONLY)) >= 0) {
    char header[CHECKSUM_HEADERSIZE];
    if (pread_full(cfd, header, CHECKSUM_HEADERSIZE, 0) == CHECKSUM_HEADERSIZE) {
      if ((header[0] == '\1') && (header[1] == '\0') && (header[2] == '\0') && (header[3] == '\0')) {
        return cfd;
      } else {
        LogCvmfs(kLogChecksum, kLogDebug, "invalid checksum header for %s", filename.c_str());
      }
    } else {
      LogCvmfs(kLogChecksum, kLogDebug, "could not read checksum file %s (errno=%d, %s)",
               filename.c_str(), errno, strerror(errno));
    }
    close(cfd);
  } else {
    LogCvmfs(kLogChecksum, kLogDebug, "could not open checksum file %s (errno=%d, %s)",
             filename.c_str(), errno, strerror(errno));
  }
  return -EIO;
}

int
ChecksumFileReader::verify(int fd, int cfd, const unsigned char *buf, size_t len, off_t off) {

  if (len == (size_t)-1) {return errno ? -errno : -EIO;}

  crc32_error_t error_info;

  uint8_t data_buf[CHECKSUM_BLOCKSIZE]; memset(data_buf, '\0', CHECKSUM_BLOCKSIZE);

  off_t cfd_begin = (off / CHECKSUM_BLOCKSIZE) * CHECKSUM_BLOCKSIZE;
  off_t cfd_end = ((off+len) % CHECKSUM_BLOCKSIZE == 0) ?
    (off+len) :
   (((off+len) / CHECKSUM_BLOCKSIZE + 1) * CHECKSUM_BLOCKSIZE);

  size_t sum_count = (cfd_end - cfd_begin) / CHECKSUM_BLOCKSIZE;
  ssize_t sum_byte_count = sum_count * CHECKSUM_SIZE;
  std::vector<uint32_t> sums; sums.reserve(sum_count);
  uint32_t *sums_ptr = &sums[0];
  bool read_beginning = false;

  // Read checks from file
  off_t sum_byte_offset = (cfd_begin/CHECKSUM_BLOCKSIZE)*CHECKSUM_SIZE + CHECKSUM_HEADERSIZE;
  if (pread_full(cfd, sums_ptr, sum_byte_count, sum_byte_offset) != sum_byte_count) {
    LogCvmfs(kLogChecksum, kLogDebug, "Failed to read checksum file");
    return (errno ? -errno : -EIO);
  }

  // Useful dev debugging lines
  /*LogCvmfs(kLogChecksum, kLogDebug, "First checksum %d", sums[0]);
  LogCvmfs(kLogChecksum, kLogDebug, "Beginning block %d; end block %d.", cfd_begin, cfd_end);
  LogCvmfs(kLogChecksum, kLogDebug, "Offset: %d; len %d.", off, len);
  LogCvmfs(kLogChecksum, kLogDebug, "Sum bytecount: %d; sum offset: %d", sum_byte_count, sum_byte_offset);*/

  // Do a check on a partial beginning buffer.
  if (off != cfd_begin) { // FUSE ought to align our accesses... libcvmfs may not!
    if (pread_full(fd, data_buf, CHECKSUM_BLOCKSIZE, cfd_begin) < 0) {
      LogCvmfs(kLogChecksum, kLogDebug, "Failed to read data file for beginning buffer");
      return (errno ? -errno : -EIO);
    }
    if (bulk_verify_crc(data_buf, CHECKSUM_BLOCKSIZE,
        sums_ptr, CRC32C_POLYNOMIAL,
        CHECKSUM_BLOCKSIZE,
        &error_info)) {
      LogCvmfs(kLogChecksum, kLogDebug, "Checksum failed verification for beginning buffer - got %d; expected %d; at data block starting at %d", error_info.got_crc, error_info.expected_crc, error_info.bad_data - data_buf);
      return -EIO;
    }
    buf += CHECKSUM_BLOCKSIZE - (off - cfd_begin);
    off += CHECKSUM_BLOCKSIZE - (off - cfd_begin);
    len -= CHECKSUM_BLOCKSIZE - (off - cfd_begin);
    cfd_begin += CHECKSUM_BLOCKSIZE;
    read_beginning = true;
  }
  if (cfd_begin >= cfd_end) {return 0;}

  // Do a check on a partial end buffer.
  off_t end = off+len;
  //LogCvmfs(kLogChecksum, kLogDebug, "End byte: %d", end);
  if (end < cfd_end) {
    memset(data_buf, '\0', CHECKSUM_BLOCKSIZE);
    size_t left_to_read = cfd_end - end; 
    size_t leftover_bytes = CHECKSUM_BLOCKSIZE - left_to_read;
    memcpy(data_buf, buf + len - leftover_bytes, leftover_bytes);
    int res;
    if ((res = pread_full(fd, data_buf + leftover_bytes, left_to_read, end)) < 0) {
      LogCvmfs(kLogChecksum, kLogDebug, "Failed to read data file for ending buffer");
      return (errno ? -errno : -EIO);
    }
    //LogCvmfs(kLogChecksum, kLogDebug, "Read %d bytes starting at %d", res, end);
    //LogCvmfs(kLogChecksum, kLogDebug, "Checksum size - %d", sum_count-1);
    if (bulk_verify_crc(data_buf, CHECKSUM_BLOCKSIZE, sums_ptr + (sum_count-1), CRC32C_POLYNOMIAL, CHECKSUM_BLOCKSIZE, &error_info)) {
      LogCvmfs(kLogChecksum, kLogDebug, "Checksum failed for ending buffer - got %d; expected %d; at data block starting at %d", error_info.got_crc, error_info.expected_crc, error_info.bad_data - data_buf);
      return -EIO;
    }
    len -= CHECKSUM_BLOCKSIZE - (cfd_end - end);
    cfd_end -= CHECKSUM_BLOCKSIZE;
  }
  if (cfd_begin == cfd_end) {return 0;}

  // Bulk verify remainder.
  if (bulk_verify_crc(reinterpret_cast<const uint8_t *>(buf), len, sums_ptr + (read_beginning ? 1 : 0), CRC32C_POLYNOMIAL,CHECKSUM_BLOCKSIZE, &error_info)) {
    LogCvmfs(kLogChecksum, kLogDebug, "Bulk checksum failed verification; got %d, expected %d at data block starting at %d", error_info.got_crc, error_info.expected_crc, error_info.bad_data - buf);
    return -EIO;
  }

  return 0;
}
