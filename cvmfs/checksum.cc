
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

using namespace checksum;


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
pwrite_full(int fd, void *in_buf, size_t len, off_t offset) {
  if (len == 0) return 0;

  char *buf = static_cast<char *>(in_buf);
  ssize_t counter = 0;
  ssize_t res;
  errno = 0;
  while (((res = pwrite(fd, buf, len, offset)) >= 0) && (errno != EINTR)) {
    if (res > 0) {
      buf += res;
      len -= res;
      counter += res;
      offset += res;
    }
    if (len == 0) {return counter;}
  }
  if (res < 0) return counter;
  return counter;
}


ChecksumFileWriter::ChecksumFileWriter(int fd, off_t fsize, bool sumonly)
  : m_size(fsize), m_running_sum(CRC_INITIAL_VAL), m_buffer_offset(0), m_fd(fd),
    m_error(true), m_done(false)
{
  memset(m_partial_buffer, '\0', CHECKSUM_BLOCKSIZE);
  char header[CHECKSUM_HEADERSIZE];
  memset(header, '\0', CHECKSUM_HEADERSIZE);
  header[0] = '\1';
  if (sumonly) {
    if (!calculate_crc(reinterpret_cast<unsigned char *>(header),
        CHECKSUM_HEADERSIZE, &m_running_sum, CRC32C_POLYNOMIAL)) {
      m_error = false;
    }
    return;
  }
  if (m_fd < 0) {
    LogCvmfs(kLogChecksum, kLogDebug, "Checksum file not open.");
    return;
  }
  off_t block_size = (m_size / CHECKSUM_BLOCKSIZE + 1) * sizeof(uint32_t);
  int retval;
  if ((retval = posix_fallocate(m_fd, m_size, CHECKSUM_HEADERSIZE + block_size + 1))) {
    LogCvmfs(kLogChecksum, kLogDebug, "Could not pre-allocate checksum data: "
             "%s (errno=%d)", strerror(errno), errno);
    return;
  }
  if (pwrite_full(m_fd, header, CHECKSUM_HEADERSIZE, m_size) != CHECKSUM_HEADERSIZE) {
    LogCvmfs(kLogChecksum, kLogDebug, "Could not write checksum header to "
             "file: %s (errno=%d)", strerror(errno), errno);
    return;
  }
  if (!fsize) {m_done=true;}
  m_error = false;
}


int
ChecksumFileWriter::finalize(uint32_t &hash) {
  if (unlikely(m_error)) {return -1;}

  if (m_buffer_offset) {
    memset(m_partial_buffer + m_buffer_offset, '\0',
           CHECKSUM_BLOCKSIZE - m_buffer_offset);
    checksum(m_partial_buffer, CHECKSUM_BLOCKSIZE);
  }
  if (m_error) {return -1;}
  hash = m_running_sum;
  return 0;
}


ChecksumFileWriter::~ChecksumFileWriter() {
  uint32_t hash;
  finalize(hash);
}


// Note -- len here is assumed to be aligned to CHECKSUM_BLOCKSIZE by stream
// below.
void
ChecksumFileWriter::checksum(const uint8_t * buf, size_t len, off_t off) {

  if (unlikely(m_error)) {return;}

  ssize_t c_count = len / CHECKSUM_BLOCKSIZE;
  std::vector<uint32_t> sums; sums.reserve(c_count);
  if (bulk_calculate_crc(buf, len,
                    &sums[0], CRC32C_POLYNOMIAL,
                    CHECKSUM_BLOCKSIZE)) {
    LogCvmfs(kLogChecksum, kLogDebug, "Could not calculate checksum for file");
    m_error = true;
  }
  if (calculate_crc(reinterpret_cast<const unsigned char *>(&sums[0]),
      c_count, &m_running_sum, CRC32C_POLYNOMIAL)) {
    m_error = true;
  }
  off_t off2 = m_size + CHECKSUM_HEADERSIZE +
               (off / CHECKSUM_BLOCKSIZE)*sizeof(uint32_t);
  if (m_fd >= 0 && pwrite_full(m_fd, &sums[0], c_count*CHECKSUM_SIZE, off2) !=
                   c_count*CHECKSUM_SIZE)
  {
    LogCvmfs(kLogChecksum, kLogDebug, "Could not write to checksum file: %s "
             "(errno=%d)", strerror(errno), errno);
    m_error = true;
  }
}


int
ChecksumFileWriter::stream(const unsigned char *buf, size_t len) {

  if (unlikely(m_error)) {return -1;}

  if (m_buffer_offset) {
    int to_copy = CHECKSUM_BLOCKSIZE - m_buffer_offset;
    if (static_cast<size_t>(to_copy) > len) {to_copy = len;}

    memcpy(m_partial_buffer + m_buffer_offset, buf, to_copy);
    m_buffer_offset += to_copy;
    buf += to_copy;
    len -= to_copy;

    if (m_buffer_offset == CHECKSUM_BLOCKSIZE) {
      checksum(m_partial_buffer, CHECKSUM_BLOCKSIZE);
      m_buffer_offset = 0;
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
    memcpy(m_partial_buffer, buf, len);
    m_buffer_offset = len;
  }
  return !m_error ? 0 : -1;
}


ChecksumFileReader::ChecksumFileReader(int fd, off_t fsize)
  : m_error(true), m_fd(fd), m_size(fsize)
{
  char header[CHECKSUM_HEADERSIZE];
  if (pread_full(m_fd, header, CHECKSUM_HEADERSIZE, m_size) == 
      CHECKSUM_HEADERSIZE)
  {
    if ((header[0] == '\1') && (header[1] == '\0') && (header[2] == '\0') &&
        (header[3] == '\0'))
    {
      m_error = false;
    } else {
      LogCvmfs(kLogChecksum, kLogDebug, "Invalid checksum header");
    }
  } else {
    LogCvmfs(kLogChecksum, kLogDebug, "Could not read checksum file: %s "
             "(errno=%d)", strerror(errno), errno);
  }
}


int
ChecksumFileReader::verify(const unsigned char *buf, size_t len, off_t off) {

  if (len == (size_t)-1) {
    return errno ? -errno : -EIO;
  }

  crc32_error_t error_info;

  uint8_t data_buf[CHECKSUM_BLOCKSIZE];
  memset(data_buf, '\0', CHECKSUM_BLOCKSIZE);

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
  off_t sum_byte_offset = m_size +
                          (cfd_begin/CHECKSUM_BLOCKSIZE)*CHECKSUM_SIZE +
                          CHECKSUM_HEADERSIZE;
  if (pread_full(m_fd, sums_ptr, sum_byte_count, sum_byte_offset) != 
      sum_byte_count) 
  {
    LogCvmfs(kLogChecksum, kLogDebug, "Failed to read checksum file");
    return (errno ? -errno : -EIO);
  }

  // Useful dev debugging lines
  /*LogCvmfs(kLogChecksum, kLogDebug, "First checksum %d", sums[0]);
  LogCvmfs(kLogChecksum, kLogDebug, "Beginning block %d; end block %d.",
           cfd_begin, cfd_end);
  LogCvmfs(kLogChecksum, kLogDebug, "Offset: %d; len %d.", off, len);
  LogCvmfs(kLogChecksum, kLogDebug, "Sum bytecount: %d; sum offset: %d",
           sum_byte_count, sum_byte_offset);*/

  // Do a check on a partial beginning buffer.
  if (off != cfd_begin) {  // FUSE ought to align our accesses... libcvmfs may not!
    if (pread_full(m_fd, data_buf, CHECKSUM_BLOCKSIZE, cfd_begin) < 0) {
      LogCvmfs(kLogChecksum, kLogDebug, "Failed to read data file for "
               "beginning buffer");
      return (errno ? -errno : -EIO);
    }
    if (bulk_verify_crc(data_buf, CHECKSUM_BLOCKSIZE,
        sums_ptr, CRC32C_POLYNOMIAL,
        CHECKSUM_BLOCKSIZE,
        &error_info)) {
      LogCvmfs(kLogChecksum, kLogDebug, "Checksum failed verification for "
               "beginning buffer - got %d; expected %d; at data block "
               "starting at %d", error_info.got_crc, error_info.expected_crc,
               error_info.bad_data - data_buf);
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
    if ((res = pread_full(m_fd, data_buf + leftover_bytes, left_to_read, end))
        < 0)
    {
      LogCvmfs(kLogChecksum, kLogDebug, "Failed to read data file for ending"
               " buffer");
      return (errno ? -errno : -EIO);
    }
    //LogCvmfs(kLogChecksum, kLogDebug, "Read %d bytes starting at %d", res,
    //         end);
    //LogCvmfs(kLogChecksum, kLogDebug, "Checksum size - %d", sum_count-1);
    if (bulk_verify_crc(data_buf, CHECKSUM_BLOCKSIZE,
                        sums_ptr + (sum_count - 1), CRC32C_POLYNOMIAL,
                        CHECKSUM_BLOCKSIZE, &error_info))
    {
      LogCvmfs(kLogChecksum, kLogDebug, "Checksum failed for ending buffer -"
               " got %d; expected %d; at data block starting at %d",
               error_info.got_crc, error_info.expected_crc,
               error_info.bad_data - data_buf);
      return -EIO;
    }
    len -= CHECKSUM_BLOCKSIZE - (cfd_end - end);
    cfd_end -= CHECKSUM_BLOCKSIZE;
  }
  if (cfd_begin == cfd_end) {return 0;}

  // Bulk verify remainder.
  if (bulk_verify_crc(reinterpret_cast<const uint8_t *>(buf), len,
                      sums_ptr + (read_beginning ? 1 : 0),
                     CRC32C_POLYNOMIAL, CHECKSUM_BLOCKSIZE, &error_info))
  {
    LogCvmfs(kLogChecksum, kLogDebug, "Bulk checksum failed verification; got"
             " %d, expected %d at data block starting at %d",
             error_info.got_crc, error_info.expected_crc,
             error_info.bad_data - buf);
    return -EIO;
  }

  return 0;
}

