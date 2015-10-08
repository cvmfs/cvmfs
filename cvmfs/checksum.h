
#ifndef __CHECKSUM_H_
#define __CHECKSUM_H_

#include <sys/types.h>
#include <stdint.h>

#include <string>

#define CHECKSUM_SUFFIX ".checksums"
#define CHECKSUM_HEADERSIZE 4
#define CHECKSUM_SIZE 4
#define CHECKSUM_BLOCKSIZE 4096

namespace checksum {

/*
 * Write out a checksum file for a corresponding data stream.
 *
 * On object creation, this will open/truncate the given file, write a header,
 * and write out a series of checksums corresponding to the data stream.
 *
 * If write is called for a partial buffer, this is kept in memory.
 *
 * When the object is destroyed, any partial buffer contents are checksummed and
 * flushed out to disk.  We assume this is immutable once written.
 *
 * If an error occurs in the destructor, any subsequent validation will fail.
 *
 * At no time will invalid data be written to disk.  However, if the object is
 * not destroyed, partial data may be present on disk.
 *
 * NOTE - this is not transaction-safe or multi-writer safe.
 * This does not throw any non-standard exceptions.
 */
class ChecksumFileWriter {

public:
  /**
   * Given an open file descriptor and an eventual file size, the
   * ChecksumFileWriter will fallocate enough space to keep the per-block
   * checksums in the same file.
   *
   * @param fd: Open file descriptor to data file.
   * @param fsize: Eventual size of the data file.
   * @param sumonly: Do not write out checksum data; instead, keep a running
   * checksum-of-checksums.
   */
  ChecksumFileWriter(int fd, off_t fsize, bool sumonly=false);
  ~ChecksumFileWriter();

    /*
     * Calculate additional checksums.
     *
     * If this fails once, all subsequent calls will fail.
     *
     * Returns 0 on success; negative on failure.
     */
  int stream(const unsigned char * buff, size_t len);

    /*
     * Finalize the checksum calculation.
     *
     * After this is called, one cannot call stream() again.
     *
     * The return value is a checksum-of-checksums which can
     * be used to identify the entirity of the file contents.
     * 
     * Returns 0 on success; negative on failure.
     */
  int finalize(uint32_t &checksum);

  const bool isGood() {return !m_error;}

private:
  void checksum(const unsigned char * buf, size_t len);

  unsigned char m_partial_buffer[CHECKSUM_BLOCKSIZE];
  off_t m_size;

  /**
   * Number of checksum blocks that have been written this far.
   */
  off_t m_count;

  uint32_t m_running_sum;
  size_t m_buffer_offset;
  int m_fd;
  bool m_error;
  bool m_done;

};

/*
 * Verify the contents of a data stream
 */
class ChecksumFileReader {
public:

  /**
   * Given an open file descriptor to a data file, parse and read
   * the checksum contents.
   *
   * @param fd: Open file descriptor to data file.
   * @param fsize: Size of the data contents of file; checksum information
   *               is assumed to be immediately past the data.
   */
  ChecksumFileReader(int fd, off_t fsize);

  /**
   * Verify the contents of the buffer.
   *
   * If the buffer is not aligned to the 4K boundaries, then this
   * will perform a pread from the given file descriptor
   *
   * This will return 0 on success; negative otherwise.
   * 
   * Parameters:
   *  - buff: Data buffer to verify.
   *  - count: Size of data buffer.
   *  - off: Offset of data buffer in file.
   */
  int verify(const unsigned char * buff, size_t count, off_t off);

  bool isGood() {return !m_error;}

private:

  /**
   * Denotes an error has occurred; if set, no further computations
   * or writing will be performed.
   */
  bool m_error;

  /**
   * FD for the data file.
   */
  int m_fd;

  /**
   * Location of start of checksum data in file.
   */
  off_t m_size;
};

}

#endif

