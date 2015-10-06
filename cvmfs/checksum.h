
#ifndef __CHECKSUM_H_
#define __CHECKSUM_H_

#include <sys/types.h>
#include <stdint.h>

#include <string>

#define CHECKSUM_SUFFIX ".checksums"
#define CHECKSUM_HEADERSIZE 4
#define CHECKSUM_SIZE 4
#define CHECKSUM_BLOCKSIZE 4096

namespace hash {
  struct Any;
}

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
  ChecksumFileWriter(const hash::Any &hash, bool sumonly=false);
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

  const bool isGood() {return !error;}

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
namespace ChecksumFileReader {

  /*
   * Open a file for checksumming, based on the hash.
   *
   * Returns 0 on success and negative otherwise.
   */
int open(const hash::Any &hash);

  /*
   * Verify the contents of the buffer.
   *
   * If the buffer is not aligned to the 4K boundaries, then this
   * will perform a pread from the given file descriptor
   *
   * This will return 0 on success; negative otherwise.
   * 
   * Parameters:
   *  - fd: File descriptor to data file in cache.
   *  - fd2: File descriptor to checksum file.
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

