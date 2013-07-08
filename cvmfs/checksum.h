
#ifndef __CHECKSUM_H_
#define __CHECKSUM_H_

#include <sys/types.h>

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
  ChecksumFileWriter(const hash::Any &hash);
  ~ChecksumFileWriter();

    /*
     * Calculate additional checksums.
     *
     * If this fails once, all subsequent calls will fail.
     *
     * Returns 0 on success; negative on failure.
     */
  int stream(const unsigned char * buff, size_t len);

  const bool isGood() {return !error;}

private:
  void checksum(const unsigned char * buf, size_t len);

  std::string filename;
  unsigned char partial_buffer[CHECKSUM_BLOCKSIZE];
  int buffer_offset;
  int fd;
  bool error;

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
int verify(int fd, int fd2, const unsigned char * buff, size_t count, off_t off);

}

#endif

