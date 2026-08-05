/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_DECOMPRESSOR_ZLIB_H_
#define CVMFS_COMPRESSION_DECOMPRESSOR_ZLIB_H_

#include <string>

#include "decompressor.h"
#include "duplex_zlib.h"

namespace zip {

/**
 * ZlibDecompressor is a decompressor that just plainly copies data
 * (like 'echo'). It that does not do any kind of compression.
 */
class ZlibDecompressor: public Decompressor {
 public:
  explicit ZlibDecompressor(const Algorithms &alg);
  ~ZlibDecompressor();

  /**
   * Compression function.
   * Takes a read-only data source, compresses the data and writes the result to
   * a given sink.
   *
   * Must be able to handle empty sources and just write the compression frame
   * where applicable.
   *
   * @return kStreamEnd if successful and compression stream finished
   *         kStreamContinue if successful and compression stream is unfinished
   *         StreamState Error value if failure
   */
  virtual StreamStates DecompressStream(InputAbstract *input,
                                                           cvmfs::Sink *output);
  /**
   * Reset stream to perform decompression on a new, independent input
   */
  virtual bool Reset();
  Decompressor* Clone();
  virtual std::string Describe();
  static bool WillHandle(const zip::Algorithms &alg);

 private:
  z_stream stream_;
  bool is_healthy_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_DECOMPRESSOR_ZLIB_H_
