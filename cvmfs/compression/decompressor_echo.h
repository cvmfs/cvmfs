/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_DECOMPRESSOR_ECHO_H_
#define CVMFS_COMPRESSION_DECOMPRESSOR_ECHO_H_

#include <string>

#include "decompression.h"

namespace zlib {

/**
 * EchoDecompressor is a decompressor that just plainly copies data
 * (like 'echo'). It that does not do any kind of compression.
 */
class EchoDecompressor: public Decompressor {
 public:
  explicit EchoDecompressor(const Algorithms &alg);

    /**
   * Compression function.
   * Takes a read-only data source, compresses the data and writes the result to
   * a given sink.
   *
   * Must be able to handle empty sources and just write the compression frame
   * where applicable.
   *
   * @return kStreamEnd if successful and compression stream finished
   *         kStreamContinue <not applicable for EchoDecompressor>
   *         StreamState Error value if failure
   */
  virtual StreamStates DecompressStream(InputAbstract *input,
                                      cvmfs::Sink *output);
  /**
   * Reset stream to perform decompression on a new, independent input
   */
  virtual bool Reset() { is_healthy_ = true; return true; }
  virtual Decompressor* Clone();
  virtual std::string Describe();
  static bool WillHandle(const zlib::Algorithms &alg);

 private:
  bool is_healthy_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_DECOMPRESSOR_ECHO_H_
