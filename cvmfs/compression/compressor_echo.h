/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_COMPRESSOR_ECHO_H_
#define CVMFS_COMPRESSION_COMPRESSOR_ECHO_H_

#include <string>
#include "compressor.h"

namespace zip {

/**
 * EchoCompressor is a compressor that just plainly copies data (like 'echo').
 * It that does not do any kind of compression.
 */
class EchoCompressor: public Compressor {
 public:
  explicit EchoCompressor(const Algorithms &alg);
  explicit EchoCompressor();

  virtual StreamStates Compress(InputAbstract *input, cvmfs::Sink *output);
  virtual StreamStates Compress(InputAbstract *input, cvmfs::Sink *output,
                                shash::Any *compressed_hash);
  virtual StreamStates StreamingStep(InputAbstract* input,
                                     cvmfs::MemSink* output, const bool flush);
  virtual size_t CompressUpperBound(const size_t bytes);
  virtual bool Reset()
                      { is_healthy_ = true; output_full_ = false; return true; }
  Compressor* Clone();
  virtual std::string Describe();

  static bool WillHandle(const zip::Algorithms &alg);

 private:
  bool output_full_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_COMPRESSOR_ECHO_H_
