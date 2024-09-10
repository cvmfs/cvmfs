/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_COMPRESSOR_ECHO_H_
#define CVMFS_COMPRESSION_COMPRESSOR_ECHO_H_

#include <string>
#include "compression.h"

namespace zlib {

/**
 * EchoCompressor is a compressor that just plainly copies data (like 'echo').
 * It that does not do any kind of compression.
 */
class EchoCompressor: public Compressor {
 public:
  explicit EchoCompressor(const Algorithms &alg);

  virtual StreamStates Compress(InputAbstract *input, cvmfs::Sink *output);
  virtual StreamStates Compress(InputAbstract *input, cvmfs::Sink *output,
                                shash::Any *compressed_hash);
  virtual StreamStates CompressStream(InputAbstract *input,
                                cvmfs::MemSink *output, const bool flush)
                                { return kStreamError; };
  virtual size_t CompressUpperBound(const size_t bytes);
  virtual bool Reset() { is_healthy_ = true; return true; }
  Compressor* Clone();
  virtual std::string Describe();

  static bool WillHandle(const zlib::Algorithms &alg);

 private:
  bool is_healthy_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_COMPRESSOR_ECHO_H_
