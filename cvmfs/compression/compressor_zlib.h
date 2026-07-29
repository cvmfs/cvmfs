/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_COMPRESSOR_ZLIB_H_
#define CVMFS_COMPRESSION_COMPRESSOR_ZLIB_H_

#include <string>

#include "compressor.h"
#include "duplex_zlib.h"

namespace zip {

/**
 * Zlib wrapper for compression.
 */
class ZlibCompressor : public Compressor {
 public:
  explicit ZlibCompressor(const Algorithms &alg);
  ZlibCompressor(const ZlibCompressor &other);
  ZlibCompressor();
  ~ZlibCompressor();

  void Init();
  virtual StreamStates Compress(InputAbstract *input, cvmfs::Sink *output);
  virtual StreamStates Compress(InputAbstract *input, cvmfs::Sink *output,
                                shash::Any *compressed_hash);
  virtual StreamStates StreamingStep(InputAbstract* input,
                                     cvmfs::MemSink* output, const bool flush);
  virtual bool Reset();
  virtual size_t CompressUpperBound(const size_t bytes);
  Compressor* Clone();
  virtual std::string Describe();
  static bool WillHandle(const zip::Algorithms &alg);

 private:
  z_stream stream_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_COMPRESSOR_ZLIB_H_
