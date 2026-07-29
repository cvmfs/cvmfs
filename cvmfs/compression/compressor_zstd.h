/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_COMPRESSOR_ZSTD_H_
#define CVMFS_COMPRESSION_COMPRESSOR_ZSTD_H_

#include <zstd.h>

#include <string>

#include "compressor.h"

namespace zip {

/**
 * Zlib wrapper for compression.
 */
class ZstdCompressor : public Compressor {
 public:
  explicit ZstdCompressor(const Algorithms &alg);
  explicit ZstdCompressor();
  ZstdCompressor(const ZstdCompressor &other);
  ~ZstdCompressor();

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
  ZSTD_CCtx *stream_;
  bool compress_stream_outbuf_full_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_COMPRESSOR_ZSTD_H_
