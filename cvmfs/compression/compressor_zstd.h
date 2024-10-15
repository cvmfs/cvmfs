/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_COMPRESSOR_ZSTD_H_
#define CVMFS_COMPRESSION_COMPRESSOR_ZSTD_H_

#include <zstd.h>

#include <string>

#include "compression.h"

namespace zlib {

/**
 * Zlib wrapper for compression.
 */
class ZstdCompressor : public Compressor {
 public:
  explicit ZstdCompressor(const Algorithms &alg);
  ZstdCompressor(const ZstdCompressor &other);
  ~ZstdCompressor();

  virtual StreamStates Compress(InputAbstract *input, cvmfs::Sink *output);
  virtual StreamStates Compress(InputAbstract *input, cvmfs::Sink *output,
                                shash::Any *compressed_hash);
  virtual StreamStates CompressStream(InputAbstract *input,
                                      cvmfs::MemSink *output, const bool flush);
  bool CompressStreamOld(const bool flush,
                      unsigned char **inbuf, size_t *inbufsize,
                      unsigned char **outbuf, size_t *outbufsize);
  virtual bool Reset();
  virtual size_t CompressUpperBound(const size_t bytes);
  Compressor* Clone();
  virtual std::string Describe();
  static bool WillHandle(const zlib::Algorithms &alg);

 private:
  ZSTD_CCtx *stream_;
  bool is_healthy_;  // ZStream is healthy
  size_t zstd_chunk_;
  bool compress_stream_outbuf_full_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_COMPRESSOR_ZSTD_H_
