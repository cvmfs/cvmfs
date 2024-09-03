/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_COMPRESSOR_ZLIB_H_
#define CVMFS_COMPRESSION_COMPRESSOR_ZLIB_H_

#include "compression.h"
#include "duplex_zlib.h"

namespace zlib {

/**
 * Zlib wrapper for compression.
 */
class ZlibCompressor : public Compressor {
 public:
  explicit ZlibCompressor(const Algorithms &alg);
  ZlibCompressor(const ZlibCompressor &other);
  ~ZlibCompressor();

  virtual StreamStates CompressStream(InputAbstract *input,
                                      cvmfs::Sink *output);
  virtual StreamStates CompressStream(InputAbstract *input,
                                      cvmfs::Sink *output,
                                      shash::Any *compressed_hash);
  virtual bool CompressStream(const bool flush,
                                unsigned char **inbuf, size_t *inbufsize,
                                unsigned char **outbuf, size_t *outbufsize);
  virtual bool Reset();
  virtual size_t CompressUpperBound(const size_t bytes);
  Compressor* Clone();
  virtual std::string Describe();
  static bool WillHandle(const zlib::Algorithms &alg);

 private:
  z_stream stream_;
  bool is_healthy_;  // ZStream is healthy
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_COMPRESSOR_ZLIB_H_
