/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_COMPRESSOR_ECHO_H_
#define CVMFS_COMPRESSION_COMPRESSOR_ECHO_H_

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string>

#include "compression.h"
#include "duplex_zlib.h"
#include "network/sink.h"
#include "util/plugin.h"

namespace zlib {

/**
 * EchoCompressor is a compressor that just plainly copies data (like 'echo').
 * It that does not do any kind of compression.
 */
class EchoCompressor: public Compressor {
 public:
  explicit EchoCompressor(const Algorithms &alg);

  virtual StreamStates CompressStream(InputAbstract *input,
                                      cvmfs::Sink *output);
  virtual StreamStates CompressStream(InputAbstract *input,
                                      cvmfs::Sink *output,
                                      shash::Any *compressed_hash);
  bool CompressStream(const bool /*flush*/,
                      unsigned char **inbuf, size_t *inbufsize,
                      unsigned char **outbuf, size_t *outbufsize);
  virtual size_t CompressUpperBound(const size_t bytes);
  virtual size_t DeflateBound(const size_t bytes) {
                                             return CompressUpperBound(bytes); }
  Compressor* Clone();
  static bool WillHandle(const zlib::Algorithms &alg);

 private:
  bool is_healthy_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_COMPRESSOR_ECHO_H_
