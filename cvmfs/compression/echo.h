/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_ECHO_H_
#define CVMFS_COMPRESSION_ECHO_H_

#include <errno.h>
#include <stdint.h>
#include <stdio.h>

#include <string>

#include "duplex_zlib.h"
#include "network/sink.h"
#include "util/plugin.h"

namespace zlib {

class EchoCompressor: public Compressor {
 public:
  explicit EchoCompressor(const Algorithms &alg);
  bool CompressStream(const bool flush,
                      unsigned char **inbuf, size_t *inbufsize,
                      unsigned char **outbuf, size_t *outbufsize);
  virtual size_t CompressUpperBound(const size_t bytes);
  virtual size_t DeflateBound(const size_t bytes) { return CompressUpperBound(bytes); }
  Compressor* Clone();
  static bool WillHandle(const zlib::Algorithms &alg);
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_ECHO_H_
