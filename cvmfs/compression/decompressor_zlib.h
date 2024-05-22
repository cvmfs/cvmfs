/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_DECOMPRESSOR_ZLIB_H_
#define CVMFS_COMPRESSION_DECOMPRESSOR_ZLIB_H_

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string>

#include "decompression.h"
#include "duplex_zlib.h"
#include "network/sink.h"
#include "util/plugin.h"

namespace zlib {

/**
 * ZlibDecompressor is a decompressor that just plainly copies data
 * (like 'echo'). It that does not do any kind of compression.
 */
class ZlibDecompressor: public Decompressor {
 public:
  explicit ZlibDecompressor(const Algorithms &alg);
  ~ZlibDecompressor();

  virtual StreamStates DecompressStream(InputAbstract *input,
                                                           cvmfs::Sink *output);
  virtual bool Reset();
  Decompressor* Clone();
  static bool WillHandle(const zlib::Algorithms &alg);

 private:
  z_stream stream_;
  bool is_healthy_;
};

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_DECOMPRESSOR_ZLIB_H_
