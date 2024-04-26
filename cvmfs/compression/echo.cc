/**
 * This file is part of the CernVM File System.
 *
 * This is a wrapper around zlib.  It provides
 * a set of functions to conveniently compress and decompress stuff.
 * Almost all of the functions return true on success, otherwise false.
 *
 * TODO: think about code deduplication
 */

#include "cvmfs_config.h"
#include "compression/compression.h"
#include "compression/echo.h"

#include <alloca.h>
#include <stdlib.h>
#include <sys/stat.h>

#include <algorithm>
#include <cassert>
#include <cstring>

#include "crypto/hash.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

namespace zlib {

EchoCompressor::EchoCompressor(const zlib::Algorithms &alg):
                                                             Compressor(alg) { }


bool EchoCompressor::WillHandle(const zlib::Algorithms &alg) {
  return alg == kNoCompression;
}


Compressor* EchoCompressor::Clone() {
  return new EchoCompressor(zlib::kNoCompression);
}


bool EchoCompressor::CompressStream(
  const bool flush,
  unsigned char **inbuf, size_t *inbufsize,
  unsigned char **outbuf, size_t *outbufsize)
{
  size_t bytes_to_copy = min(*outbufsize, *inbufsize);
  memcpy(*outbuf, *inbuf, bytes_to_copy);
  const bool done = (bytes_to_copy == *inbufsize);

  // Update the return variables
  *inbuf += bytes_to_copy;
  *outbufsize = bytes_to_copy;
  *inbufsize -= bytes_to_copy;

  return done;
}


size_t EchoCompressor::CompressUpperBound(const size_t bytes) {
  // zero bytes as an upper bound is no good because some callers want to
  // allocate buffers according to this value
  return (bytes == 0) ? 1 : bytes;
}

}  // namespace zlib
