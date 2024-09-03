/**
 * This file is part of the CernVM File System.
 */

#include "decompressor_zlib.h"

#include <cassert>

#include "decompression.h"

namespace zlib {

ZlibDecompressor::ZlibDecompressor(const zlib::Algorithms &alg) :
                                                             Decompressor(alg) {
  stream_.zalloc   = Z_NULL;
  stream_.zfree    = Z_NULL;
  stream_.opaque   = Z_NULL;
  stream_.next_in  = Z_NULL;
  stream_.avail_in = 0;
  int retval = inflateInit(&stream_);
  assert(retval == 0);

  is_healthy_ = true;
}

ZlibDecompressor::~ZlibDecompressor() {
  const int retcode = inflateEnd(&stream_);
  assert(retcode == Z_OK);
}

bool ZlibDecompressor::WillHandle(const zlib::Algorithms &alg) {
  return alg == kZlibDefault;
}


Decompressor* ZlibDecompressor::Clone() {
  ZlibDecompressor* other = new ZlibDecompressor(zlib::kZlibDefault);
  assert(stream_.avail_in == 0);
  // Delete the other stream
  int retcode = inflateEnd(&other->stream_);
  assert(retcode == Z_OK);
  retcode = inflateCopy(const_cast<z_streamp>(&other->stream_), &stream_);
  assert(retcode == Z_OK);
  return other;
}

StreamStates ZlibDecompressor::DecompressStream(InputAbstract *input,
                                            cvmfs::Sink *output) {
  if (!is_healthy_) {
    return kStreamError;
  }

  unsigned char out[kZChunk];
  int z_ret;

  do {
    if (!input->NextChunk()) {
      return kStreamIOError;
    }

    stream_.avail_in = input->chunk_size();
    stream_.next_in = input->chunk();

    // Run deflate() on input until output buffer has no space left
    do {
      stream_.avail_out = kZChunk;
      stream_.next_out = out;

      z_ret = inflate(&stream_, Z_NO_FLUSH);
      switch (z_ret) {
        case Z_NEED_DICT:
          z_ret = Z_DATA_ERROR;  // and fall through
        case Z_STREAM_ERROR:
        case Z_DATA_ERROR:
          is_healthy_ = false;
          return kStreamDataError;
        case Z_MEM_ERROR:
          is_healthy_ = false;
          return kStreamIOError;
      }
      const size_t have = kZChunk - stream_.avail_out;
      const int64_t written = output->Write(out, have);

      if ((written < 0) || written != static_cast<int64_t>(have)) {
        is_healthy_ = false;
        return kStreamIOError;
      }
    } while (stream_.avail_out == 0);
  } while (input->has_chunk_left());

  output->Flush();

  if (z_ret == Z_STREAM_END) {
    inflateReset(&stream_);
    return kStreamEnd;
  } else {
    return kStreamContinue;
  }
}

bool ZlibDecompressor::Reset() {
  if (inflateReset(&stream_) == Z_OK) {
    is_healthy_ = true;
    return true;
  } else {
    is_healthy_ = false;
    return false;
  }
}

}  // namespace zlib
