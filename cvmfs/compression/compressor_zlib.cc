/**
 * This file is part of the CernVM File System.
 *
 */


#include "compressor_zlib.h"

#include <alloca.h>
#include <stdlib.h>
#include <sys/stat.h>

#include <algorithm>
#include <cassert>
#include <cstring>

#include "cvmfs_config.h"
#include "compression.h"
#include "crypto/hash.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT

namespace zlib {

bool ZlibCompressor::WillHandle(const zlib::Algorithms &alg) {
  return alg == kZlibDefault;
}


ZlibCompressor::ZlibCompressor(const Algorithms &alg)
  : Compressor(alg)
{
  stream_.zalloc   = Z_NULL;
  stream_.zfree    = Z_NULL;
  stream_.opaque   = Z_NULL;
  stream_.next_in  = Z_NULL;
  stream_.avail_in = 0;
  const int zlib_retval = deflateInit(&stream_, Z_DEFAULT_COMPRESSION);
  assert(zlib_retval == 0);
  is_healthy_ = true;
}


Compressor* ZlibCompressor::Clone() {
  ZlibCompressor* other = new ZlibCompressor(zlib::kZlibDefault);
  assert(stream_.avail_in == 0);
  // Delete the other stream
  int retcode = deflateEnd(&other->stream_);
  assert(retcode == Z_OK);
  retcode = deflateCopy(const_cast<z_streamp>(&other->stream_), &stream_);
  assert(retcode == Z_OK);
  return other;
}

bool ZlibCompressor::CompressStream(const bool flush,
                                   unsigned char **inbuf, size_t *inbufsize,
                                   unsigned char **outbuf, size_t *outbufsize) {
  // Adding compression
  stream_.avail_in = *inbufsize;
  stream_.next_in = *inbuf;
  const int flush_int = (flush) ? Z_FINISH : Z_NO_FLUSH;
  int retcode = 0;

  // TODO(jblomer) Figure out what exactly behaves differently with zlib 1.2.10
  // if ((*inbufsize == 0) && !flush)
  //   return true;

  stream_.avail_out = *outbufsize;
  stream_.next_out = *outbuf;

  // Deflate in zlib!
  retcode = deflate(&stream_, flush_int);
  assert(retcode == Z_OK || retcode == Z_STREAM_END);

  *outbufsize -= stream_.avail_out;
  *inbuf = stream_.next_in;
  *inbufsize = stream_.avail_in;

  return (flush_int == Z_NO_FLUSH && retcode == Z_OK && stream_.avail_in == 0)
         || (flush_int == Z_FINISH  && retcode == Z_STREAM_END);
}


ZlibCompressor::~ZlibCompressor() {
  int retcode = deflateEnd(&stream_);
  assert(retcode == Z_OK);
}


size_t ZlibCompressor::CompressUpperBound(const size_t bytes) {
  // Call zlib's deflate bound
  return deflateBound(&stream_, bytes);
}

StreamStates ZlibCompressor::CompressStream(InputAbstract *input,
                                            cvmfs::Sink *output) {
  if (!is_healthy_) {
    return kStreamError;
  }

  unsigned char out[kZChunk];
  int z_ret;
  int flush = Z_NO_FLUSH;

  do {
    if (!input->NextChunk()) {
      return kStreamIOError;
    }

    stream_.avail_in = input->chunk_size();
    if (!input->has_chunk_left()) {
      flush = Z_FINISH;
    }
    stream_.next_in = input->chunk();

    // Run deflate() on input until output buffer has no space left
    do {
      stream_.avail_out = kZChunk;
      stream_.next_out = out;

      z_ret = deflate(&stream_, flush);
      if (z_ret == Z_STREAM_ERROR) {
        deflateEnd(&stream_);
        is_healthy_ = false;
        return kStreamDataError;
      }
      size_t have = kZChunk - stream_.avail_out;
      int64_t written = output->Write(out, have);

      if (written != static_cast<int64_t>(have)) {
        deflateEnd(&stream_);
        is_healthy_ = false;
        return kStreamIOError;
      }
    } while (stream_.avail_out == 0);
  } while (flush != Z_FINISH);

  output->Flush();

  if (z_ret != Z_STREAM_END) {
    // here in original code "output" was reset and deleted
    deflateEnd(&stream_);
    is_healthy_ = false;
    return kStreamDataError;
  } else {
    (void)deflateReset(&stream_);
    return kStreamEnd;
  }
}

StreamStates ZlibCompressor::CompressStream(InputAbstract *input,
                                            cvmfs::Sink *output,
                                            shash::Any *compressed_hash) {
  if (!is_healthy_) {
    return kStreamError;
  }

  unsigned char out[kZChunk];
  int z_ret;
  int flush = Z_NO_FLUSH;

  shash::ContextPtr hash_context(compressed_hash->algorithm);
  hash_context.buffer = alloca(hash_context.size);
  shash::Init(hash_context);

  do {
    if (!input->NextChunk()) {
      return kStreamIOError;
    }

    stream_.avail_in = input->chunk_size();
    if (!input->has_chunk_left()) {
      flush = Z_FINISH;
    }
    stream_.next_in = input->chunk();

    // Run deflate() on input until output buffer has no space left
    do {
      stream_.avail_out = kZChunk;
      stream_.next_out = out;

      z_ret = deflate(&stream_, flush);
      if (z_ret == Z_STREAM_ERROR) {
        deflateEnd(&stream_);
        is_healthy_ = false;
        return kStreamDataError;
      }
      size_t have = kZChunk - stream_.avail_out;
      int64_t written = output->Write(out, have);

      if (written != static_cast<int64_t>(have)) {
        deflateEnd(&stream_);
        is_healthy_ = false;
        return kStreamIOError;
      }
      shash::Update(out, have, hash_context);
    } while (stream_.avail_out == 0);
  } while (flush != Z_FINISH);

  output->Flush();

  if (z_ret != Z_STREAM_END) {
    // here in original code "output" was reset and deleted
    deflateEnd(&stream_);
    is_healthy_ = false;
    return kStreamDataError;
  } else {
    shash::Final(hash_context, compressed_hash);
    (void)deflateReset(&stream_);
    return kStreamEnd;
  }
}

}  // namespace zlib
