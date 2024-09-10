/**
 * This file is part of the CernVM File System.
 *
 */

#include "compressor_zlib.h"

#include <alloca.h>
#include <stdlib.h>

#include <algorithm>
#include <cassert>

#include "crypto/hash.h"
#include "network/sink_mem.h"

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

bool ZlibCompressor::CompressStreamOld(const bool flush,
                                   unsigned char **inbuf, size_t *inbufsize,
                                   unsigned char **outbuf, size_t *outbufsize) {
  // Adding compression
  stream_.avail_in = *inbufsize;
  stream_.next_in = *inbuf;
  const int flush_int = (flush) ? Z_FINISH : Z_NO_FLUSH;
  int retcode = 0;

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

StreamStates ZlibCompressor::CompressStream(InputAbstract *input,
                                     cvmfs::MemSink *output, const bool flush) {
  if (!is_healthy_) {
    return kStreamError;
  }

  int flush_int = Z_NO_FLUSH;
  int z_ret;

  do {
    // TODO TODO replace with input->HasInputLeftInChunk()
    if (input->GetIdxInsideChunk() < input->chunk_size()
        && input->chunk_size() != 0) {
      // still stuff to process in the current chunk
    } else if (!input->NextChunk() && stream_.avail_out != 0) {
      return kStreamIOError;
    }

    size_t avail_in = input->chunk_size() - input->GetIdxInsideChunk();
    stream_.avail_in = avail_in;
    stream_.next_in = input->chunk() + input->GetIdxInsideChunk();

    if (!input->has_chunk_left()) {
      flush_int = (flush) ? Z_FINISH : Z_NO_FLUSH;
    }

    size_t avail_out = output->size() - output->pos();
    stream_.avail_out = avail_out;
    stream_.next_out = output->data() + output->pos();

    // Deflate in zlib!
    z_ret = deflate(&stream_, flush_int);

    assert(z_ret == Z_OK || z_ret == Z_STREAM_END);
    assert(output->SetPos(output->pos() + avail_out - stream_.avail_out));

    const size_t processed_in = avail_in - stream_.avail_in;
    input->SetIdxInsideChunk(input->GetIdxInsideChunk() + processed_in);

    if (stream_.avail_out == 0) {
      return kStreamOutBufFull;
    }
  // TODO TODO replace with input->HasInputLeftInChunk()
  } while (input->has_chunk_left()
          || (input->GetIdxInsideChunk() < input->chunk_size()
              && input->chunk_size() != 0));

  if ((flush_int == Z_NO_FLUSH && z_ret == Z_OK && stream_.avail_in == 0)
      || (flush_int == Z_FINISH  && z_ret == Z_STREAM_END)) {
    return kStreamEnd;
  }

  return kStreamContinue;
}


ZlibCompressor::~ZlibCompressor() {
  const int retcode = deflateEnd(&stream_);
  assert(retcode == Z_OK);
}


size_t ZlibCompressor::CompressUpperBound(const size_t bytes) {
  // Call zlib's deflate bound
  return deflateBound(&stream_, bytes);
}

StreamStates ZlibCompressor::Compress(InputAbstract *input,
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
      const size_t have = kZChunk - stream_.avail_out;
      const int64_t written = output->Write(out, have);

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

StreamStates ZlibCompressor::Compress(InputAbstract *input, cvmfs::Sink *output,
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
      const size_t have = kZChunk - stream_.avail_out;
      const int64_t written = output->Write(out, have);

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

bool ZlibCompressor::Reset() {
  if (deflateReset(&stream_) == Z_OK) {
    is_healthy_ = true;
    return true;
  } else {
    is_healthy_ = false;
    return false;
  }
}

std::string ZlibCompressor::Describe() {
  return "ZlibCompressor (default)";
}

}  // namespace zlib
