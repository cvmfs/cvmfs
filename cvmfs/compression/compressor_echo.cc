/**
 * This file is part of the CernVM File System.
 *
 */

#include "compressor_echo.h"

#include <stdlib.h>

#include <algorithm>
#include <cassert>

#include "crypto/hash.h"

namespace zlib {

EchoCompressor::EchoCompressor(const zlib::Algorithms &alg) : Compressor(alg) {
  is_healthy_ = true;
  output_full_ = false;
}


bool EchoCompressor::WillHandle(const zlib::Algorithms &alg) {
  return alg == kNoCompression;
}


Compressor* EchoCompressor::Clone() {
  return new EchoCompressor(zlib::kNoCompression);
}

StreamStates EchoCompressor::CompressStream(InputAbstract *input,
                                cvmfs::MemSink *output, const bool /*flush*/) {
  if (!is_healthy_) {
    return kStreamError;
  }

  do {
    // TODO TODO replace with input->HasInputLeftInChunk()
    if (input->GetIdxInsideChunk() < input->chunk_size()
        && input->chunk_size() != 0) {
      // still stuff to process in the current chunk
    } else if (!input->NextChunk() && !output_full_) {
      return kStreamIOError;
    }

    const size_t have = input->chunk_size();
    const int64_t written = output->Write(input->chunk(), have);

    input->SetIdxInsideChunk(input->GetIdxInsideChunk() + written);
    if (written != static_cast<int64_t>(have)) {
      output_full_ = true;
      return kStreamOutBufFull;
    }

  // TODO TODO replace with input->HasInputLeftInChunk()
  } while (input->has_chunk_left()
          || (input->GetIdxInsideChunk() < input->chunk_size()
              && input->chunk_size() != 0));
  output_full_ = false;
  return kStreamEnd;
}

// bool EchoCompressor::CompressStream(
//   const bool /*flush*/,
//   unsigned char **inbuf, size_t *inbufsize,
//   unsigned char **outbuf, size_t *outbufsize)
// {
//   const size_t bytes_to_copy = std::min(*outbufsize, *inbufsize);
//   memcpy(*outbuf, *inbuf, bytes_to_copy);
//   const bool done = (bytes_to_copy == *inbufsize);

//   // Update the return variables
//   *inbuf += bytes_to_copy;
//   *outbufsize = bytes_to_copy;
//   *inbufsize -= bytes_to_copy;

//   return done;
// }

StreamStates EchoCompressor::Compress(InputAbstract *input,
                                      cvmfs::Sink *output) {
  if (!is_healthy_) {
    return kStreamError;
  }

  do {
    if (!input->NextChunk()) {
      return kStreamIOError;
    }

    const size_t have = input->chunk_size();
    const int64_t written = output->Write(input->chunk(), have);

    if (written != static_cast<int64_t>(have)) {
      is_healthy_ = false;
      return kStreamIOError;
    }
  } while (input->has_chunk_left());

  output->Flush();
  return kStreamEnd;
}

// not sure if this makes sense to even have this function available?
StreamStates EchoCompressor::Compress(InputAbstract *input, cvmfs::Sink *output,
                                      shash::Any *compressed_hash) {
  if (!is_healthy_) {
    return kStreamError;
  }

  shash::ContextPtr hash_context(compressed_hash->algorithm);
  hash_context.buffer = alloca(hash_context.size);
  shash::Init(hash_context);

  do {
    if (!input->NextChunk()) {
      return kStreamIOError;
    }

    const size_t have = input->chunk_size();
    const int64_t written = output->Write(input->chunk(), have);

    if (written != static_cast<int64_t>(have)) {
      is_healthy_ = false;
      return kStreamIOError;
    }
    shash::Update(input->chunk(), have, hash_context);
  } while (input->has_chunk_left());

  output->Flush();
  shash::Final(hash_context, compressed_hash);
  return kStreamEnd;
}


size_t EchoCompressor::CompressUpperBound(const size_t bytes) {
  // zero bytes as an upper bound is no good because some callers want to
  // allocate buffers according to this value
  return (bytes == 0) ? 1 : bytes;
}

std::string EchoCompressor::Describe() {
  return "EchoCompressor (no compression)";
}

}  // namespace zlib
