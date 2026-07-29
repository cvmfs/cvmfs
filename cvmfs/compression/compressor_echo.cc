/**
 * This file is part of the CernVM File System.
 *
 */

#include "compressor_echo.h"

#include <stdlib.h>

#include <algorithm>
#include <cassert>

#include "crypto/hash.h"

namespace zip {

EchoCompressor::EchoCompressor(const zip::Algorithms &alg) : Compressor(alg) {
  is_healthy_ = true;
  output_full_ = false;
}

EchoCompressor::EchoCompressor() : Compressor(zip::Algorithm::kNoCompression) {
  is_healthy_ = true;
  output_full_ = false;
}

bool EchoCompressor::WillHandle(const zip::Algorithms &alg) {
  return alg == kNoCompression;
}


Compressor* EchoCompressor::Clone() {
  return new EchoCompressor(zip::kNoCompression);
}

StreamStates EchoCompressor::StreamingStep(InputAbstract* input,
                                           cvmfs::MemSink* output,
                                           const bool flush) {
  const size_t have = input->chunk_size() - input->GetIdxInsideChunk();
  const size_t can_write = output->size() - output->pos();
  const size_t gonna_write = std::min(have, can_write);
  if (gonna_write == 0) {
    assert(flush);
    return kStreamEnd;
  }
  const int64_t written = output->Write(input->chunk() + input->GetIdxInsideChunk(), gonna_write);
  if (written < 0) {
    return kStreamIOError;
  }
  input->SetIdxInsideChunk(input->GetIdxInsideChunk() + written);
  if (!input->HasInputLeftInChunk() && !input->has_chunk_left() && flush) {
    return kStreamEnd;
  } else {
    return kStreamContinue;
  }
}

StreamStates EchoCompressor::Compress(InputAbstract *input,
                                      cvmfs::Sink *output) {
  if (!is_healthy_) {
    return kStreamError;
  }

  do {
    if (input->GetIdxInsideChunk() == input->chunk_size() && input->has_chunk_left()) {
      bool ok = input->NextChunk();
      if (!ok) {
        return kStreamIOError;
      }
    }

    const size_t have = input->chunk_size();
    const int64_t written = output->Write(input->chunk() + input->GetIdxInsideChunk(), have);
    if (written < 0) {
      is_healthy_ = false;
      return kStreamIOError;
    }
    input->SetIdxInsideChunk(input->GetIdxInsideChunk() + written);
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
    if (input->GetIdxInsideChunk() == input->chunk_size() && input->has_chunk_left()) {
      bool ok = input->NextChunk();
      if (!ok) {
        return kStreamIOError;
      }
    }

    const size_t have = input->chunk_size();
    const int64_t written = output->Write(input->chunk() + input->GetIdxInsideChunk(), have);
    if (written < 0) {
      is_healthy_ = false;
      return kStreamIOError;
    }
    input->SetIdxInsideChunk(input->GetIdxInsideChunk() + written);
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
