/**
 * This file is part of the CernVM File System.
 *
 */

#include "compressor_zstd.h"

#include <alloca.h>
#include <stdlib.h>

#include <algorithm>
#include <cassert>

#include "crypto/hash.h"
#include "network/sink_mem.h"

using namespace std;  // NOLINT

namespace zip {

bool ZstdCompressor::WillHandle(const zip::Algorithms &alg) {
  return alg == kZstd;
}

ZstdCompressor::ZstdCompressor(const Algorithms& alg)
    : Compressor(alg, ZSTD_CStreamOutSize()) {
  Init();
}

ZstdCompressor::ZstdCompressor()
    : Compressor(zip::Algorithm::kZstd, ZSTD_CStreamOutSize()) {
  Init();
}

void ZstdCompressor::Init() {
  stream_ = ZSTD_createCCtx();
  ZSTD_CCtx_setParameter(stream_, ZSTD_c_compressionLevel, 3);
  ZSTD_CCtx_setParameter(stream_, ZSTD_c_checksumFlag, 1);
  is_healthy_ = true;
  compress_stream_outbuf_full_ = false;
}


/**
 * Duplicate an existing context `srcCCtx` into another one `dstCCtx`.
 * Only works during stage ZSTDcs_init
 * (i.e. after creation, but before first call to ZSTD_compressContinue()).
 */
Compressor* ZstdCompressor::Clone() {
  ZstdCompressor* other = new ZstdCompressor(zip::kZstd);

  // WARNING WARNING NOT IMPLEMENTED


/* ***************************************************************************
 *   ADVANCED AND EXPERIMENTAL FUNCTIONS
 *****************************************************************************
 * The definitions in the following section are considered experimental.
 * They are provided for advanced scenarios.
 * They should never be used with a dynamic library, as prototypes may change
 * in the future.
 * Use them only in association with static linking.
 * ****************************************************************************/

  // #if defined(ZSTD_STATIC_LINKING_ONLY)
  //                                && !defined(ZSTD_H_ZSTD_STATIC_LINKING_ONLY)
  // assert(ZSTD_copyCCtx(other->stream_, stream_, ZSTD_CONTENTSIZE_UNKNOWN)
  //                                                                      == 0);
  return other;
}

StreamStates ZstdCompressor::StreamingStep(InputAbstract* input,
                                           cvmfs::MemSink* output,
                                           const bool flush) {
  ZSTD_EndDirective mode;
  if (!input->has_chunk_left() && flush) {
    mode = ZSTD_e_end;
  } else {
    mode = ZSTD_e_continue;
  }
  ZSTD_inBuffer inBuffer = {input->chunk(), input->chunk_size(),
    input->GetIdxInsideChunk()};
  ZSTD_outBuffer outBuffer = {output->data(), output->size(), output->pos()};
  size_t remaining = ZSTD_compressStream2(stream_, &outBuffer, &inBuffer, mode);
  if (ZSTD_isError(remaining)) {
    is_healthy_ = false;
    return kStreamDataError;
  }
  assert(output->SetPos(outBuffer.pos));
  input->SetIdxInsideChunk(inBuffer.pos);
  if (!input->HasInputLeftInChunk() && !input->has_chunk_left() && flush && remaining == 0) {
    return kStreamEnd;
  }
  return kStreamContinue;
}


ZstdCompressor::~ZstdCompressor() {
  auto ret = ZSTD_freeCCtx(stream_);
  assert(ret == 0);
}


size_t ZstdCompressor::CompressUpperBound(const size_t bytes) {
  return ZSTD_compressBound(bytes);
}

// ZSTDLIB_API size_t ZSTD_CStreamInSize(void);
// **< recommended size for input buffer */
// ZSTDLIB_API size_t ZSTD_CStreamOutSize(void);
// **< recommended size for output buffer. Guarantee to successfully
//  flush at least one complete compressed block. */
StreamStates ZstdCompressor::Compress(InputAbstract *input,
                                      cvmfs::Sink *output) {
  if (!is_healthy_) {
    return kStreamError;
  }

  unsigned char out[kZChunk_];
  ZSTD_EndDirective mode = ZSTD_e_continue;

  do {
    if (input->GetIdxInsideChunk() == input->chunk_size() && input->has_chunk_left()) {
      bool ok = input->NextChunk();
      if (!ok) {
        return kStreamIOError;
      }
    }

    if (!input->has_chunk_left()) {
      mode = ZSTD_e_end;
    }
    ZSTD_inBuffer inBuffer = {input->chunk(), input->chunk_size(), 0};

    size_t remaining;
    // Run deflate() on input until output buffer has no space left
    do {
      ZSTD_outBuffer outBuffer = {out, kZChunk_, 0};

      remaining = ZSTD_compressStream2(stream_, &outBuffer, &inBuffer, mode);
      if (ZSTD_isError(remaining)) {
        is_healthy_ = false;
        return kStreamDataError;
      }
      input->SetIdxInsideChunk(inBuffer.pos);
      const size_t have = outBuffer.pos;
      const int64_t written = output->Write(out, have);

      if (written != static_cast<int64_t>(have)) {
        is_healthy_ = false;
        return kStreamIOError;
      }
    } while (inBuffer.pos < inBuffer.size);
  } while (mode != ZSTD_e_end);

  output->Flush();

  Reset();
  return kStreamEnd;
}

StreamStates ZstdCompressor::Compress(InputAbstract *input, cvmfs::Sink *output,
                                      shash::Any *compressed_hash) {
  if (!is_healthy_) {
    return kStreamError;
  }

  unsigned char out[kZChunk_];
  ZSTD_EndDirective mode = ZSTD_e_continue;

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

    if (!input->has_chunk_left()) {
      mode = ZSTD_e_end;
    }
    ZSTD_inBuffer inBuffer = {input->chunk(), input->chunk_size(), 0};

    size_t remaining;
    // Run deflate() on input until output buffer has no space left
    do {
      ZSTD_outBuffer outBuffer = {out, kZChunk_, 0};

      remaining = ZSTD_compressStream2(stream_, &outBuffer, &inBuffer, mode);
      if (ZSTD_isError(remaining)) {
        is_healthy_ = false;
        return kStreamDataError;
      }
      input->SetIdxInsideChunk(inBuffer.pos);
      const size_t have = outBuffer.pos;
      const int64_t written = output->Write(out, have);

      if (written != static_cast<int64_t>(have)) {
        is_healthy_ = false;
        return kStreamIOError;
      }
      shash::Update(out, have, hash_context);
    } while (inBuffer.pos < inBuffer.size);
  } while (mode != ZSTD_e_end);

  output->Flush();

  shash::Final(hash_context, compressed_hash);
  Reset();
  return kStreamEnd;
}

bool ZstdCompressor::Reset() {
  if (ZSTD_CCtx_reset(stream_, ZSTD_reset_session_and_parameters) == 0) {
    ZSTD_CCtx_setParameter(stream_, ZSTD_c_compressionLevel, 3);
    ZSTD_CCtx_setParameter(stream_, ZSTD_c_checksumFlag, 1);
    is_healthy_ = true;
    return true;
  } else {
    is_healthy_ = false;
    return false;
  }
}

std::string ZstdCompressor::Describe() {
  return "ZstdCompressor (default)";
}

}  // namespace zlib
