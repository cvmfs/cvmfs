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

namespace zlib {

bool ZstdCompressor::WillHandle(const zlib::Algorithms &alg) {
  return alg == kZstdDefault;
}


ZstdCompressor::ZstdCompressor(const Algorithms &alg) : Compressor(alg) {
  stream_ = ZSTD_createCCtx();
  ZSTD_CCtx_setParameter(stream_, ZSTD_c_compressionLevel, 3);
  ZSTD_CCtx_setParameter(stream_, ZSTD_c_checksumFlag, 1);
  is_healthy_ = true;
  compress_stream_outbuf_full_ = false;
  zstd_chunk_ = ZSTD_CStreamOutSize();
}


/**
 * Duplicate an existing context `srcCCtx` into another one `dstCCtx`.
 * Only works during stage ZSTDcs_init
 * (i.e. after creation, but before first call to ZSTD_compressContinue()).
 */
Compressor* ZstdCompressor::Clone() {
  ZstdCompressor* other = new ZstdCompressor(zlib::kZstdDefault);

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

StreamStates ZstdCompressor::CompressStream(InputAbstract *input,
                                     cvmfs::MemSink *output, const bool flush) {
  if (!is_healthy_) {
    return kStreamError;
  }

  ZSTD_EndDirective mode = ZSTD_e_continue;

  size_t remaining;
  do {
    // TODO TODO replace with input->HasInputLeftInChunk()
    if (input->GetIdxInsideChunk() < input->chunk_size()
        && input->chunk_size() != 0) {
      // still stuff to process in the current chunk
    } else if (!input->NextChunk() && !compress_stream_outbuf_full_) {
      return kStreamIOError;
    }
    compress_stream_outbuf_full_ = false;

    ZSTD_inBuffer inBuffer = {input->chunk(), input->chunk_size(),
                              input->GetIdxInsideChunk()};

    if (!input->has_chunk_left()) {
      mode = (flush) ? ZSTD_e_end : ZSTD_e_continue;
    }

    ZSTD_outBuffer outBuffer = {output->data(), output->size(), output->pos()};

    remaining = ZSTD_compressStream2(stream_, &outBuffer, &inBuffer, mode);

    if (ZSTD_isError(remaining)) {
      ZSTD_freeCCtx(stream_);
      is_healthy_ = false;
      return kStreamDataError;
    }

    assert(output->SetPos(outBuffer.pos));
    input->SetIdxInsideChunk(inBuffer.pos);

    if (outBuffer.pos == outBuffer.size) {
      compress_stream_outbuf_full_ = true;
      return kStreamOutBufFull;
    }
  // TODO TODO replace with input->HasInputLeftInChunk()
  } while (input->has_chunk_left()
          || (input->GetIdxInsideChunk() < input->chunk_size()
              && input->chunk_size() != 0));
  return kStreamEnd;
}


ZstdCompressor::~ZstdCompressor() {
  assert(ZSTD_freeCCtx(stream_) == 0);
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

  unsigned char out[zstd_chunk_];
  ZSTD_EndDirective mode = ZSTD_e_continue;

  do {
    if (!input->NextChunk()) {
      return kStreamIOError;
    }

    if (!input->has_chunk_left()) {
      mode = ZSTD_e_end;
    }
    ZSTD_inBuffer inBuffer = {input->chunk(), input->chunk_size(), 0};

    size_t remaining;
    // Run deflate() on input until output buffer has no space left
    do {
      ZSTD_outBuffer outBuffer = {out, zstd_chunk_, 0};

      remaining = ZSTD_compressStream2(stream_, &outBuffer, &inBuffer, mode);
      if (ZSTD_isError(remaining)) {
        ZSTD_freeCCtx(stream_);
        is_healthy_ = false;
        return kStreamDataError;
      }
      const size_t have = outBuffer.pos;
      const int64_t written = output->Write(out, have);

      if (written != static_cast<int64_t>(have)) {
        ZSTD_freeCCtx(stream_);
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

  unsigned char out[zstd_chunk_];
  ZSTD_EndDirective mode = ZSTD_e_continue;

  shash::ContextPtr hash_context(compressed_hash->algorithm);
  hash_context.buffer = alloca(hash_context.size);
  shash::Init(hash_context);

  do {
    if (!input->NextChunk()) {
      return kStreamIOError;
    }

    if (!input->has_chunk_left()) {
      mode = ZSTD_e_end;
    }
    ZSTD_inBuffer inBuffer = {input->chunk(), input->chunk_size(), 0};

    size_t remaining;
    // Run deflate() on input until output buffer has no space left
    do {
      ZSTD_outBuffer outBuffer = {out, zstd_chunk_, 0};

      remaining = ZSTD_compressStream2(stream_, &outBuffer, &inBuffer, mode);
      if (ZSTD_isError(remaining)) {
        ZSTD_freeCCtx(stream_);
        is_healthy_ = false;
        return kStreamDataError;
      }
      const size_t have = outBuffer.pos;
      const int64_t written = output->Write(out, have);

      if (written != static_cast<int64_t>(have)) {
        ZSTD_freeCCtx(stream_);
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
