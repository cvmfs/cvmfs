/**
 * This file is part of the CernVM File System.
 */

#include "decompressor_zstd.h"

#include <cassert>
#include <zstd.h>
#include <zstd_errors.h>

#include "decompressor.h"
#include "util/logging.h"

namespace zip {

ZstdDecompressor::ZstdDecompressor(const zip::Algorithms &alg) :
                                      Decompressor(alg, ZSTD_DStreamOutSize()) {
  stream_ = ZSTD_createDCtx();
  assert(stream_ != NULL);
  is_healthy_ = true;
}

ZstdDecompressor::~ZstdDecompressor() {
  assert(ZSTD_freeDCtx(stream_) == 0);
}

bool ZstdDecompressor::WillHandle(const zip::Algorithms &alg) {
  return alg == kZstd;
}


Decompressor* ZstdDecompressor::Clone() {
  ZstdDecompressor* other = new ZstdDecompressor(zip::kZstd);

  // WARNING WARNING WARNING
  // not implemented
  //
  // assert(stream_.avail_in == 0);
  // // Delete the other stream
  // int retcode = inflateEnd(&other->stream_);
  // assert(retcode == Z_OK);
  // retcode = inflateCopy(const_cast<z_streamp>(&other->stream_), &stream_);
  // assert(retcode == Z_OK);
  return other;
}

StreamStates ZstdDecompressor::DecompressStream(InputAbstract *input,
                                            cvmfs::Sink *output) {
  if (!is_healthy_) {
    return kStreamError;
  }

  unsigned char out[kZChunk_];
  size_t z_ret;

  // FIXME maybe do ZSTD_initDStream()?
  do {
    if (!input->HasInputLeftInChunk()) {
      if (!input->NextChunk()) {
        return kStreamIOError;
      }
    }

    ZSTD_inBuffer inBuffer = {
      input->chunk(),
      input->chunk_size(),
      input->GetIdxInsideChunk()};

    // Run deflate() on input until output buffer has no space left
    do {
      ZSTD_outBuffer outBuffer = {out, kZChunk_, 0};

      z_ret = ZSTD_decompressStream(stream_, &outBuffer, &inBuffer);
      if (ZSTD_isError(z_ret)) {
        LogCvmfs(kLogCompress, kLogSyslogErr | kLogStderr,
                                          "Error during zstd decompression: %s (ZSTD_ErrorCode %d)",
                                           ZSTD_getErrorName(z_ret), ZSTD_getErrorCode(z_ret));
        is_healthy_ = false;
        return kStreamDataError;
      }
      input->SetIdxInsideChunk(inBuffer.pos);
      const size_t have = outBuffer.pos;
      if (have == 0) {
        break;
      }
      const int64_t written = output->Write(out, have);

      if (written < 0) {
        is_healthy_ = false;
        return kStreamIOError;
      }
      if (z_ret == 0) {
        break;
      }
    } while (
        inBuffer.pos < inBuffer.size // input buffer has something to process
        );
  } while (input->has_chunk_left());

  output->Flush();

  if (z_ret == 0) {
    // Reset();
    return kStreamEnd;
  } else {
    // z_ret != 0 means that ZSTD_decompressStream did not end on a frame, but
    // we reached the end of the input
    return kStreamContinue;
  }
}

bool ZstdDecompressor::Reset() {
  if (ZSTD_DCtx_reset(stream_, ZSTD_reset_session_and_parameters) == 0) {
    is_healthy_ = true;
    return true;
  } else {
    is_healthy_ = false;
    return false;
  }
}

std::string ZstdDecompressor::Describe() {
  return "ZstdDecompressor (default)";
}

}  // namespace zlib
