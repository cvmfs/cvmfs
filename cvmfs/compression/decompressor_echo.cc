/**
 * This file is part of the CernVM File System.
 */

#include "decompressor_echo.h"

namespace zip {

EchoDecompressor::EchoDecompressor(const zip::Algorithms &alg) :
                                                             Decompressor(alg) {
  is_healthy_ = true;
}


bool EchoDecompressor::WillHandle(const zip::Algorithms &alg) {
  return alg == kNoCompression;
}


Decompressor* EchoDecompressor::Clone() {
  return new EchoDecompressor(zip::kNoCompression);
}

StreamStates EchoDecompressor::DecompressStream(InputAbstract *input,
                                                          cvmfs::Sink *output) {
  if (!is_healthy_) {
    return kStreamError;
  }

  do {
    if (!input->HasInputLeftInChunk()) {
      if (!input->NextChunk()) {
        return kStreamIOError;
      }
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

std::string EchoDecompressor::Describe() {
  return "EchoDecompressor (no compression)";
}

}  // namespace zlib
