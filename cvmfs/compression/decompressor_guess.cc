/**
 * This file is part of the CernVM File System.
 */

#include <cstring>

#include "decompressor_guess.h"

namespace zip {

GuessDecompressor::GuessDecompressor(const zip::Algorithms& alg)
    : Decompressor(alg),
    is_fresh_(true),
    backend_(NULL)
{
}

bool GuessDecompressor::WillHandle(const zip::Algorithms &alg) {
  return true;
}


Decompressor* GuessDecompressor::Clone() {
  return new GuessDecompressor(zip::kNoCompression);
}

void GuessDecompressor::Guess(InputAbstract* input, cvmfs::Sink* output)
{
  const unsigned char * const data = input->chunk();
  const size_t data_len = input->chunk_size();

  // What CVMFS tends to create. See with:
  // for x in /srv/cvmfs/*/data/*/*; do file $x; xxd $x | head -n1; done
  const unsigned char zlib_sig[2] = {0x78, 0x9c};
  const unsigned char zstd_sig[4] = {0x28, 0xb5, 0x2f, 0xfd};

  // TODO test that decompression is successful.
  // What if it's just such a file, in a repo having compression disabled?
  // But that's not the default configuration so less important.
  if (data_len >= sizeof(zlib_sig) && !memcmp(data, zlib_sig, sizeof(zlib_sig))) {
    alg_ = zip::Algorithm::kZlib;
  } else if (data_len >= sizeof(zstd_sig) && !memcmp(data, zstd_sig, sizeof(zstd_sig))) {
    alg_ = zip::Algorithm::kZlib;
  } else {
    alg_ = zip::Algorithm::kNoCompression;
  }
  backend_ = zip::Decompressor::Construct(alg_);
}

StreamStates GuessDecompressor::DecompressStream(InputAbstract* input,
                                                 cvmfs::Sink* output) {
  if (is_fresh_) {
    assert(!backend_);
    Guess(input, output);
  } else {
    assert(backend_);
  }

  const zip::StreamStates ret = backend_->DecompressStream(input, output);
  return ret;
}

std::string GuessDecompressor::Describe() {
  return "GuessDecompressor";
}

}  // namespace zip
