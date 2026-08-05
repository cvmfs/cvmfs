/**
 * This file is part of the CernVM File System.
 */

#include <cstring>

#include "cache.h"
#include "decompressor_guess.h"
#include "decompressor_zlib.h"
#include "decompressor_zstd.h"
#include "decompressor_echo.h"
#include "util/exception.h"
#include "util/logging.h"

namespace zip {

GuessDecompressor::GuessDecompressor(const zip::Algorithms& alg)
    : Decompressor(alg)
    , backend_(NULL)
    , expected_fmt_(ExpectedContentFormat::kInvalidFormat)
{
}

GuessDecompressor::GuessDecompressor(enum ExpectedContentFormat fmt)
    : Decompressor(zip::Algorithm::kGuessDecompression)
    , backend_(NULL)
    , expected_fmt_(fmt)
{
}

GuessDecompressor::~GuessDecompressor()
{
  delete backend_;
}

GuessDecompressor::GuessDecompressor(const CacheManager::Label &label)
    : Decompressor(zip::Algorithm::kGuessDecompression)
    , backend_(NULL)
{
  if (label.flags & (CacheManager::kLabelCatalog | CacheManager::kLabelHistory)) {
    SetExpectedFormat(kSQLite3);
    return;
  }
  if (label.flags & CacheManager::kLabelCertificate) {
    SetExpectedFormat(kPEM);
    return;
  }
  if (label.flags & CacheManager::kLabelMetainfo) {
    SetExpectedFormat(kJSON);
    return;
  }
  AssertOrLog(false, kLogCvmfs, kLogSyslogWarn | kLogDebug, "No object label flags indicating specific content format to enable decompressor autoconfiguration.");
}

void GuessDecompressor::SetExpectedFormat(enum ExpectedContentFormat fmt)
{
  expected_fmt_ = fmt;
}

char GuessDecompressor::ExpectedFirstByte(enum ExpectedContentFormat fmt)
{
  assert(fmt != kInvalidFormat);
  assert(fmt != kArbitrary);
#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wswitch"
  switch (fmt) {
    case kManifest:  return 'C';
    case kPEM:       return '-';
    case kJSON:      return '{';
    case kSQLite3:   return 'S';
    case kInvalidFormat:   return '\0';
    case kArbitrary: return '\0';
  }
#pragma GCC diagnostic pop
}

bool GuessDecompressor::WillHandle(const zip::Algorithms &alg) {
  return alg == zip::Algorithm::kGuessDecompression;
}


Decompressor* GuessDecompressor::Clone() {
  GuessDecompressor *n = new GuessDecompressor(zip::Algorithm::kGuessDecompression);
  n->SetExpectedFormat(expected_fmt_);
  return n;
}

bool GuessDecompressor::Guess(InputAbstract* input, cvmfs::Sink* output)
{
  assert(!backend_);
  assert(expected_fmt_ != ExpectedContentFormat::kInvalidFormat);
  assert(expected_fmt_ != ExpectedContentFormat::kArbitrary);
  if (input->chunk_size() == 0) {
    bool ok = input->NextChunk();
    if (!ok) {
      return false;
    }
  }
  if (input->chunk_size() == 0 && !input->has_chunk_left()) {
    // empty input. Just set backend to something and say it's OK
    alg_ = zip::Algorithm::kNoCompression;
    backend_ = new zip::EchoDecompressor(alg_);
    return true;
  }
  const unsigned char * const data = input->chunk();
  const size_t data_len = input->chunk_size();

  /* These are some reliable longer signatures of compression methods
   * implemented in CVMFS, for potential future use:
  const unsigned char zlib_sig[2] = {0x78, 0x9c};
  const unsigned char zstd_sig[4] = {0x28, 0xb5, 0x2f, 0xfd};
  */

  const char expected_first_byte = ExpectedFirstByte(expected_fmt_);
  assert(data_len != 0);
  const char first_byte = data[0];
  switch (first_byte) {
    case 0x78: {
      alg_ = zip::Algorithm::kZlib;
      backend_ = new zip::ZlibDecompressor(alg_);
      break;
    }
    case 0x28: {
      alg_ = zip::Algorithm::kZstd;
      backend_ = new zip::ZstdDecompressor(alg_);
      break;
    }
    case 'C':
    case '-':
    case '{':
    case 'S':
    {
      if (first_byte == expected_first_byte) {
        alg_ = zip::Algorithm::kNoCompression;
        backend_ = new zip::EchoDecompressor(alg_);
        break;
      } else {
        LogCvmfs(kLogCvmfs, kLogStderr, "Decompression autoconfiguration failed: expected format %d with first byte 0x%hhx, got 0x%hhx", expected_fmt_, expected_first_byte, first_byte);
        return false;
      }
    }
    default: {
      LogCvmfs(kLogCvmfs, kLogStderr, "Decompression autoconfiguration failed: expected format %d with first byte 0x%hhx, got 0x%hhx (doesn't match any expected compression or content format)", expected_fmt_, expected_first_byte, first_byte);
      return false;
    }
  }
  return true;
}

StreamStates GuessDecompressor::DecompressStream(InputAbstract* input,
                                                 cvmfs::Sink* output) {
  if (!backend_) {
    // Can't read if it's not valid.
    // For example, InputPath on a file which doesn't exist.
    if (!input->IsValid()) {
      return kStreamIOError;
    }
    bool ok = Guess(input, output);
    if (!ok) {
      return kStreamDataError;
    }
  }

  const zip::StreamStates ret = backend_->DecompressStream(input, output);
  return ret;
}

std::string GuessDecompressor::Describe() {
  return "GuessDecompressor";
}

}  // namespace zip
