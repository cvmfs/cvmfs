/**
 * This file is part of the CernVM File System.
 */

#include "decompressor.h"

#include "decompressor_echo.h"
#include "decompressor_guess.h"
#include "decompressor_zlib.h"
#include "decompressor_zstd.h"
namespace zip {

void Decompressor::RegisterPlugins() {
  RegisterPlugin<ZlibDecompressor>();
  RegisterPlugin<ZstdDecompressor>();
  RegisterPlugin<EchoDecompressor>();
  RegisterPlugin<GuessDecompressor>();
}

}  // namespace zlib
