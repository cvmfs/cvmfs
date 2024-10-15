/**
 * This file is part of the CernVM File System.
 */

#include "decompression.h"

#include "decompressor_echo.h"
#include "decompressor_zlib.h"
#include "decompressor_zstd.h"
namespace zlib {

void Decompressor::RegisterPlugins() {
  RegisterPlugin<ZlibDecompressor>();
  RegisterPlugin<ZstdDecompressor>();
  RegisterPlugin<EchoDecompressor>();
}

}  // namespace zlib
