/**
 * This file is part of the CernVM File System.
 */

#include "decompression.h"

#include "decompressor_echo.h"
#include "decompressor_zlib.h"
namespace zlib {

void Decompressor::RegisterPlugins() {
  RegisterPlugin<ZlibDecompressor>();
  RegisterPlugin<EchoDecompressor>();
}

}  // namespace zlib
