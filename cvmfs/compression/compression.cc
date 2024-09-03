/**
 * This file is part of the CernVM File System.
 *
 * This is a wrapper around zlib.  It provides
 * a set of functions to conveniently compress and decompress stuff.
 * Almost all of the functions return true on success, otherwise false.
 *
 * TODO: think about code deduplication
 */

#include "compression.h"

#include <alloca.h>
#include <stdlib.h>
#include <sys/stat.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>

#include "cvmfs_config.h"
#include "compressor_echo.h"
#include "compressor_zlib.h"

#include "crypto/hash.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/smalloc.h"

namespace zlib {

void Compressor::RegisterPlugins() {
  RegisterPlugin<ZlibCompressor>();
  RegisterPlugin<EchoCompressor>();
}

}  // namespace zlib
