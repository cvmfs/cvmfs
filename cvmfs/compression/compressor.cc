/**
 * This file is part of the CernVM File System.
 *
 * This is a wrapper around zlib.  It provides
 * a set of functions to conveniently compress and decompress stuff.
 * Almost all of the functions return true on success, otherwise false.
 *
 * TODO: think about code deduplication
 */

#include "compressor.h"

#include <alloca.h>
#include <stdlib.h>
#include <sys/stat.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>

#include "compressor_echo.h"
#include "compressor_zlib.h"
#include "compressor_zstd.h"

#include "crypto/hash.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/smalloc.h"

namespace zip {

void Compressor::RegisterPlugins() {
  RegisterPlugin<ZlibCompressor>();
  RegisterPlugin<ZstdCompressor>();
  RegisterPlugin<EchoCompressor>();
}

StreamStates Compressor::CompressStream(InputAbstract* input,
                                        cvmfs::MemSink* output,
                                        const bool flush) {
  if (!is_healthy_) {
    return kStreamError;
  }

  do {
    assert(output->pos() <= output->size());
    if (!(output->pos() <= output->size())) {
      return kStreamError;
    }

    if (output->pos() == output->size()) {
      return kStreamOutBufFull;
    }
    if (input->GetIdxInsideChunk() == input->chunk_size() && !input->has_chunk_left() && !flush) {
      return kStreamEnd;
    }
    if (input->GetIdxInsideChunk() == input->chunk_size() && input->has_chunk_left()) {
      bool ok = input->NextChunk();
      if (!ok) {
        return kStreamIOError;
      }
    }
    StreamStates step_ret = StreamingStep(input, output, flush);
    if (step_ret != kStreamContinue) {
      return step_ret;
    }

  } while (true);
}
}  // namespace zlib
