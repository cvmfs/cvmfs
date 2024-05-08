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
#include "echo.h"
#include "zlib.h"

#include "crypto/hash.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/smalloc.h"

namespace zlib {

const unsigned kBufferSize = 32768;

/**
 * Aborts if string doesn't match any of the algorithms.
 */
Algorithms ParseCompressionAlgorithm(const std::string &algorithm_option) {
  if ((algorithm_option == "default") || (algorithm_option == "zlib"))
    return kZlibDefault;
  if (algorithm_option == "none")
    return kNoCompression;
  PANIC(kLogStderr, "unknown compression algorithms: %s",
        algorithm_option.c_str());
}


std::string AlgorithmName(const zlib::Algorithms alg) {
  switch (alg) {
    case kZlibDefault:
      return "zlib";
      break;
    case kNoCompression:
      return "none";
      break;
    // Purposely did not add a 'default' statement here: this will
    // cause the compiler to generate a warning if a new algorithm
    // is added but this function is not updated.
  }
  return "unknown";
}


void CompressInit(z_stream *strm) {
  strm->zalloc = Z_NULL;
  strm->zfree = Z_NULL;
  strm->opaque = Z_NULL;
  strm->next_in = Z_NULL;
  strm->avail_in = 0;
  int retval = deflateInit(strm, Z_DEFAULT_COMPRESSION);
  assert(retval == 0);
}


void DecompressInit(z_stream *strm) {
  strm->zalloc = Z_NULL;
  strm->zfree = Z_NULL;
  strm->opaque = Z_NULL;
  strm->avail_in = 0;
  strm->next_in = Z_NULL;
  int retval = inflateInit(strm);
  assert(retval == 0);
}


void CompressFini(z_stream *strm) {
  (void)deflateEnd(strm);
}


void DecompressFini(z_stream *strm) {
  (void)inflateEnd(strm);
}

StreamStates DecompressZStream2Sink(
  const void *buf,
  const int64_t size,
  z_stream *strm,
  cvmfs::Sink *sink)
{
  unsigned char out[kZChunk];
  int z_ret;
  int64_t pos = 0;

  do {
    strm->avail_in = (kZChunk > (size-pos)) ? size-pos : kZChunk;
    strm->next_in = ((unsigned char *)buf)+pos;

    // Run inflate() on input until output buffer not full
    do {
      strm->avail_out = kZChunk;
      strm->next_out = out;
      z_ret = inflate(strm, Z_NO_FLUSH);
      switch (z_ret) {
        case Z_NEED_DICT:
          z_ret = Z_DATA_ERROR;  // and fall through
        case Z_STREAM_ERROR:
        case Z_DATA_ERROR:
          return kStreamDataError;
        case Z_MEM_ERROR:
          return kStreamIOError;
      }
      size_t have = kZChunk - strm->avail_out;
      int64_t written = sink->Write(out, have);
      if ((written < 0) || (static_cast<uint64_t>(written) != have))
        return kStreamIOError;
    } while (strm->avail_out == 0);

    pos += kZChunk;
  } while (pos < size);

  return (z_ret == Z_STREAM_END ? kStreamEnd : kStreamContinue);
}


StreamStates DecompressZStream2File(
  const void *buf,
  const int64_t size,
  z_stream *strm,
  FILE *f)
{
  unsigned char out[kZChunk];
  int z_ret;
  int64_t pos = 0;

  do {
    strm->avail_in = (kZChunk > (size-pos)) ? size-pos : kZChunk;
    strm->next_in = ((unsigned char *)buf)+pos;

    // Run inflate() on input until output buffer not full
    do {
      strm->avail_out = kZChunk;
      strm->next_out = out;
      z_ret = inflate(strm, Z_NO_FLUSH);
      switch (z_ret) {
        case Z_NEED_DICT:
          z_ret = Z_DATA_ERROR;  // and fall through
        case Z_STREAM_ERROR:
        case Z_DATA_ERROR:
          return kStreamDataError;
        case Z_MEM_ERROR:
          return kStreamIOError;
      }
      size_t have = kZChunk - strm->avail_out;
      if (fwrite(out, 1, have, f) != have || ferror(f)) {
        LogCvmfs(kLogCompress, kLogDebug, "Inflate to file failed with %s "
             "(errno=%d)", strerror(errno), errno);
        return kStreamIOError;
      }
    } while (strm->avail_out == 0);

    pos += kZChunk;
  } while (pos < size);

  return (z_ret == Z_STREAM_END ? kStreamEnd : kStreamContinue);
}

bool DecompressPath2Path(const std::string &src, const std::string &dest) {
  FILE *fsrc = NULL;
  FILE *fdest = NULL;
  int result = false;

  fsrc = fopen(src.c_str(), "r");
  if (!fsrc) goto decompress_path2path_final;

  fdest = fopen(dest.c_str(), "w");
  if (!fdest) goto decompress_path2path_final;

  result = DecompressFile2File(fsrc, fdest);

 decompress_path2path_final:
  if (fsrc) fclose(fsrc);
  if (fdest) fclose(fdest);
  return result;
}

bool DecompressFile2File(FILE *fsrc, FILE *fdest) {
  bool result = false;
  StreamStates stream_state = kStreamIOError;
  z_stream strm;
  size_t have;
  unsigned char buf[kBufferSize];

  DecompressInit(&strm);

  while ((have = fread(buf, 1, kBufferSize, fsrc)) > 0) {
    stream_state = DecompressZStream2File(buf, have, &strm, fdest);
    if ((stream_state == kStreamDataError) || (stream_state == kStreamIOError))
      goto decompress_file2file_final;
  }
  LogCvmfs(kLogCompress, kLogDebug, "end of decompression, state=%d, error=%d",
           stream_state, ferror(fsrc));
  if ((stream_state != kStreamEnd) || ferror(fsrc))
    goto decompress_file2file_final;

  result = true;

 decompress_file2file_final:
  DecompressFini(&strm);
  return result;
}


bool DecompressPath2File(const std::string &src, FILE *fdest) {
  FILE *fsrc = fopen(src.c_str(), "r");
  if (!fsrc)
    return false;

  bool retval = DecompressFile2File(fsrc, fdest);
  fclose(fsrc);
  return retval;
}


/**
 * User of this function has to free out_buf.
 */
bool CompressMem2Mem(const void *buf, const int64_t size,
                    void **out_buf, uint64_t *out_size)
{
  unsigned char out[kZChunk];
  int z_ret;
  int flush;
  z_stream strm;
  int64_t pos = 0;
  uint64_t alloc_size = kZChunk;

  CompressInit(&strm);
  *out_buf = smalloc(alloc_size);
  *out_size = 0;

  do {
    strm.avail_in = (kZChunk > (size-pos)) ? size-pos : kZChunk;
    flush = (pos + kZChunk) >= size ? Z_FINISH : Z_NO_FLUSH;
    strm.next_in = ((unsigned char *)buf) + pos;

    // Run deflate() on input until output buffer not full
    do {
      strm.avail_out = kZChunk;
      strm.next_out = out;
      z_ret = deflate(&strm, flush);
      if (z_ret == Z_STREAM_ERROR) {
        CompressFini(&strm);
        free(*out_buf);
        *out_buf = NULL;
        *out_size = 0;
        return false;
      }
      size_t have = kZChunk - strm.avail_out;
      if (*out_size+have > alloc_size) {
        alloc_size *= 2;
        *out_buf = srealloc(*out_buf, alloc_size);
      }
      memcpy(static_cast<unsigned char *>(*out_buf) + *out_size, out, have);
      *out_size += have;
    } while (strm.avail_out == 0);

    pos += kZChunk;
  } while (flush != Z_FINISH);

  CompressFini(&strm);
  if (z_ret != Z_STREAM_END) {
    free(*out_buf);
    *out_buf = NULL;
    *out_size = 0;
    return false;
  } else {
    return true;
  }
}


/**
 * User of this function has to free out_buf.
 */
bool DecompressMem2Mem(const void *buf, const int64_t size,
                       void **out_buf, uint64_t *out_size)
{
  unsigned char out[kZChunk];
  int z_ret = Z_ERRNO;
  z_stream strm;
  int64_t pos = 0;
  uint64_t alloc_size = kZChunk;

  DecompressInit(&strm);
  *out_buf = smalloc(alloc_size);
  *out_size = 0;

  do {
    strm.avail_in = ((size-pos) < kZChunk) ? size-pos : kZChunk;
    strm.next_in = ((unsigned char *)buf)+pos;

    // Run inflate() on input until output buffer not full
    do {
      strm.avail_out = kZChunk;
      strm.next_out = out;
      z_ret = inflate(&strm, Z_NO_FLUSH);
      switch (z_ret) {
        case Z_NEED_DICT:
          z_ret = Z_DATA_ERROR;  // and fall through
        case Z_STREAM_ERROR:
        case Z_DATA_ERROR:
        case Z_MEM_ERROR:
          DecompressFini(&strm);
          free(*out_buf);
          *out_buf = NULL;
          *out_size = 0;
          return false;
      }
      size_t have = kZChunk - strm.avail_out;
      if (*out_size+have > alloc_size) {
        alloc_size *= 2;
        *out_buf = srealloc(*out_buf, alloc_size);
      }
      memcpy(static_cast<unsigned char *>(*out_buf) + *out_size, out, have);
      *out_size += have;
    } while (strm.avail_out == 0);

    pos += kZChunk;
  } while (pos < size);

  DecompressFini(&strm);
  if (z_ret != Z_STREAM_END) {
    free(*out_buf);
    *out_buf = NULL;
    *out_size = 0;
    return false;
  }

  return true;
}


//------------------------------------------------------------------------------


void Compressor::RegisterPlugins() {
  RegisterPlugin<ZlibCompressor>();
  RegisterPlugin<EchoCompressor>();
}


}  // namespace zlib
