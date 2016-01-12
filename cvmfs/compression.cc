/**
 * This file is part of the CernVM File System.
 *
 * This is a wrapper around zlib.  It provides
 * a set of functions to conveniently compress and decompress stuff.
 * Allmost all of the functions return true on success, otherwise false.
 *
 * TODO: think about code deduplication
 */

#include "cvmfs_config.h"
#include "compression.h"

#include <alloca.h>
#include <stdlib.h>
#include <sys/stat.h>

#include <algorithm>
#include <cassert>
#include <cstring>

#include "hash.h"
#include "logging.h"
#include "platform.h"
#include "smalloc.h"
#include "util.h"

using namespace std;  // NOLINT


static bool CopyFile2File(FILE *fsrc, FILE *fdest) {
  unsigned char buf[1024];
  rewind(fsrc);
  rewind(fdest);

  size_t have;
  do {
    have = fread(buf, 1, 1024, fsrc);
    if (fwrite(buf, 1, have, fdest) != have)
      return false;
  } while (have == 1024);
  return true;
}


bool CopyPath2Path(const string &src, const string &dest) {
  FILE *fsrc = NULL;
  FILE *fdest = NULL;
  int retval = -1;
  struct stat info;

  fsrc = fopen(src.c_str(), "r");
  if (!fsrc) goto file_copy_final;

  fdest = fopen(dest.c_str(), "w");
  if (!fdest) goto file_copy_final;

  if (!CopyFile2File(fsrc, fdest)) goto file_copy_final;
  retval = fstat(fileno(fsrc), &info);
  retval |= fchmod(fileno(fdest), info.st_mode);

 file_copy_final:
  if (fsrc) fclose(fsrc);
  if (fdest) fclose(fdest);
  return retval == 0;
}


bool CopyMem2File(const unsigned char *buffer, const unsigned buffer_size,
                  FILE *fdest)
{
  int written = fwrite(buffer, 1, buffer_size, fdest);
  return (written >=0) && (unsigned(written) == buffer_size);
}


bool CopyMem2Path(const unsigned char *buffer, const unsigned buffer_size,
                  const string &path)
{
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, kDefaultFileMode);
  if (fd < 0)
    return false;

  int written = write(fd, buffer, buffer_size);
  close(fd);

  return (written >=0) && (unsigned(written) == buffer_size);
}


bool CopyPath2Mem(const string &path,
                  unsigned char **buffer, unsigned *buffer_size)
{
  const int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0)
    return false;

  *buffer_size = 512;
  *buffer = reinterpret_cast<unsigned char *>(smalloc(*buffer_size));
  unsigned total_bytes = 0;
  while (true) {
    int num_bytes = read(fd, *buffer + total_bytes, *buffer_size - total_bytes);
    if (num_bytes == 0)
      break;
    if (num_bytes < 0) {
      close(fd);
      free(*buffer);
      *buffer_size = 0;
      return false;
    }
    total_bytes += num_bytes;
    if (total_bytes >= *buffer_size) {
      *buffer_size *= 2;
      *buffer =
        reinterpret_cast<unsigned char *>(srealloc(*buffer, *buffer_size));
    }
  }

  close(fd);
  *buffer_size = total_bytes;
  return true;
}

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
  LogCvmfs(kLogCompress, kLogStderr, "unknown compression algorithms: %s",
           algorithm_option.c_str());
  assert(false);
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


StreamStates CompressZStream2Null(
  const void *buf,
  const int64_t size,
  const bool eof,
  z_stream *strm,
  shash::ContextPtr *hash_context)
{
  unsigned char out[kZChunk];
  int z_ret;

  strm->avail_in = size;
  strm->next_in = static_cast<unsigned char *>(const_cast<void *>(buf));
  // Run deflate() on input until output buffer not full, finish
  // compression if all of source has been read in
  do {
    strm->avail_out = kZChunk;
    strm->next_out = out;
    z_ret = deflate(strm, eof ? Z_FINISH : Z_NO_FLUSH);  // no bad return value
    if (z_ret == Z_STREAM_ERROR)
      return kStreamDataError;
    size_t have = kZChunk - strm->avail_out;
    shash::Update(out, have, *hash_context);
  } while (strm->avail_out == 0);

  return (z_ret == Z_STREAM_END ? kStreamEnd : kStreamContinue);
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


bool CompressPath2Path(const string &src, const string &dest) {
  FILE *fsrc = fopen(src.c_str(), "r");
  if (!fsrc) {
    LogCvmfs(kLogCompress, kLogDebug,  "open %s as compression source failed",
             src.c_str());
    return false;
  }

  FILE *fdest = fopen(dest.c_str(), "w");
  if (!fdest) {
    LogCvmfs(kLogCompress, kLogDebug, "open %s as compression destination  "
             "failed with errno=%d", dest.c_str(), errno);
    fclose(fsrc);
    return false;
  }

  LogCvmfs(kLogCompress, kLogDebug, "opened %s and %s for compression",
           src.c_str(), dest.c_str());
  const bool result = CompressFile2File(fsrc, fdest);

  fclose(fsrc);
  fclose(fdest);
  return result;
}


bool CompressPath2Path(const string &src, const string &dest,
                       shash::Any *compressed_hash)
{
  FILE *fsrc = fopen(src.c_str(), "r");
  if (!fsrc) {
    LogCvmfs(kLogCompress, kLogDebug, "open %s as compression source failed",
             src.c_str());
    return false;
  }

  FILE *fdest = fopen(dest.c_str(), "w");
  if (!fdest) {
    LogCvmfs(kLogCompress, kLogDebug, "open %s as compression destination "
             "failed with errno=%d", dest.c_str(), errno);
    fclose(fsrc);
    return false;
  }

  LogCvmfs(kLogCompress, kLogDebug, "opened %s and %s for compression",
           src.c_str(), dest.c_str());
  bool result = false;
  if (!CompressFile2File(fsrc, fdest, compressed_hash))
    goto compress_path2path_final;
  struct stat info;
  if (fstat(fileno(fsrc), &info) != 0) goto compress_path2path_final;
  // TODO(jakob): open in the right mode from the beginning
  if (fchmod(fileno(fdest), info.st_mode) != 0) goto compress_path2path_final;

  result = true;

 compress_path2path_final:
  fclose(fsrc);
  fclose(fdest);
  return result;
}


bool DecompressPath2Path(const string &src, const string &dest) {
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


bool CompressFile2Null(FILE *fsrc, shash::Any *compressed_hash) {
  int z_ret, flush;
  bool result = -1;
  unsigned have;
  z_stream strm;
  unsigned char in[kZChunk];
  unsigned char out[kZChunk];
  shash::ContextPtr hash_context(compressed_hash->algorithm);

  CompressInit(&strm);
  hash_context.buffer = alloca(hash_context.size);
  shash::Init(hash_context);

  // Compress until end of file
  do {
    strm.avail_in = fread(in, 1, kZChunk, fsrc);
    if (ferror(fsrc)) goto compress_file2null_final;

    flush = feof(fsrc) ? Z_FINISH : Z_NO_FLUSH;
    strm.next_in = in;

    // Run deflate() on input until output buffer not full, finish
    // compression if all of source has been read in
    do {
      strm.avail_out = kZChunk;
      strm.next_out = out;
      z_ret = deflate(&strm, flush);  // no bad return value
      if (z_ret == Z_STREAM_ERROR)
        goto compress_file2null_final;  // state not clobbered
      have = kZChunk - strm.avail_out;
      shash::Update(out, have, hash_context);
    } while (strm.avail_out == 0);

    // Done when last data in file processed
  } while (flush != Z_FINISH);

  // stream will be complete
  if (z_ret != Z_STREAM_END) goto compress_file2null_final;

  shash::Final(hash_context, compressed_hash);
  result = true;

  // Clean up and return
 compress_file2null_final:
  CompressFini(&strm);
  LogCvmfs(kLogCompress, kLogDebug, "file compression finished with result %d",
           result);
  return result;
}


bool CompressFd2Null(int fd_src, shash::Any *compressed_hash,
                     uint64_t *processed_bytes) {
  int z_ret, flush;
  bool result = false;
  unsigned have;
  z_stream strm;
  unsigned char in[kZChunk];
  unsigned char out[kZChunk];
  off_t cksum_bytes = 0;
  shash::ContextPtr hash_context(compressed_hash->algorithm);

  CompressInit(&strm);
  hash_context.buffer = alloca(hash_context.size);
  shash::Init(hash_context);

  // Compress until end of file
  do {
    ssize_t bytes_read = read(fd_src, in, kZChunk);
    if (bytes_read < 0) {
      if (errno == EINTR) {continue;}
      goto compress_fd2null_final;
    }
    cksum_bytes += bytes_read;
    strm.avail_in = bytes_read;

    flush = (static_cast<size_t>(bytes_read) < kZChunk) ? Z_FINISH : Z_NO_FLUSH;
    strm.next_in = in;

    // Run deflate() on input until output buffer not full, finish
    // compression if all of source has been read in
    do {
      strm.avail_out = kZChunk;
      strm.next_out = out;
      z_ret = deflate(&strm, flush);  // no bad return value
      if (z_ret == Z_STREAM_ERROR)
        goto compress_fd2null_final;  // state not clobbered
      have = kZChunk - strm.avail_out;
      shash::Update(out, have, hash_context);
    } while (strm.avail_out == 0);

    // Done when last data in file processed
  } while (flush != Z_FINISH);

  // stream will be complete
  if (z_ret != Z_STREAM_END) goto compress_fd2null_final;

  shash::Final(hash_context, compressed_hash);
  if (processed_bytes) {
    *processed_bytes = cksum_bytes;
  }
  result = true;

  // Clean up and return
 compress_fd2null_final:
  CompressFini(&strm);
  LogCvmfs(kLogCompress, kLogDebug, "file compression finished with result %d",
           result);
  return result;
}


bool CompressFile2File(FILE *fsrc, FILE *fdest) {
  int z_ret, flush;
  bool result = false;
  unsigned have;
  z_stream strm;
  unsigned char in[kZChunk];
  unsigned char out[kZChunk];

  CompressInit(&strm);

  // Compress until end of file
  do {
    strm.avail_in = fread(in, 1, kZChunk, fsrc);
    if (ferror(fsrc)) goto compress_file2file_final;

    flush = feof(fsrc) ? Z_FINISH : Z_NO_FLUSH;
    strm.next_in = in;

    // Run deflate() on input until output buffer not full, finish
    // compression if all of source has been read in
    do {
      strm.avail_out = kZChunk;
      strm.next_out = out;
      z_ret = deflate(&strm, flush);  // no bad return value
      if (z_ret == Z_STREAM_ERROR)
        goto compress_file2file_final;  // state not clobbered
      have = kZChunk - strm.avail_out;
      if (fwrite(out, 1, have, fdest) != have || ferror(fdest))
        goto compress_file2file_final;
    } while (strm.avail_out == 0);

    // Done when last data in file processed
  } while (flush != Z_FINISH);

  // stream will be complete
  if (z_ret != Z_STREAM_END) goto compress_file2file_final;

  result = true;

  // Clean up and return
 compress_file2file_final:
  CompressFini(&strm);
  LogCvmfs(kLogCompress, kLogDebug, "file compression finished with result %d",
           result);
  return result;
}

bool CompressPath2File(const string &src, FILE *fdest,
                       shash::Any *compressed_hash)
{
  FILE *fsrc = fopen(src.c_str(), "r");
  if (!fsrc)
    return false;

  bool retval = CompressFile2File(fsrc, fdest, compressed_hash);
  fclose(fsrc);
  return retval;
}


bool CompressFile2File(FILE *fsrc, FILE *fdest, shash::Any *compressed_hash) {
  int z_ret, flush;
  bool result = false;
  unsigned have;
  z_stream strm;
  unsigned char in[kZChunk];
  unsigned char out[kZChunk];
  shash::ContextPtr hash_context(compressed_hash->algorithm);

  CompressInit(&strm);
  hash_context.buffer = alloca(hash_context.size);
  shash::Init(hash_context);

  // Compress until end of file
  do {
    strm.avail_in = fread(in, 1, kZChunk, fsrc);
    if (ferror(fsrc)) goto compress_file2file_hashed_final;

    flush = feof(fsrc) ? Z_FINISH : Z_NO_FLUSH;
    strm.next_in = in;

    // Run deflate() on input until output buffer not full, finish
    // compression if all of source has been read in
    do {
      strm.avail_out = kZChunk;
      strm.next_out = out;
      z_ret = deflate(&strm, flush);  // no bad return value
      if (z_ret == Z_STREAM_ERROR)
        goto compress_file2file_hashed_final;  // state not clobbered
      have = kZChunk - strm.avail_out;
      if (fwrite(out, 1, have, fdest) != have || ferror(fdest))
        goto compress_file2file_hashed_final;
      shash::Update(out, have, hash_context);
    } while (strm.avail_out == 0);

    // Done when last data in file processed
  } while (flush != Z_FINISH);

  // Stream will be complete
  if (z_ret != Z_STREAM_END) goto compress_file2file_hashed_final;

  shash::Final(hash_context, compressed_hash);
  result = true;

  // Clean up and return
 compress_file2file_hashed_final:
  CompressFini(&strm);
  LogCvmfs(kLogCompress, kLogDebug, "file compression finished with result %d",
           result);
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


bool DecompressPath2File(const string &src, FILE *fdest) {
  FILE *fsrc = fopen(src.c_str(), "r");
  if (!fsrc)
    return false;

  bool retval = DecompressFile2File(fsrc, fdest);
  fclose(fsrc);
  return retval;
}


bool CompressMem2File(const unsigned char *buf, const size_t size,
                      FILE *fdest, shash::Any *compressed_hash) {
  int z_ret, flush;
  bool result = false;
  unsigned have;
  z_stream strm;
  size_t offset = 0;
  size_t used   = 0;
  unsigned char out[kZChunk];
  shash::ContextPtr hash_context(compressed_hash->algorithm);

  CompressInit(&strm);
  hash_context.buffer = alloca(hash_context.size);
  shash::Init(hash_context);

  // Compress the given memory buffer
  do {
    used = min((size_t)kZChunk, size - offset);
    strm.avail_in = used;

    flush = (strm.avail_in < kZChunk) ? Z_FINISH : Z_NO_FLUSH;
    strm.next_in = const_cast<unsigned char*>(buf + offset);

    // Run deflate() on input until output buffer not full, finish
    // compression if all of source has been read in
    do {
      strm.avail_out = kZChunk;
      strm.next_out = out;
      z_ret = deflate(&strm, flush);  // no bad return value
      if (z_ret == Z_STREAM_ERROR)
        goto compress_file2file_hashed_final;  // state not clobbered
      have = kZChunk - strm.avail_out;
      if (fwrite(out, 1, have, fdest) != have || ferror(fdest))
        goto compress_file2file_hashed_final;
      shash::Update(out, have, hash_context);
    } while (strm.avail_out == 0);

    offset += used;

    // Done when last data in file processed
  } while (flush != Z_FINISH);

  // Stream will be complete
  if (z_ret != Z_STREAM_END) goto compress_file2file_hashed_final;

  shash::Final(hash_context, compressed_hash);
  result = true;

  // Clean up and return
 compress_file2file_hashed_final:
  CompressFini(&strm);
  LogCvmfs(kLogCompress, kLogDebug, "file compression finished with result %d",
           result);
  return result;
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
  int z_ret;
  z_stream strm;
  int64_t pos = 0;
  uint64_t alloc_size = kZChunk;

  DecompressInit(&strm);
  *out_buf = smalloc(alloc_size);
  *out_size = 0;

  do {
    strm.avail_in = (kZChunk > (size-pos)) ? size-pos : kZChunk;
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


//------------------------------------------------------------------------------


bool ZlibCompressor::WillHandle(const zlib::Algorithms &alg) {
  return alg == kZlibDefault;
}


ZlibCompressor::ZlibCompressor(const Algorithms &alg)
  : Compressor(alg)
{
  stream_.zalloc   = Z_NULL;
  stream_.zfree    = Z_NULL;
  stream_.opaque   = Z_NULL;
  stream_.next_in  = Z_NULL;
  stream_.avail_in = 0;
  const int zlib_retval = deflateInit(&stream_, Z_DEFAULT_COMPRESSION);
  assert(zlib_retval == 0);
}


Compressor* ZlibCompressor::Clone() {
  ZlibCompressor* other = new ZlibCompressor(zlib::kZlibDefault);
  assert(stream_.avail_in == 0);
  // Delete the other stream
  int retcode = deflateEnd(&other->stream_);
  assert(retcode == Z_OK);
  retcode = deflateCopy(const_cast<z_streamp>(&other->stream_), &stream_);
  assert(retcode == Z_OK);
  return other;
}

bool ZlibCompressor::Deflate(
  const bool flush,
  unsigned char **inbuf, size_t *inbufsize,
  unsigned char **outbuf, size_t *outbufsize)
{
  // Adding compression
  stream_.avail_in = *inbufsize;
  stream_.next_in = *inbuf;
  const int flush_int = (flush) ? Z_FINISH : Z_NO_FLUSH;
  int retcode = 0;

  stream_.avail_out = *outbufsize;
  stream_.next_out = *outbuf;

  // Deflate in zlib!
  retcode = deflate(&stream_, flush_int);
  assert(retcode == Z_OK || retcode == Z_STREAM_END);

  *outbufsize -= stream_.avail_out;
  *inbuf = stream_.next_in;
  *inbufsize = stream_.avail_in;

  return (flush_int == Z_NO_FLUSH && retcode == Z_OK && stream_.avail_in == 0)
         || (flush_int == Z_FINISH  && retcode == Z_STREAM_END);
}


ZlibCompressor::~ZlibCompressor() {
  int retcode = deflateEnd(&stream_);
  assert(retcode == Z_OK);
}


size_t ZlibCompressor::DeflateBound(const size_t bytes) {
  // Call zlib's deflate bound
  return deflateBound(&stream_, bytes);
}


//------------------------------------------------------------------------------


EchoCompressor::EchoCompressor(const zlib::Algorithms &alg):
  Compressor(alg)
{
}


bool EchoCompressor::WillHandle(const zlib::Algorithms &alg) {
  return alg == kNoCompression;
}


Compressor* EchoCompressor::Clone() {
  return new EchoCompressor(zlib::kNoCompression);
}


bool EchoCompressor::Deflate(
  const bool flush,
  unsigned char **inbuf, size_t *inbufsize,
  unsigned char **outbuf, size_t *outbufsize)
{
  size_t bytes_to_copy = min(*outbufsize, *inbufsize);
  memcpy(*outbuf, *inbuf, bytes_to_copy);
  const bool done = (bytes_to_copy == *inbufsize);

  // Update the return variables
  *inbuf += bytes_to_copy;
  *outbufsize = bytes_to_copy;
  *inbufsize -= bytes_to_copy;

  return done;
}


size_t EchoCompressor::DeflateBound(const size_t bytes) {
  // zero bytes as an upper bound is no good because some callers want to
  // allocate buffers according to this value
  return (bytes == 0) ? 1 : bytes;
}

}  // namespace zlib
