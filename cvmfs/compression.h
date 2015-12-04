/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_H_
#define CVMFS_COMPRESSION_H_

#include <errno.h>
#include <stdint.h>
#include <stdio.h>

#include <string>

#include "duplex_zlib.h"
#include "sink.h"

namespace shash {
struct Any;
class ContextPtr;
}

bool CopyPath2Path(const std::string &src, const std::string &dest);
bool CopyMem2Path(const unsigned char *buffer, const unsigned buffer_size,
                  const std::string &path);
bool CopyMem2File(const unsigned char *buffer, const unsigned buffer_size,
                  FILE *fdest);
bool CopyPath2Mem(const std::string &path,
                  unsigned char **buffer, unsigned *buffer_size);

namespace zlib {

enum StreamStates {
  kStreamDataError = 0,
  kStreamIOError,
  kStreamContinue,
  kStreamEnd,
};


void CompressInit(z_stream *strm);
void DecompressInit(z_stream *strm);
void CompressFini(z_stream *strm);
void DecompressFini(z_stream *strm);

StreamStates CompressZStream2Null(
  const void *buf, const int64_t size, const bool eof,
  z_stream *strm, shash::ContextPtr *hash_context);
StreamStates DecompressZStream2File(const void *buf, const int64_t size,
                                    z_stream *strm, FILE *f);
StreamStates DecompressZStream2Sink(const void *buf, const int64_t size,
                                    z_stream *strm, cvmfs::Sink *sink);

bool CompressPath2Path(const std::string &src, const std::string &dest);
bool CompressPath2Path(const std::string &src, const std::string &dest,
                       shash::Any *compressed_hash);
bool DecompressPath2Path(const std::string &src, const std::string &dest);

bool CompressFile2Null(FILE *fsrc, shash::Any *compressed_hash);
bool CompressFd2Null(int fd_src, shash::Any *compressed_hash,
                     off_t* size = NULL);
bool CompressFile2File(FILE *fsrc, FILE *fdest);
bool CompressFile2File(FILE *fsrc, FILE *fdest, shash::Any *compressed_hash);
bool CompressPath2File(const std::string &src, FILE *fdest,
                       shash::Any *compressed_hash);
bool DecompressFile2File(FILE *fsrc, FILE *fdest);
bool DecompressPath2File(const std::string &src, FILE *fdest);

bool CompressMem2File(const unsigned char *buf, const size_t size,
                      FILE *fdest, shash::Any *compressed_hash);

// User of these functions has to free out_buf, if successful
bool CompressMem2Mem(const void *buf, const int64_t size,
                     void **out_buf, uint64_t *out_size);
bool DecompressMem2Mem(const void *buf, const int64_t size,
                       void **out_buf, uint64_t *out_size);

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_H_
