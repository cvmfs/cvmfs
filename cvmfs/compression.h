/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_H_
#define CVMFS_COMPRESSION_H_

#include <stdio.h>
#include <stdint.h>

#include <string>

#include "duplex_zlib.h"

namespace hash {
  struct Any;
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
  kStreamError = 0,
  kStreamContinue,
  kStreamEnd,
};

void CompressInit(z_stream *strm);
void DecompressInit(z_stream *strm);
void CompressFini(z_stream *strm);
void DecompressFini(z_stream *strm);

StreamStates DecompressZStream2File(z_stream *strm, FILE *f, const void *buf,
                                    const int64_t size);

bool CompressPath2Path(const std::string &src, const std::string &dest);
bool CompressPath2Path(const std::string &src, const std::string &dest,
                       hash::Any *compressed_hash);
bool DecompressPath2Path(const std::string &src, const std::string &dest);

bool CompressFile2Null(FILE *fsrc, hash::Any *compressed_hash);
bool CompressFile2File(FILE *fsrc, FILE *fdest);
bool CompressFile2File(FILE *fsrc, FILE *fdest, hash::Any *compressed_hash);
bool CompressPath2File(const std::string &src, FILE *fdest,
                       hash::Any *compressed_hash);
bool CompressPath2Mem(const std::string &src, unsigned char *mem, const size_t mem_size,
                      size_t *bytes_used, hash::Any *compressed_hash);
bool CompressFile2Mem(FILE *fsrc, unsigned char *mem, const size_t mem_size,
                      size_t *bytes_used, hash::Any *compressed_hash);
bool DecompressFile2File(FILE *fsrc, FILE *fdest);

bool CompressMem2File(const unsigned char *buf, const size_t size,
                      FILE *fdest, hash::Any *compressed_hash);

// User of these functions has to free out_buf, if successful
bool CompressMem2Mem(const void *buf, const int64_t size,
                     void **out_buf, int64_t *out_size);
bool DecompressMem2Mem(const void *buf, const int64_t size,
                       void **out_buf, int64_t *out_size);

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_H_
