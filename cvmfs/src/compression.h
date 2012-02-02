/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_H_
#define CVMFS_COMPRESSION_H_

#include <stdio.h>
#include <stdint.h>
#include "zlib-duplex.h"

namespace hash {
struct t_sha1;
}

bool CopyPath2Path(const char *src, const char *dest);

namespace zlib {

enum StreamStates {
  kStreamError = 0,
  kStreamContinue,
  kStreamEnd,
};

bool CompressInit(z_stream *strm);
bool DecompressInit(z_stream *strm);
void CompressFini(z_stream *strm);
void DecompressFini(z_stream *strm);

StreamStates DecompressZStream2File(z_stream *strm, FILE *f, const void *buf,
                                    const int64_t size);

bool CompressPath2Path(const char *src, const char *dest);
bool CompressPath2Path(const char *src, const char *dest,
                       hash::t_sha1 *compressed_hash);
bool DecompressPath2Path(const char *src, const char *dest);

bool CompressFile2Null(FILE *fsrc, hash::t_sha1 *compressed_hash);
bool CompressFile2File(FILE *fsrc, FILE *fdest);
bool CompressFile2File(FILE *fsrc, FILE *fdest, hash::t_sha1 *compressed_hash);
bool DecompressFile2File(FILE *fsrc, FILE *fdest);

// User of these functions has to free out_buf, if successful
bool CompressMem2Mem(const void *buf, const int64_t size,
                     void **out_buf, int64_t *out_size);
bool DecompressMem2Mem(const void *buf, const int64_t size,
                       void **out_buf, int64_t *out_size);

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_H_
