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
#include "util.h"

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

const unsigned kZChunk = 16384;

enum StreamStates {
  kStreamDataError = 0,
  kStreamIOError,
  kStreamContinue,
  kStreamEnd,
};

// Do not change order of algorithms.  Used as flags in the catalog
enum Algorithms {
  kZlibDefault = 0,
  kNoCompression,
};

/**
 * Abstract Compression class which is inherited by implementations of
 * compression engines such as zlib.
 *
 * In order to add a new compression method, you simply need to add a new class
 * which is a sub-class of the Compressor.  The subclass needs to implement the
 * Deflate, DeflateBound, Clone, and WillHandle functions.  For information on
 * the WillHandle function, read up on the PolymorphicConstruction class.
 * The new sub-class must be listed in the implemention of the
 * Compressor::RegisterPlugins function.
 *
 */
class Compressor: public PolymorphicConstruction<Compressor, Algorithms> {
 public:
  explicit Compressor(const Algorithms &alg) { }
  virtual ~Compressor() { }
  /**
   * Deflate function.  The arguments and returns closely match the input and
   * output of the zlib deflate function.
   * Input:
   *   - outbuf - Ouput buffer to write the compressed data.
   *   - outbufsize - Size of the output buffer
   *   - inbuf - Input data to be compressed
   *   - inbufsize - Size of the input buffer
   *   - flush - Whether the compression stream should be flushed / finished
   * Upon return:
   *   returns: true - if done compressing, false otherwise
   *   - outbuf - output buffer pointer (unchanged from input)
   *   - outbufsize - The number of bytes used in the outbuf
   *   - inbuf - Pointer to the next byte of input to read in
   *   - inbufsize - the remaining bytes of input to read in.
   *   - flush - unchanged from input
   */
  virtual bool Deflate(const bool flush,
                       unsigned char **inbuf, size_t *inbufsize,
                       unsigned char **outbuf, size_t *outbufsize) = 0;

  /**
   * Return an upper bound on the number of bytes required in order to compress
   * an input number of bytes.
   * Returns: Upper bound on the number of bytes required to compress.
   */
  virtual size_t DeflateBound(const size_t bytes) = 0;
  virtual Compressor* Clone() = 0;

  static void RegisterPlugins();
};


class ZlibCompressor: public Compressor {
 public:
  explicit ZlibCompressor(const Algorithms &alg);
  explicit ZlibCompressor(const ZlibCompressor &other);
  ~ZlibCompressor();

  bool Deflate(const bool flush,
               unsigned char **inbuf, size_t *inbufsize,
               unsigned char **outbuf, size_t *outbufsize);
  size_t DeflateBound(const size_t bytes);
  Compressor* Clone();
  static bool WillHandle(const zlib::Algorithms &alg);

 private:
  z_stream stream_;
};


class EchoCompressor: public Compressor {
 public:
  explicit EchoCompressor(const Algorithms &alg);
  bool Deflate(const bool flush,
               unsigned char **inbuf, size_t *inbufsize,
               unsigned char **outbuf, size_t *outbufsize);
  size_t DeflateBound(const size_t bytes);
  Compressor* Clone();
  static bool WillHandle(const zlib::Algorithms &alg);
};


Algorithms ParseCompressionAlgorithm(const std::string &algorithm_option);
std::string AlgorithmName(const zlib::Algorithms alg);


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
                     uint64_t* size = NULL);
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
