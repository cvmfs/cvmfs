/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_UTIL_H_
#define CVMFS_COMPRESSION_UTIL_H_

#include <stdio.h>

#include <string>

namespace zip {

enum StreamState {
  kStreamDataError = 0,
  kStreamIOError,
  kStreamContinue,
  kStreamEnd,
  kStreamError,
  kStreamOutBufFull,  // output buffer full: needs handling but no fatal error
};
typedef StreamState StreamStates;

// Do not change order of algorithms. Used as flags in the catalog
enum Algorithm {
  kZlib = 0,
  kNoCompression,
  kZstd,
  kGuessDecompression = 1000, // for internal transient indication only
#if defined(CVMFS_COMPRESSION_DEFAULT_ZSTD)
  kDefault = kZstd,
#elif defined(CVMFS_COMPRESSION_DEFAULT_ZLIB)
  kDefault = kZlib,
#elif defined(CVMFS_COMPRESSION_DEFAULT_NONE)
  kDefault = kNoCompression,
#else
#error "Define CVMFS_COMPRESSION_DEFAULT_(something)"
#endif
};
typedef Algorithm Algorithms;
typedef Algorithm DecompressionAlg;

/**
 * Aborts if string doesn't match any of the algorithms.
 */
Algorithms ParseCompressionAlgorithm(const std::string &algorithm_option);


std::string AlgorithmName(const zip::Algorithms alg);

Algorithm DecompressionAlgFromEnv();

Algorithm CompressionAlgFromEnv();

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_UTIL_H_
