/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_UTIL_H_
#define CVMFS_COMPRESSION_UTIL_H_

#include <stdio.h>

#include <string>

namespace zip {

enum StreamStates {
  kStreamDataError = 0,
  kStreamIOError,
  kStreamContinue,
  kStreamEnd,
  kStreamError,
  kStreamOutBufFull,  // output buffer full: needs handling but no fatal error
};

// Do not change order of algorithms. Used as flags in the catalog
enum Algorithms {
  kZlib = 0,
  kNoCompression,
  kZstd,
  kZlibDefault = kZlib,
  kDefault = kZlib,
};

/**
 * Aborts if string doesn't match any of the algorithms.
 */
Algorithms ParseCompressionAlgorithm(const std::string &algorithm_option);


std::string AlgorithmName(const zip::Algorithms alg);

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_UTIL_H_
