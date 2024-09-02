/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_COMPRESSION_UTIL_H_
#define CVMFS_COMPRESSION_UTIL_H_

#include <stdio.h>

#include <string>

namespace zlib {

enum StreamStates {
  kStreamDataError = 0,
  kStreamIOError,
  kStreamContinue,
  kStreamEnd,
  kStreamError,
};

// Do not change order of algorithms. Used as flags in the catalog
enum Algorithms {
  kZlibDefault = 0,
  kNoCompression,
};

const unsigned kZChunk = 16384;

/**
 * Aborts if string doesn't match any of the algorithms.
 */
Algorithms ParseCompressionAlgorithm(const std::string &algorithm_option);


std::string AlgorithmName(const zlib::Algorithms alg);

}  // namespace zlib

#endif  // CVMFS_COMPRESSION_UTIL_H_
