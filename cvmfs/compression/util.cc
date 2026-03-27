/**
 * This file is part of the CernVM File System.
 */


#include "compression/util.h"

#include <stdio.h>

#include "util/exception.h"

namespace zip {

/**
 * Aborts if string doesn't match any of the algorithms.
 */
Algorithms ParseCompressionAlgorithm(const std::string &algorithm_option) {
  if (algorithm_option == "default")
    return kDefault;
  if (algorithm_option == "zlib")
    return kZlib;
  if (algorithm_option == "zstd")
    return kZstd;
  if (algorithm_option == "none")
    return kNoCompression;
#ifdef CVMFS_GUESS_DECOMPRESSOR
  if (algorithm_option == "guess")
    return zip::Algorithm::kGuessDecompression;
#endif
  PANIC(kLogStderr, "unknown compression algorithms: %s",
        algorithm_option.c_str());
}


std::string AlgorithmName(const zip::Algorithms alg) {
  switch (alg) {
    case kZlib:
      return "zlib";
      break;
    case kNoCompression:
      return "none";
      break;
    case kZstd:
      return "zstd";
      break;
#ifdef CVMFS_GUESS_DECOMPRESSOR
    case zip::Algorithm::kGuessDecompression:
      return "guess";
      break;
#endif
    // Purposely did not add a 'default' statement here: this will
    // cause the compiler to generate a warning if a new algorithm
    // is added but this function is not updated.
  }
  return "unknown";
}

Algorithm DecompressionAlgFromEnv() {
  const char *var = getenv("CVMFS_DECOMPRESSION_ALGORITHM");
  if (!var || !var[0]) {
    var = getenv("CVMFS_COMPRESSION_ALGORITHM");
  }
  if (!var || !var[0]) {
    var = "default";
  }
  return zip::ParseCompressionAlgorithm(var);
}

Algorithm CompressionAlgFromEnv() {
  const char *var = getenv("CVMFS_COMPRESSION_ALGORITHM");
  if (!var || !var[0]) {
    var = "default";
  }
  return zip::ParseCompressionAlgorithm(var);
}

}  // namespace zlib

