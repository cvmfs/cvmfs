/**
 * This file is part of the CernVM File System.
 */


#include "compression/util.h"

#include <stdio.h>

#include "util/exception.h"

namespace zlib {

/**
 * Aborts if string doesn't match any of the algorithms.
 */
Algorithms ParseCompressionAlgorithm(const std::string &algorithm_option) {
  if ((algorithm_option == "default") || (algorithm_option == "zlib"))
    return kZlibDefault;
  if (algorithm_option == "zstd")
    return kZstdDefault;
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
    case kZstdDefault:
      return "zstd";
      break;
    // Purposely did not add a 'default' statement here: this will
    // cause the compiler to generate a warning if a new algorithm
    // is added but this function is not updated.
  }
  return "unknown";
}

}  // namespace zlib

