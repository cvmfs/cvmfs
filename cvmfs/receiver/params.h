/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_RECEIVER_PARAMS_H_
#define CVMFS_RECEIVER_PARAMS_H_

#include <string>

#include "compression.h"
#include "hash.h"

namespace receiver {

struct Params {
  std::string spooler_configuration;
  shash::Algorithms hash_alg;
  zlib::Algorithms compression_alg;
  bool use_file_chunking;
  size_t min_chunk_size;
  size_t avg_chunk_size;
  size_t max_chunk_size;
  size_t entry_warn_thresh;
  bool use_autocatalogs;
  size_t max_weight;
  size_t min_weight;
};

bool GetParamsFromFile(const std::string& repo_name, Params* params);

}  // namespace receiver

#endif  // CVMFS_RECEIVER_PARAMS_H_
