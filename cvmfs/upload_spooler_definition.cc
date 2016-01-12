/**
 * This file is part of the CernVM File System.
 */

#include "upload_spooler_definition.h"

#include <tbb/task_scheduler_init.h>
#include <vector>

#include "logging.h"
#include "util.h"

namespace upload {

SpoolerDefinition::SpoolerDefinition(
                      const std::string&       definition_string,
                      const shash::Algorithms  hash_algorithm,
                      const zlib::Algorithms   compression_algorithm,
                      const bool               use_file_chunking,
                      const size_t             min_file_chunk_size,
                      const size_t             avg_file_chunk_size,
                      const size_t             max_file_chunk_size) :
  driver_type(Unknown),
  hash_algorithm(hash_algorithm),
  compression_alg(compression_algorithm),
  use_file_chunking(use_file_chunking),
  min_file_chunk_size(min_file_chunk_size),
  avg_file_chunk_size(avg_file_chunk_size),
  max_file_chunk_size(max_file_chunk_size),
  number_of_threads(tbb::task_scheduler_init::default_num_threads()),
  number_of_concurrent_uploads(number_of_threads * 100),
  valid_(false)
{
  // check if given file chunking values are sane
  if (use_file_chunking && (min_file_chunk_size >= avg_file_chunk_size ||
                            avg_file_chunk_size >= max_file_chunk_size)) {
    LogCvmfs(kLogSpooler, kLogStderr, "file chunk size values are not sane");
    return;
  }

  // split the spooler driver definition into name and config part
  std::vector<std::string> upstream = SplitString(definition_string, ',');
  if (upstream.size() != 3) {
    LogCvmfs(kLogSpooler, kLogStderr, "Invalid spooler driver");
    return;
  }

  // recognize and configure the spooler driver
  if (upstream[0]        == "local") {
    driver_type = Local;
  } else if (upstream[0] == "S3") {
    driver_type = S3;
  } else if (upstream[0] == "mock") {
    driver_type = Mock;  // for unit testing purpose only!
  } else {
    driver_type = Unknown;
    LogCvmfs(kLogSpooler, kLogStderr, "unknown spooler driver: %s",
      upstream[0].c_str());
    return;
  }

  // save data
  temporary_path        = upstream[1];
  spooler_configuration = upstream[2];
  valid_ = true;
}

}  // namespace upload
