/**
 * This file is part of the CernVM File System.
 */

#include "upload_spooler_definition.h"

#include <vector>

#include "logging.h"
#include "util.h"

using namespace upload;

SpoolerDefinition::SpoolerDefinition(
                      const std::string& definition_string,
                      const bool         use_file_chunking,
                      const size_t       min_file_chunk_size,
                      const size_t       avg_file_chunk_size,
                      const size_t       max_file_chunk_size) :
  driver_type(Unknown),
  use_file_chunking(use_file_chunking),
  min_file_chunk_size(min_file_chunk_size),
  avg_file_chunk_size(avg_file_chunk_size),
  max_file_chunk_size(max_file_chunk_size),
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
  } else if (upstream[0] == "riak") {
    driver_type = Riak;
  } else if (upstream[0] == "mock") {
    driver_type = Unknown; // for unit testing purpose only!
  } else {
    LogCvmfs(kLogSpooler, kLogStderr, "unknown spooler driver: %s",
      upstream[0].c_str());
    return;
  }

  // save data
  temporary_path        = upstream[1];
  spooler_configuration = upstream[2];
  valid_ = true;
}
