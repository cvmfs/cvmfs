/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_SPOOLER_DEFINITION_H_
#define CVMFS_UPLOAD_SPOOLER_DEFINITION_H_

#include <string>

#include "compression.h"
#include "hash.h"

namespace upload {

/**
 * SpoolerDefinition is given by a string of the form:
 * <spooler type>:<spooler description>
 *
 * F.e: local:/srv/cvmfs/dev.cern.ch
 *      to define a local spooler with upstream path /srv/cvmfs/dev.cern.ch
 */
struct SpoolerDefinition {
  enum DriverType {
    S3,
    Local,
    Mock,
    Unknown
  };

  /**
   * Reads a given definition_string as described above and interprets
   * it. If the provided string turns out to be malformed the created
   * SpoolerDefinition object will not be valid. A user should check this
   * after creation using IsValid().
   *
   * @param definition_string   the spooler definition string to be inter-
   *                            preted by the constructor
   */
  explicit SpoolerDefinition(
    const std::string       &definition_string,
    const shash::Algorithms  hash_algorithm,
    const zlib::Algorithms   compression_algorithm = zlib::kZlibDefault,
    const bool               use_file_chunking   = false,
    const size_t             min_file_chunk_size = 0,
    const size_t             avg_file_chunk_size = 0,
    const size_t             max_file_chunk_size = 0);
  bool IsValid() const { return valid_; }

  DriverType  driver_type;            //!< the type of the spooler driver
  std::string temporary_path;         //!< scratch space for the FileProcessor
  /**
   * A driver specific spooler configuration string (interpreted by the concrete
   * spooler)
   */
  std::string spooler_configuration;

  shash::Algorithms  hash_algorithm;
  zlib::Algorithms   compression_alg;
  bool               use_file_chunking;
  size_t             min_file_chunk_size;
  size_t             avg_file_chunk_size;
  size_t             max_file_chunk_size;

  const unsigned int number_of_threads;
  unsigned int       number_of_concurrent_uploads;

  bool valid_;
};

}  // namespace upload

#endif  // CVMFS_UPLOAD_SPOOLER_DEFINITION_H_
