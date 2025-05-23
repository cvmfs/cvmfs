/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_SPOOLER_DEFINITION_H_
#define CVMFS_UPLOAD_SPOOLER_DEFINITION_H_

#include <string>

#include "compression/compression.h"
#include "crypto/hash.h"

namespace upload {

/**
 * SpoolerDefinition is given by a string of the form:
 * <spooler type>:<spooler description>
 *
 * F.e: local:/srv/cvmfs/dev.cern.ch
 *      to define a local spooler with upstream path /srv/cvmfs/dev.cern.ch
 */
struct SpoolerDefinition {
  static const unsigned kDefaultMaxConcurrentUploads = 512;
  static const unsigned kDefaultNumUploadTasks = 1;
  static const char *kDriverNames[];  ///< corresponds to DriverType
  enum DriverType {
    S3,
    Local,
    Gateway,
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
  SpoolerDefinition(
      const std::string &definition_string,
      const shash::Algorithms hash_algorithm,
      const zlib::Algorithms compression_algorithm = zlib::kZlibDefault,
      const bool generate_legacy_bulk_chunks = false,
      const bool use_file_chunking = false,
      const size_t min_file_chunk_size = 0,
      const size_t avg_file_chunk_size = 0,
      const size_t max_file_chunk_size = 0,
      const std::string &session_token_file = "",
      const std::string &key_file = "");

  bool IsValid() const { return valid_; }

  /**
   * Creates a new SpoolerDefinition based on an existing one.  The new spooler
   * has compression set to zlib, which is required for catalogs and other meta-
   * objects.
   */
  SpoolerDefinition Dup2DefaultCompression() const;

  DriverType driver_type;      //!< the type of the spooler driver
  std::string temporary_path;  //!< scratch space for the IngestionPipeline

  /**
   * A driver specific spooler configuration string (interpreted by the concrete
   * spooler)
   */
  std::string spooler_configuration;

  shash::Algorithms hash_algorithm;
  zlib::Algorithms compression_alg;
  /**
   * If a file is chunked, clients >= 2.1.7 do not need the bulk chunk.  We can
   * force creating the bulk chunks for backwards compatibility.
   */
  bool generate_legacy_bulk_chunks;
  bool use_file_chunking;
  size_t min_file_chunk_size;
  size_t avg_file_chunk_size;
  size_t max_file_chunk_size;

  /**
   * This is the number of concurrently open files to be uploaded. It does not,
   * however, specify the degree of parallelism of the I/O write operations.
   */
  unsigned int number_of_concurrent_uploads;

  /**
   * Number of threads used for I/O write calls. Effectively this parameter
   * sets the I/O depth.
   */
  unsigned int num_upload_tasks;

  // The session_token_file parameter is only used for the HTTP driver
  std::string session_token_file;
  std::string key_file;

  bool valid_;
};

}  // namespace upload

#endif  // CVMFS_UPLOAD_SPOOLER_DEFINITION_H_
