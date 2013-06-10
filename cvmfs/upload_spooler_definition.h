/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_SPOOLER_DEFINITION_
#define CVMFS_UPLOAD_SPOOLER_DEFINITION_

#include <string>

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
    Riak,
    Local,
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
  explicit SpoolerDefinition(const std::string  &definition_string,
                             const bool          use_file_chunking   = false,
                             const size_t        min_file_chunk_size = 0,
                             const size_t        avg_file_chunk_size = 0,
                             const size_t        max_file_chunk_size = 0);
  bool IsValid() const { return valid_; }

  DriverType  driver_type;           //!< the type of the spooler driver
  std::string temporary_path;        //!< scratch space for the FileProcessor
  std::string spooler_configuration; //!< a driver specific spooler
                                     //!<  configuration string
                                     //!<  (interpreted by the concrete spooler)
  bool        use_file_chunking;
  size_t      min_file_chunk_size;
  size_t      avg_file_chunk_size;
  size_t      max_file_chunk_size;

  bool valid_;
};

}

#endif /* CVMFS_UPLOAD_SPOOLER_DEFINITION_ */
