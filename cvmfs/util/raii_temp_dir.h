/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_RAII_TEMP_DIR_H_
#define CVMFS_UTIL_RAII_TEMP_DIR_H_

#include <string>

#include "util/export.h"

/**
 * RAII based temporary directory helper class
 *
 * Instances of the class create a temporary directory which is then
 * deleted by the destructor of the class.
 *
 * The temporary directory is created using the given prefix string
 * ("prefix.<UNIQUE_SUFFIX>")
 */
class CVMFS_EXPORT RaiiTempDir {
 public:
  static RaiiTempDir *Create(const std::string &prefix);

  std::string dir() const { return dir_; }

  ~RaiiTempDir();

 private:
  explicit RaiiTempDir(const std::string &prefix);

  std::string dir_;
};

#endif  // CVMFS_UTIL_RAII_TEMP_DIR_H_
