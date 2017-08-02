/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_TEMP_DIR_H_
#define CVMFS_UTIL_TEMP_DIR_H_

#include <string>

/**
 * RAII based temporary directory helper class
 *
 * Instances of the class create a temporary directory which is then
 * deleted by the destructor of the class.
 *
 * The temporary directory is created using the given prefix string
 * ("prefix.<UNIQUE_SUFFIX>")
 */
class TempDir {
 public:
  static TempDir* Create(const std::string& prefix);

  std::string dir() const { return dir_; }

  ~TempDir();

private:
  TempDir(const std::string& prefix);

  std::string dir_;
};

#endif  // CVMFS_UTIL_TEMP_DIR_H_
