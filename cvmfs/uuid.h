/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UUID_H_
#define CVMFS_UUID_H_

#include <string>

namespace cvmfs {

/**
 * Holds a unique identifies which is either read from a file or, if the file
 * does not yet exist, created and stored in a file.
 */
class Uuid {
 public:
  static Uuid *Create(const std::string &store_path);
  std::string uuid() { return uuid_; }
 private:
  void MkUuid();
  Uuid() : uuid_("") { }

  std::string uuid_;
};

}  // namespace cvmfs

#endif  // CVMFS_UTIL_H_
