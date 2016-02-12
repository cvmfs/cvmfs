/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_REFLOG_H_
#define CVMFS_REFLOG_H_

#include <string>

namespace manifest {

class Reflog {
 public:
  static Reflog* Open(const std::string &database_path) {
    return new Reflog();
  }

  void TakeDatabaseFileOwnership() {}
  void DropDatabaseFileOwnership() {}
  bool OwnsDatabaseFile() const    { return false; }
};

} // namespace manifest

#endif /* CVMFS_REFLOG_H_ */
