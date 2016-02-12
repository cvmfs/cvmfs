/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_REFLOG_H_
#define CVMFS_REFLOG_H_

#include <string>

#include "reflog_sql.h"

namespace manifest {

// TODO(rmeusel): this shares a lot of database management code with
//                SqliteHistory and (potentially) ...Catalog. This might be an
//                architectural weakness and should be cleaned up.
class Reflog {
 public:
  static Reflog* Open(const std::string &database_path);
  static Reflog* Create(const std::string &database_path,
                        const std::string &repo_name);

  void TakeDatabaseFileOwnership() {}
  void DropDatabaseFileOwnership() {}
  bool OwnsDatabaseFile() const    { return false; }

  std::string fqrn() const;

 private:
  bool CreateDatabase(const std::string &database_path,
                      const std::string &repo_name);
  bool OpenDatabase(const std::string &database_path);

 private:
  UniquePtr<ReflogDatabase> database_;
};

} // namespace manifest

#endif /* CVMFS_REFLOG_H_ */
