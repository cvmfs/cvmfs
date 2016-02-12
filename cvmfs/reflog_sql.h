/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_REFLOG_SQL_H_
#define CVMFS_REFLOG_SQL_H_

#include "sql.h"

class ReflogDatabase : public sqlite::Database<ReflogDatabase> {
 public:
  static const float kLatestSchema;
  static const float kLatestSupportedSchema;
  // backwards-compatible schema changes
  static const unsigned kLatestSchemaRevision;

  static const std::string kFqrnKey;

  bool CreateEmptyDatabase();

  bool CheckSchemaCompatibility();
  bool LiveSchemaUpgradeIfNecessary();
  bool CompactDatabase() const { return true; }  // no implementation specific
                                                 // database compaction.

  bool InsertInitialValues(const std::string &repo_name);

 protected:
  // TODO(rmeusel): C++11 - constructor inheritance
  friend class sqlite::Database<ReflogDatabase>;
  ReflogDatabase(const std::string  &filename,
                 const OpenMode      open_mode) :
    sqlite::Database<ReflogDatabase>(filename, open_mode) {}
};

#endif /* CVMFS_REFLOG_SQL_H_ */
