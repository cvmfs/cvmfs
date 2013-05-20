/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_HISTORY_H_
#define CVMFS_HISTORY_H_

#include <string>
#include "sql.h"

namespace history {

class Database {
 public:
  static const float kLatestSchema;
  static const float kLatestSupportedSchema;
  static const float kSchemaEpsilon;  // floats get imprecise in SQlite

  Database(const std::string filename, const sqlite::DbOpenMode open_mode);
  ~Database();
  static bool Create(const std::string &filename);

  sqlite3 *sqlite_db() const { return sqlite_db_; }
  std::string filename() const { return filename_; }
  float schema_version() const { return schema_version_; }
  bool ready() const { return ready_; }

  std::string GetLastErrorMsg() const;
 private:
  Database(sqlite3 *sqlite_db, const float schema, const bool rw);

  sqlite3 *sqlite_db_;
  std::string filename_;
  float schema_version_;
  bool read_write_;
  bool ready_;
};

}  // namespace hsitory

#endif  // CVMFS_HISTORY_H_
