/**
 * This file is part of the CernVM File System.
 *
 * Rotates the statistics database for a CernVM-FS repository. This is
 * intended to be called from logrotate instead of the system sqlite3 binary,
 * so that the vendored SQLite (which supports VACUUM INTO) is used regardless
 * of the system SQLite version.
 */

#include "swissknife_rotate_statsdb.h"

#include <string>
#include <unistd.h>

#include "sql.h"
#include "statistics_database.h"
#include "util/logging.h"
#include "util/posix.h"

namespace swissknife {

ParameterList CommandRotateStatsDB::GetParams() const {
  ParameterList r;
  r.push_back(Parameter::Mandatory('p', "path to the stats.db file"));
  return r;
}

int CommandRotateStatsDB::Main(const ArgumentList &args) {
  const std::string db_path = *args.find('p')->second;
  const std::string archive_path = db_path + ".archive";

  if (!FileExists(db_path)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "stats.db not found: %s",
             db_path.c_str());
    return 1;
  }

  // Remove any existing archive so VACUUM INTO can write a fresh one
  if (FileExists(archive_path)) {
    if (unlink(archive_path.c_str()) != 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to remove existing archive: %s",
               archive_path.c_str());
      return 1;
    }
  }

  StatisticsDatabase *db =
      StatisticsDatabase::Open(db_path, StatisticsDatabase::kOpenReadWrite);
  if (db == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to open statistics database: %s",
             db_path.c_str());
    return 1;
  }

  // VACUUM INTO creates a compacted copy of the current database
  const std::string vacuum_into =
      "VACUUM INTO '" + archive_path + "';";
  if (!sqlite::Sql(db->sqlite_db(), vacuum_into).Execute()) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "VACUUM INTO failed for %s: %s",
             db_path.c_str(), db->GetLastErrorMsg().c_str());
    delete db;
    return 1;
  }

  // Clear all rows from the live database
  if (!sqlite::Sql(db->sqlite_db(),
                   "DELETE FROM publish_statistics;").Execute()) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "failed to clear publish_statistics in %s: %s",
             db_path.c_str(), db->GetLastErrorMsg().c_str());
    delete db;
    return 1;
  }
  if (!sqlite::Sql(db->sqlite_db(),
                   "DELETE FROM gc_statistics;").Execute()) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "failed to clear gc_statistics in %s: %s",
             db_path.c_str(), db->GetLastErrorMsg().c_str());
    delete db;
    return 1;
  }

  // Reclaim space freed by the DELETEs
  if (!db->Vacuum()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "VACUUM failed for %s: %s",
             db_path.c_str(), db->GetLastErrorMsg().c_str());
    delete db;
    return 1;
  }

  LogCvmfs(kLogCvmfs, kLogStdout,
           "Rotated statistics database: %s -> %s",
           db_path.c_str(), archive_path.c_str());
  delete db;
  return 0;
}

}  // namespace swissknife
