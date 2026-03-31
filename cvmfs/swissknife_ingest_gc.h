/**
 * This file is part of the CernVM File System.
 *
 * GC database helper functions for cvmfs_swissknife ingest.
 * These read/update the SQLite database produced by cvmfs_ducc gc.
 */

#ifndef CVMFS_SWISSKNIFE_INGEST_GC_H_
#define CVMFS_SWISSKNIFE_INGEST_GC_H_

#include <string>
#include <vector>

#include "duplex_sqlite3.h"
#include "util/logging.h"

/**
 * Read all pending (not yet deleted) paths from a GC SQLite database.
 * Returns true on success.
 */
inline bool ReadGCDatabase(const std::string &db_path,
                           std::vector<std::string> *paths) {
  sqlite3 *db = NULL;
  int rc = sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READONLY, NULL);
  if (rc != SQLITE_OK) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Cannot open GC database %s: %s",
             db_path.c_str(), sqlite3_errmsg(db));
    if (db) sqlite3_close(db);
    return false;
  }

  const char *sql = "SELECT path FROM gc_paths WHERE deleted = 0 ORDER BY id";
  sqlite3_stmt *stmt = NULL;
  rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
  if (rc != SQLITE_OK) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Cannot prepare GC query on %s: %s",
             db_path.c_str(), sqlite3_errmsg(db));
    sqlite3_close(db);
    return false;
  }

  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    const char *path =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
    if (path) {
      paths->push_back(std::string(path));
    }
  }

  sqlite3_finalize(stmt);
  if (rc != SQLITE_DONE) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Error reading GC database %s: %s",
             db_path.c_str(), sqlite3_errmsg(db));
    sqlite3_close(db);
    return false;
  }

  sqlite3_close(db);
  return true;
}


/**
 * Mark all pending paths as deleted in the GC SQLite database.
 * Returns true on success.
 */
inline bool MarkGCPathsDeleted(const std::string &db_path) {
  sqlite3 *db = NULL;
  int rc = sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READWRITE, NULL);
  if (rc != SQLITE_OK) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Cannot open GC database for update %s: %s",
             db_path.c_str(), sqlite3_errmsg(db));
    if (db) sqlite3_close(db);
    return false;
  }

  const char *sql = "UPDATE gc_paths SET deleted = 1 WHERE deleted = 0";
  char *err_msg = NULL;
  rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
  if (rc != SQLITE_OK) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Cannot mark GC paths as deleted in %s: %s",
             db_path.c_str(), err_msg ? err_msg : "unknown error");
    sqlite3_free(err_msg);
    sqlite3_close(db);
    return false;
  }

  sqlite3_close(db);
  return true;
}

#endif  // CVMFS_SWISSKNIFE_INGEST_GC_H_
