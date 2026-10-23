/**
 * This file is part of the CernVM File System.
 *
 * GC database helper functions for cvmfs_swissknife ingest.
 * These read/update the SQLite database produced by cvmfs_ducc gc.
 */

#ifndef CVMFS_SWISSKNIFE_INGEST_GC_H_
#define CVMFS_SWISSKNIFE_INGEST_GC_H_

#include <stdint.h>

#include <string>
#include <vector>

#include "duplex_sqlite3.h"
#include "util/logging.h"
#include "util/string.h"

/**
 * Read pending (not yet deleted) paths from a GC SQLite database.
 *
 * If batch_size > 0, at most batch_size rows are returned (in id order).
 * If ids is non-NULL, the ids of the returned rows are written to it in the
 * same order as paths.
 * Returns true on success.
 */
inline bool ReadGCDatabase(const std::string &db_path,
                           std::vector<std::string> *paths,
                           std::vector<int64_t> *ids = NULL,
                           int batch_size = 0) {
  sqlite3 *db = NULL;
  int rc = sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READONLY, NULL);
  if (rc != SQLITE_OK) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Cannot open GC database %s: %s",
             db_path.c_str(), sqlite3_errmsg(db));
    if (db) sqlite3_close(db);
    return false;
  }

  std::string sql = "SELECT id, path FROM gc_paths WHERE deleted = 0 "
                    "ORDER BY id";
  if (batch_size > 0) {
    sql += " LIMIT " + StringifyInt(batch_size);
  }
  sqlite3_stmt *stmt = NULL;
  rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, NULL);
  if (rc != SQLITE_OK) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Cannot prepare GC query on %s: %s",
             db_path.c_str(), sqlite3_errmsg(db));
    sqlite3_close(db);
    return false;
  }

  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    const int64_t id = sqlite3_column_int64(stmt, 0);
    const char *path =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1));
    if (path) {
      paths->push_back(std::string(path));
      if (ids != NULL) ids->push_back(id);
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
 * Mark paths as deleted in the GC SQLite database.
 *
 * If ids is non-empty, only the rows with the given ids are marked.
 * If ids is empty, all currently-pending rows are marked (legacy behaviour).
 * Returns true on success.
 */
inline bool MarkGCPathsDeleted(
    const std::string &db_path,
    const std::vector<int64_t> &ids = std::vector<int64_t>()) {
  sqlite3 *db = NULL;
  int rc = sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READWRITE, NULL);
  if (rc != SQLITE_OK) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Cannot open GC database for update %s: %s",
             db_path.c_str(), sqlite3_errmsg(db));
    if (db) sqlite3_close(db);
    return false;
  }

  if (ids.empty()) {
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

  const char *sql = "UPDATE gc_paths SET deleted = 1 WHERE id = ?";
  sqlite3_stmt *stmt = NULL;
  rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
  if (rc != SQLITE_OK) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Cannot prepare GC update on %s: %s",
             db_path.c_str(), sqlite3_errmsg(db));
    sqlite3_close(db);
    return false;
  }

  sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);
  for (size_t i = 0; i < ids.size(); ++i) {
    sqlite3_bind_int64(stmt, 1, ids[i]);
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Cannot mark GC path id %ld as deleted in %s: %s",
               static_cast<long>(ids[i]), db_path.c_str(),  // NOLINT
               sqlite3_errmsg(db));
      sqlite3_finalize(stmt);
      sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
      sqlite3_close(db);
      return false;
    }
    sqlite3_reset(stmt);
  }
  sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
  sqlite3_finalize(stmt);
  sqlite3_close(db);
  return true;
}

#endif  // CVMFS_SWISSKNIFE_INGEST_GC_H_
