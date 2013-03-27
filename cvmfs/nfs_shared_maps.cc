/**
 * This file is part of the CernVM File System.
 *
 * The NFS maps module maintains inode -- path relations.  An inode that is
 * issued once by an NFS exported file system might be asked for
 * any time later by clients.
 *
 * In "NFS mode", cvmfs will issue inodes consequtively and reuse inodes
 * based on path name.  The inode --> path and path --> inode maps are
 * handled by sqlite.  This workaround is comparable to the Fuse "noforget"
 * option, except that the mappings are persistent and thus consistent during
 * cvmfs restarts.  Also, sqlite allows for restricting the memory consumption.
 *
 * The maps are not accounted for by the cache quota.
 */

#define __STDC_FORMAT_MACROS

#include "nfs_shared_maps.h"

#include <stdint.h>
#include <inttypes.h>

#include <pthread.h>
#include <sqlite3.h>

#include <cassert>
#include <cstdlib>

#include "logging.h"
#include "util.h"

using namespace std;  // NOLINT

namespace nfs_shared_maps {

/* The main database handle & lock
 * We have to keep a lock on db_ operations, sqlite is only full thread-safe
 * if you open a new connection inside each thread. The entry to the various
 * look-up functions comes from numerous threads, but it'd be a big change to
 * get every thread to call some kind of ThreadInit() style function. We'll
 * just have to live with the performance hit this incurs for now...
 */
static sqlite3 *db_ = NULL;
pthread_mutex_t lock_ = PTHREAD_MUTEX_INITIALIZER;

// Max length of the sql statements below
static const int kMaxDBSqlLen = 128;

// SQL statements
static const char *kSQL_CreateTable =
  "CREATE TABLE IF NOT EXISTS inodes (path TEXT PRIMARY KEY);";
static const char *kSQL_AddRoot =
  "INSERT INTO inodes (rowid, path) VALUES (?, \"\");";
static const char *kSQL_AddInode =
  "INSERT INTO inodes VALUES (?);";
static const char *kSQL_GetInode =
  "SELECT rowid FROM inodes where path = ?;";
static const char *kSQL_GetPath =
  "SELECT path FROM inodes where rowid = ?;";

// Statistics to keep
static uint64_t dbstat_seq_ = 0;
static uint64_t dbstat_added_ = 0;
static uint64_t dbstat_path_found_ = 0;
static uint64_t dbstat_inode_found_ = 0;

/**
 * Finds an inode by path
 * \return inode number, 0 if path not found
 */
static uint64_t FindInode(const PathString &path) {
  int state;
  uint64_t inode;
  sqlite3_stmt *stmt;
  if (sqlite3_prepare_v2(db_, kSQL_GetInode, kMaxDBSqlLen, &stmt, NULL)
                           != SQLITE_OK) {
    LogCvmfs(kLogNfsMaps, kLogDebug, "Failed to prepare FindInode (%s)",
             path.c_str());
    return 0;
  }
  if (sqlite3_bind_text(stmt, 1, path.GetChars(), path.GetLength(),
                        SQLITE_TRANSIENT) != SQLITE_OK) {
    LogCvmfs(kLogNfsMaps, kLogDebug, "Failed to bind path in FindInode (%s)",
             path.c_str());
    sqlite3_finalize(stmt);
    return 0;
  }
  state = sqlite3_step(stmt);
  if (state == SQLITE_DONE) {
    // Path not found in DB
    sqlite3_finalize(stmt);
    return 0;
  }
  if (state != SQLITE_ROW) {
    LogCvmfs(kLogNfsMaps, kLogDebug,
             "Error finding inode (%s): %s",
             path.c_str(), sqlite3_errmsg(db_));
    sqlite3_finalize(stmt);
    return 0;
  }
  inode = sqlite3_column_int64(stmt, 0);
  sqlite3_finalize(stmt);
  return inode;
}


/**
 * Adds a new inode by path
 * \return New inode number, 0 on error
 */
static uint64_t IssueInode(const PathString &path) {
  int state;
  uint64_t inode;
  sqlite3_stmt *stmt;
  if (sqlite3_prepare_v2(db_, kSQL_AddInode, kMaxDBSqlLen, &stmt, NULL)
                           != SQLITE_OK) {
    LogCvmfs(kLogNfsMaps, kLogDebug, "Failed to prepare IssueInode (%s)",
             path.c_str());
    return 0;
  }
  if (sqlite3_bind_text(stmt, 1, path.GetChars(), path.GetLength(),
                        SQLITE_TRANSIENT) != SQLITE_OK) {
    LogCvmfs(kLogNfsMaps, kLogDebug,
             "Failed to bind path in IssueInode (%s)", path.c_str());
    sqlite3_finalize(stmt);
    return 0;
  }
  state = sqlite3_step(stmt);
  if (state != SQLITE_DONE) {
    LogCvmfs(kLogNfsMaps, kLogDebug,
             "Failed to execute SQL for IssueInode (%s): %s",
             path.c_str(), sqlite3_errmsg(db_));
    sqlite3_finalize(stmt);
    return 0;
  }
  inode = sqlite3_last_insert_rowid(db_);
  sqlite3_finalize(stmt);
  dbstat_seq_ = inode;
  dbstat_added_++;
  return inode;
}


uint64_t RetryGetInode(const PathString &path, int attempt) {
  if (attempt > 2) {
    // We have to give up eventually
    LogCvmfs(kLogNfsMaps, kLogSyslog, "Failed to find & create path (%s)",
             path.c_str());
    return 0;
  }

  uint64_t inode;
  pthread_mutex_lock(&lock_);
  inode = FindInode(path);
  if (inode) {
    dbstat_path_found_++;
    pthread_mutex_unlock(&lock_);
    return inode;
  }
  // Inode not found, issue a new one
  inode = IssueInode(path);
  pthread_mutex_unlock(&lock_);
  if (!inode) {
    inode = RetryGetInode(path, attempt + 1);
  }
  return inode;
}


/**
 * Finds the inode for path or issues a new inode.
 */
uint64_t GetInode(const PathString &path) {
  return RetryGetInode(path, 0);
}


/**
 * Finds the path that belongs to an inode.  This must be successful.  The
 * inode input comes from the file system, i.e. it must have been issued
 * before.
 * \return false if not found
 */
bool GetPath(const uint64_t inode, PathString *path) {
  int state;
  sqlite3_stmt *stmt;
  pthread_mutex_lock(&lock_);
  if (sqlite3_prepare_v2(db_, kSQL_GetPath, kMaxDBSqlLen, &stmt, NULL)
                           != SQLITE_OK) {
    pthread_mutex_unlock(&lock_);
    LogCvmfs(kLogNfsMaps, kLogDebug, "Failed to prepare GetPath (%"PRIu64")",
             inode);
    return 0;
  }
  if (sqlite3_bind_int64(stmt, 1, inode) != SQLITE_OK) {
    LogCvmfs(kLogNfsMaps, kLogDebug,
             "Failed to bind inode in GetPath (%"PRIu64")", inode);
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&lock_);
    return false;
  }
  state = sqlite3_step(stmt);
  if (state == SQLITE_DONE) {
    // Success, but inode not found!
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&lock_);
    return false;
  }
  if (state != SQLITE_ROW) {
    LogCvmfs(kLogNfsMaps, kLogSyslog,
             "Failed to execute SQL for GetPath (%"PRIu64"): %s",
             inode, sqlite3_errmsg(db_));
    pthread_mutex_unlock(&lock_);
    abort();
  }
  const char *raw_path = (const char *)sqlite3_column_text(stmt, 0);
  path->Assign(raw_path, strlen(raw_path));
  sqlite3_finalize(stmt);
  pthread_mutex_unlock(&lock_);
  dbstat_inode_found_++;
  return true;
}


string GetStatistics() {
  string result = "Total number of issued inodes: " +
                  StringifyInt(dbstat_added_) + "\n";
  result += "Last inode issued: " + StringifyInt(dbstat_seq_) + "\n";
  result += "inode --> path hits: " + StringifyInt(dbstat_path_found_)
              + "\n";
  result += "path --> inode hits: " + StringifyInt(dbstat_inode_found_)
              + "\n";

  return result;
}


bool Init(const string &db_dir, const uint64_t root_inode,
                            const bool rebuild) {
  assert(root_inode > 0);
  string db_path = db_dir + "/inode_maps.db";
  static sqlite3_stmt *stmt;

  if (rebuild) {
    LogCvmfs(kLogNfsMaps, kLogSyslog,
             "Ignoring rebuild flag as this may crash other cluster nodes.");
  }
  sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
  // We don't want the shared cache, we want minimal caching so sync is kept
  sqlite3_enable_shared_cache(0);
  // Open the database
  if (sqlite3_open_v2(db_path.c_str(), &db_,
                        SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE
                      | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK) {
    LogCvmfs(kLogNfsMaps, kLogDebug,
             "Failed to create inode_maps file (%s)",
             db_path.c_str());
    // No need to Fini() here as nothing was succesfully created.
    return false;
  }
  // Be prepared to wait for up to 1 minute for transactions to complete
  // Being stuck for a long time is far more favorable than failing
  sqlite3_busy_timeout(db_, 60000);
  // Set-up the main inode table if it doesn't exist
  if (sqlite3_prepare_v2(db_, kSQL_CreateTable, kMaxDBSqlLen,
                         &stmt, NULL) != SQLITE_OK) {
    LogCvmfs(kLogNfsMaps, kLogSyslog,
             "Failed to prepare create table statement: %s",
             sqlite3_errmsg(db_));
    Fini();
    return false;
  }
  if (sqlite3_step(stmt) != SQLITE_DONE) {
    LogCvmfs(kLogNfsMaps, kLogSyslog,
             "Failed to create main inode table: %s", sqlite3_errmsg(db_));
    sqlite3_finalize(stmt);
    Fini();
    return false;
  }
  sqlite3_finalize(stmt);
  stmt = NULL;

  // Check the root inode exists, if not create it
  PathString rootpath("", 0);
  if (sqlite3_prepare_v2(db_, kSQL_AddRoot, kMaxDBSqlLen,
                         &stmt, NULL) != SQLITE_OK) {
    LogCvmfs(kLogNfsMaps, kLogSyslog,
             "failed to prepare CreateRoot");
    abort();
  }
  if (!FindInode(rootpath)) {
    if (sqlite3_bind_int64(stmt, 1, root_inode) != SQLITE_OK) {
      LogCvmfs(kLogNfsMaps, kLogSyslog,
               "Failed to bind CreateRoot");
      abort();
    }
    if (sqlite3_step(stmt) != SQLITE_DONE) {
      LogCvmfs(kLogNfsMaps, kLogSyslog,
               "Failed to execute CreateRoot: %s",
               sqlite3_errmsg(db_));
      abort();
    }
  }
  sqlite3_finalize(stmt);
  return true;
}


void Spawn() {
  // Sqlite has no background threads so nothing to do here.
  return;
}


void Fini() {
  // Close the handles, it is explicitly OK to call close with NULL
  sqlite3_close_v2(db_);
  db_ = NULL;
}

}  // namespace nfs_shared_maps
