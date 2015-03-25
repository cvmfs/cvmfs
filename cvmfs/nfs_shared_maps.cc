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

#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>

#include <cassert>
#include <cstdlib>

#include "atomic.h"
#include "cvmfs.h"
#include "duplex_sqlite3.h"
#include "logging.h"
#include "prng.h"
#include "statistics.h"
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

sqlite3_stmt *stmt_get_path_ = NULL;
sqlite3_stmt *stmt_get_inode_ = NULL;
sqlite3_stmt *stmt_add_ = NULL;

// Statistics to keep
perf::Counter *no_dbstat_seq_;
perf::Counter *n_dbstat_added_;
perf::Counter *n_dbstat_path_found_;
perf::Counter *n_dbstat_inode_found_;

struct BusyHandlerInfo {
  BusyHandlerInfo() {
    accumulated_ms = 0;
  }
  static const unsigned max_wait_ms = 60000;
  static const unsigned max_backoff_ms = 100;
  unsigned accumulated_ms;
};
BusyHandlerInfo *busy_handler_info_ = NULL;
Prng *prng_;

/**
 * Finds an inode by path
 * \return inode number, 0 if path not found
 */
static uint64_t FindInode(const PathString &path) {
  int sqlite_state;
  uint64_t inode;
  sqlite_state = sqlite3_bind_text(stmt_get_inode_, 1, path.GetChars(),
                                   path.GetLength(), SQLITE_TRANSIENT);
  assert(sqlite_state == SQLITE_OK);
  sqlite_state = sqlite3_step(stmt_get_inode_);
  if (sqlite_state == SQLITE_DONE) {
    // Path not found in DB
    sqlite3_reset(stmt_get_inode_);
    return 0;
  }
  if (sqlite_state != SQLITE_ROW) {
    LogCvmfs(kLogNfsMaps, kLogDebug, "Error finding inode (%s): %s",
             path.c_str(), sqlite3_errmsg(db_));
    sqlite3_reset(stmt_get_inode_);
    return 0;
  }
  inode = sqlite3_column_int64(stmt_get_inode_, 0);
  sqlite3_reset(stmt_get_inode_);
  return inode;
}


/**
 * Adds a new inode by path
 * \return New inode number, 0 on error
 */
static uint64_t IssueInode(const PathString &path) {
  int sqlite_state;
  uint64_t inode;
  sqlite_state = sqlite3_prepare_v2(db_, kSQL_AddInode, kMaxDBSqlLen,
                                    &stmt_add_, NULL);
  assert(sqlite_state == SQLITE_OK);
  sqlite_state = sqlite3_bind_text(stmt_add_, 1, path.GetChars(),
                                   path.GetLength(), SQLITE_TRANSIENT);
  if (sqlite_state != SQLITE_OK) {
    LogCvmfs(kLogNfsMaps, kLogDebug,
             "Failed to bind path in IssueInode (%s)", path.c_str());
    sqlite3_reset(stmt_add_);
    return 0;
  }
  sqlite_state = sqlite3_step(stmt_add_);
  if (sqlite_state != SQLITE_DONE) {
    LogCvmfs(kLogNfsMaps, kLogDebug,
             "Failed to execute SQL for IssueInode (%s): %s",
             path.c_str(), sqlite3_errmsg(db_));
    sqlite3_reset(stmt_add_);
    return 0;
  }
  inode = sqlite3_last_insert_rowid(db_);
  sqlite3_reset(stmt_add_);
  no_dbstat_seq_->Set(inode);
  perf::Inc(n_dbstat_added_);
  return inode;
}


uint64_t RetryGetInode(const PathString &path, int attempt) {
  if (attempt > 2) {
    // We have to give up eventually
    LogCvmfs(kLogNfsMaps, kLogSyslogErr, "Failed to find & create path (%s)",
             path.c_str());
    return 0;
  }

  uint64_t inode;
  pthread_mutex_lock(&lock_);
  inode = FindInode(path);
  if (inode) {
    perf::Inc(n_dbstat_path_found_);
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
  int sqlite_state;
  pthread_mutex_lock(&lock_);
  sqlite_state = sqlite3_bind_int64(stmt_get_path_, 1, inode);
  assert(sqlite_state == SQLITE_OK);
  sqlite_state = sqlite3_step(stmt_get_path_);
  if (sqlite_state == SQLITE_DONE) {
    // Success, but inode not found!
    sqlite3_reset(stmt_get_path_);
    pthread_mutex_unlock(&lock_);
    return false;
  }
  if (sqlite_state != SQLITE_ROW) {
    LogCvmfs(kLogNfsMaps, kLogSyslogErr,
             "Failed to execute SQL for GetPath (%"PRIu64"): %s",
             inode, sqlite3_errmsg(db_));
    pthread_mutex_unlock(&lock_);
    abort();
  }
  const char *raw_path = (const char *)sqlite3_column_text(stmt_get_path_, 0);
  path->Assign(raw_path, strlen(raw_path));
  sqlite3_reset(stmt_get_path_);
  pthread_mutex_unlock(&lock_);
  perf::Inc(n_dbstat_inode_found_);
  return true;
}


string GetStatistics() {
  string result = "Total number of issued inodes: " +
      n_dbstat_added_->Print() + "\n";
  result += "Last inode issued: " + no_dbstat_seq_->Print() + "\n";
  result += "inode --> path hits: " + n_dbstat_path_found_->Print() + "\n";
  result += "path --> inode hits: " + n_dbstat_inode_found_->Print() + "\n";

  return result;
}


static int BusyHandler(void *data, int attempt) {
  BusyHandlerInfo *handler_info = static_cast<BusyHandlerInfo *>(data);
  // Reset the accumulated time if this is the start of a new request
  if (attempt == 0)
    handler_info->accumulated_ms = 0;
  LogCvmfs(kLogNfsMaps, kLogDebug,
           "busy handler, attempt %d, accumulated waiting time %u",
           attempt, handler_info->accumulated_ms);
  if (handler_info->accumulated_ms >= handler_info->max_wait_ms)
    return 0;

  const unsigned backoff_range_ms = 1 << attempt;
  unsigned backoff_ms = prng_->Next(backoff_range_ms);
  if (handler_info->accumulated_ms + backoff_ms > handler_info->max_wait_ms)
    backoff_ms = handler_info->max_wait_ms - handler_info->accumulated_ms;
  if (backoff_ms > handler_info->max_backoff_ms)
    backoff_ms = handler_info->max_backoff_ms;

  SafeSleepMs(backoff_ms);
  handler_info->accumulated_ms += backoff_ms;
  return 1;
}


bool Init(const string &db_dir, const uint64_t root_inode,
          const bool rebuild)
{
  assert(root_inode > 0);
  string db_path = db_dir + "/inode_maps.db";

  no_dbstat_seq_ = cvmfs::statistics_->Register(
      "nfs_shared_maps.no_dbstat_seq", "Last inode issued");
  n_dbstat_added_ = cvmfs::statistics_->Register(
      "nfs_shared_maps.n_dbstat_added", "Total number of issued nodes");
  n_dbstat_path_found_ = cvmfs::statistics_->Register(
      "nfs_shared_maps.n_dbstat_path_found", "inode --> path hits");
  n_dbstat_inode_found_ = cvmfs::statistics_->Register(
      "nfs_shared_maps.n_dbstat_inode_found", "path --> inode hits");

  sqlite3_stmt *stmt;
  if (rebuild) {
    LogCvmfs(kLogNfsMaps, kLogSyslogWarn,
             "Ignoring rebuild flag as this may crash other cluster nodes.");
  }
  // We don't want the shared cache, we want minimal caching so sync is kept
  int retval = sqlite3_enable_shared_cache(0);
  assert(retval == SQLITE_OK);
  // Open the database
  retval = sqlite3_open_v2(db_path.c_str(), &db_,
                           SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE
                           | SQLITE_OPEN_CREATE, NULL);
  if (retval != SQLITE_OK) {
    LogCvmfs(kLogNfsMaps, kLogDebug,
             "Failed to create inode_maps file (%s)",
             db_path.c_str());
    // No need to Fini() here as nothing was succesfully created.
    return false;
  }
  // Be prepared to wait for up to 1 minute for transactions to complete
  // Being stuck for a long time is far more favorable than failing
  // TODO(jblomer): another busy handler.  This one conflicts with SIGALRM
  busy_handler_info_ = new BusyHandlerInfo();
  retval = sqlite3_busy_handler(db_, BusyHandler, busy_handler_info_);
  assert(retval == SQLITE_OK);

  // Set-up the main inode table if it doesn't exist
  retval = sqlite3_prepare_v2(db_, kSQL_CreateTable, kMaxDBSqlLen, &stmt, NULL);
  if (retval != SQLITE_OK) {
    LogCvmfs(kLogNfsMaps, kLogDebug | kLogSyslogErr,
             "Failed to prepare create table statement: %s",
             sqlite3_errmsg(db_));
    Fini();
    return false;
  }
  if (sqlite3_step(stmt) != SQLITE_DONE) {
    LogCvmfs(kLogNfsMaps, kLogSyslogErr,
             "Failed to create main inode table: %s", sqlite3_errmsg(db_));
    sqlite3_finalize(stmt);
    Fini();
    return false;
  }
  sqlite3_finalize(stmt);
  stmt = NULL;

  prng_ = new Prng();
  prng_->InitLocaltime();

  // Prepare lookup and add-inode statements
  retval = sqlite3_prepare_v2(db_, kSQL_GetPath, kMaxDBSqlLen, &stmt_get_path_,
                              NULL);
  assert(retval == SQLITE_OK);
  retval = sqlite3_prepare_v2(db_, kSQL_GetInode, kMaxDBSqlLen,
                              &stmt_get_inode_, NULL);
  assert(retval == SQLITE_OK);
  retval = sqlite3_prepare_v2(db_, kSQL_AddInode, kMaxDBSqlLen, &stmt_add_,
                              NULL);
  assert(retval == SQLITE_OK);

  // Check the root inode exists, if not create it
  PathString rootpath("", 0);
  if (!FindInode(rootpath)) {
    retval = sqlite3_prepare_v2(db_, kSQL_AddRoot, kMaxDBSqlLen, &stmt, NULL);
    assert(retval == SQLITE_OK);
    sqlite3_bind_int64(stmt, 1, root_inode);
    assert(retval == SQLITE_OK);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
      LogCvmfs(kLogNfsMaps, kLogDebug | kLogSyslogErr,
               "Failed to execute CreateRoot: %s", sqlite3_errmsg(db_));
      abort();
    }
    sqlite3_finalize(stmt);
  }

  return true;
}


void Spawn() {
  // Sqlite has no background threads so nothing to do here.
  return;
}


void Fini() {
  if (stmt_add_) sqlite3_finalize(stmt_add_);
  if (stmt_get_path_) sqlite3_finalize(stmt_get_path_);
  if (stmt_get_inode_) sqlite3_finalize(stmt_get_inode_);
  stmt_add_ = NULL;
  stmt_get_path_ = NULL;
  stmt_get_inode_ = NULL;
  // Close the handles, it is explicitly OK to call close with NULL
  sqlite3_close_v2(db_);
  db_ = NULL;

  delete prng_;
  delete busy_handler_info_;
  prng_ = NULL;
  busy_handler_info_ = NULL;
}

}  // namespace nfs_shared_maps
