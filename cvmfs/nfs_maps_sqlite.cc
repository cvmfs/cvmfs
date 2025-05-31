/**
 * This file is part of the CernVM File System.
 *
 * The NFS maps module maintains inode -- path relations.  An inode that is
 * issued once by an NFS exported file system might be asked for
 * any time later by clients.
 *
 * In "NFS mode", cvmfs will issue inodes consecutively and reuse inodes
 * based on path name.  The inode --> path and path --> inode maps are
 * handled by sqlite.  This workaround is comparable to the Fuse "noforget"
 * option, except that the mappings are persistent and thus consistent during
 * cvmfs restarts.  Also, sqlite allows for restricting the memory consumption.
 *
 * The maps are not accounted for by the cache quota.
 */

#include "nfs_maps_sqlite.h"

#include <unistd.h>

#include <cassert>
#include <cstddef>
#include <cstdlib>

#include "statistics.h"
#include "util/concurrency.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/prng.h"
#include "util/smalloc.h"
#include "util/string.h"

using namespace std;  // NOLINT


const char *NfsMapsSqlite::kSqlCreateTable = "CREATE TABLE IF NOT EXISTS "
                                             "inodes (path TEXT PRIMARY KEY);";
const char *NfsMapsSqlite::
    kSqlAddRoot = "INSERT INTO inodes (oid, path) VALUES (?, ?);";
const char *NfsMapsSqlite::kSqlAddInode = "INSERT INTO inodes VALUES (?);";
const char
    *NfsMapsSqlite::kSqlGetInode = "SELECT rowid FROM inodes where path = ?;";
const char
    *NfsMapsSqlite::kSqlGetPath = "SELECT path FROM inodes where rowid = ?;";


int NfsMapsSqlite::BusyHandler(void *data, int attempt) {
  BusyHandlerInfo *handler_info = static_cast<BusyHandlerInfo *>(data);
  // Reset the accumulated time if this is the start of a new request
  if (attempt == 0)
    handler_info->accumulated_ms = 0;
  LogCvmfs(kLogNfsMaps, kLogDebug,
           "busy handler, attempt %d, accumulated waiting time %u", attempt,
           handler_info->accumulated_ms);
  if (handler_info->accumulated_ms >= handler_info->kMaxWaitMs)
    return 0;

  const unsigned backoff_range_ms = 1 << attempt;
  unsigned backoff_ms = handler_info->prng.Next(backoff_range_ms);
  if (handler_info->accumulated_ms + backoff_ms > handler_info->kMaxWaitMs) {
    backoff_ms = handler_info->kMaxWaitMs - handler_info->accumulated_ms;
  }
  if (backoff_ms > handler_info->kMaxBackoffMs) {
    backoff_ms = handler_info->kMaxBackoffMs;
  }

  SafeSleepMs(backoff_ms);
  handler_info->accumulated_ms += backoff_ms;
  return 1;
}


NfsMapsSqlite *NfsMapsSqlite::Create(const string &db_dir,
                                     const uint64_t root_inode,
                                     const bool rebuild,
                                     perf::Statistics *statistics) {
  assert(root_inode > 0);
  UniquePtr<NfsMapsSqlite> maps(new NfsMapsSqlite());
  maps->n_db_added_ = statistics->Register("nfs.sqlite.n_added",
                                           "total number of issued inode");
  maps->n_db_seq_ = statistics->Register("nfs.sqlite.n_seq",
                                         "last inode issued");
  maps->n_db_path_found_ = statistics->Register("nfs.sqlite.n_path_hit",
                                                "inode --> path hits");
  maps->n_db_inode_found_ = statistics->Register("nfs.sqlite.n_inode_hit",
                                                 "path --> inode hits");

  const string db_path = db_dir + "/inode_maps.db";

  sqlite3_stmt *stmt;
  if (rebuild) {
    LogCvmfs(kLogNfsMaps, kLogDebug | kLogSyslogWarn,
             "Ignoring rebuild flag as this may crash other cluster nodes.");
  }
  // We don't want the shared cache, we want minimal caching so sync is kept
  int retval = sqlite3_enable_shared_cache(0);
  assert(retval == SQLITE_OK);

  retval = sqlite3_open_v2(
      db_path.c_str(), &maps->db_,
      SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
  if (retval != SQLITE_OK) {
    LogCvmfs(kLogNfsMaps, kLogDebug, "Failed to create inode_maps file (%s)",
             db_path.c_str());
    return NULL;
  }
  // Be prepared to wait for up to 1 minute for transactions to complete
  // Being stuck for a long time is far more favorable than failing
  // TODO(jblomer): another busy handler.  This one conflicts with SIGALRM
  retval = sqlite3_busy_handler(maps->db_, BusyHandler,
                                &maps->busy_handler_info_);
  assert(retval == SQLITE_OK);

  // Set-up the main inode table if it doesn't exist
  retval = sqlite3_prepare_v2(maps->db_, kSqlCreateTable, -1, &stmt, NULL);
  if (retval != SQLITE_OK) {
    LogCvmfs(kLogNfsMaps, kLogDebug | kLogSyslogErr,
             "Failed to prepare create table statement: %s",
             sqlite3_errmsg(maps->db_));
    return NULL;
  }
  if (sqlite3_step(stmt) != SQLITE_DONE) {
    LogCvmfs(kLogNfsMaps, kLogSyslogErr,
             "Failed to create main inode table: %s",
             sqlite3_errmsg(maps->db_));
    sqlite3_finalize(stmt);
    return NULL;
  }
  sqlite3_finalize(stmt);

  // Prepare lookup and add-inode statements
  retval = sqlite3_prepare_v2(maps->db_, kSqlGetPath, -1, &maps->stmt_get_path_,
                              NULL);
  assert(retval == SQLITE_OK);
  retval = sqlite3_prepare_v2(maps->db_, kSqlGetInode, -1,
                              &maps->stmt_get_inode_, NULL);
  assert(retval == SQLITE_OK);
  retval = sqlite3_prepare_v2(maps->db_, kSqlAddInode, -1, &maps->stmt_add_,
                              NULL);
  assert(retval == SQLITE_OK);

  // Check the root inode exists, if not create it
  const PathString rootpath("", 0);
  if (!maps->FindInode(rootpath)) {
    retval = sqlite3_prepare_v2(maps->db_, kSqlAddRoot, -1, &stmt, NULL);
    assert(retval == SQLITE_OK);
    retval = sqlite3_bind_int64(stmt, 1, root_inode);
    assert(retval == SQLITE_OK);
    retval = sqlite3_bind_text(stmt, 2, "", 0, SQLITE_TRANSIENT);
    assert(retval == SQLITE_OK);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
      PANIC(kLogDebug | kLogSyslogErr, "Failed to execute CreateRoot: %s",
            sqlite3_errmsg(maps->db_));
    }
    sqlite3_finalize(stmt);
  }

  return maps.Release();
}


/**
 * Finds an inode by path
 * \return inode number, 0 if path not found
 */
uint64_t NfsMapsSqlite::FindInode(const PathString &path) {
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
uint64_t NfsMapsSqlite::IssueInode(const PathString &path) {
  int sqlite_state;
  uint64_t inode;
  sqlite_state = sqlite3_prepare_v2(db_, kSqlAddInode, -1, &stmt_add_, NULL);
  assert(sqlite_state == SQLITE_OK);
  sqlite_state = sqlite3_bind_text(stmt_add_, 1, path.GetChars(),
                                   path.GetLength(), SQLITE_TRANSIENT);
  if (sqlite_state != SQLITE_OK) {
    LogCvmfs(kLogNfsMaps, kLogDebug, "Failed to bind path in IssueInode (%s)",
             path.c_str());
    sqlite3_reset(stmt_add_);
    return 0;
  }
  sqlite_state = sqlite3_step(stmt_add_);
  if (sqlite_state != SQLITE_DONE) {
    LogCvmfs(kLogNfsMaps, kLogDebug,
             "Failed to execute SQL for IssueInode (%s): %s", path.c_str(),
             sqlite3_errmsg(db_));
    sqlite3_reset(stmt_add_);
    return 0;
  }
  inode = sqlite3_last_insert_rowid(db_);
  sqlite3_reset(stmt_add_);
  n_db_seq_->Set(inode);
  perf::Inc(n_db_added_);
  return inode;
}


uint64_t NfsMapsSqlite::RetryGetInode(const PathString &path, int attempt) {
  if (attempt > 2) {
    // We have to give up eventually
    LogCvmfs(kLogNfsMaps, kLogSyslogErr, "Failed to find & create path (%s)",
             path.c_str());
    return 0;
  }

  uint64_t inode;
  {
    const MutexLockGuard m(lock_);
    inode = FindInode(path);
    if (inode) {
      perf::Inc(n_db_path_found_);
      return inode;
    }
    // Inode not found, issue a new one
    inode = IssueInode(path);
  }

  if (!inode) {
    inode = RetryGetInode(path, attempt + 1);
  }
  return inode;
}


/**
 * Finds the inode for path or issues a new inode.
 */
uint64_t NfsMapsSqlite::GetInode(const PathString &path) {
  return RetryGetInode(path, 0);
}


/**
 * Finds the path that belongs to an inode.  This must be successful.  The
 * inode input comes from the file system, i.e. it must have been issued
 * before.
 * \return false if not found
 */
bool NfsMapsSqlite::GetPath(const uint64_t inode, PathString *path) {
  int sqlite_state;
  const MutexLockGuard m(lock_);

  sqlite_state = sqlite3_bind_int64(stmt_get_path_, 1, inode);
  assert(sqlite_state == SQLITE_OK);
  sqlite_state = sqlite3_step(stmt_get_path_);
  if (sqlite_state == SQLITE_DONE) {
    // Success, but inode not found!
    sqlite3_reset(stmt_get_path_);
    return false;
  }
  if (sqlite_state != SQLITE_ROW) {
    PANIC(kLogSyslogErr, "Failed to execute SQL for GetPath (%" PRIu64 "): %s",
          inode, sqlite3_errmsg(db_));
  }
  const char *raw_path = (const char *)sqlite3_column_text(stmt_get_path_, 0);
  path->Assign(raw_path, strlen(raw_path));
  sqlite3_reset(stmt_get_path_);
  perf::Inc(n_db_inode_found_);
  return true;
}


NfsMapsSqlite::NfsMapsSqlite()
    : db_(NULL)
    , stmt_get_path_(NULL)
    , stmt_get_inode_(NULL)
    , stmt_add_(NULL)
    , lock_(NULL)
    , n_db_seq_(NULL)
    , n_db_added_(NULL)
    , n_db_path_found_(NULL)
    , n_db_inode_found_(NULL) {
  lock_ = reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  const int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


NfsMapsSqlite::~NfsMapsSqlite() {
  if (stmt_add_)
    sqlite3_finalize(stmt_add_);
  if (stmt_get_path_)
    sqlite3_finalize(stmt_get_path_);
  if (stmt_get_inode_)
    sqlite3_finalize(stmt_get_inode_);
  // Close the handles, it is explicitly OK to call close with NULL
  sqlite3_close_v2(db_);
  pthread_mutex_destroy(lock_);
  free(lock_);
}
