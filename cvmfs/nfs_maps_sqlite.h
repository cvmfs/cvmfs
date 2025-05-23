/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NFS_MAPS_SQLITE_H_
#define CVMFS_NFS_MAPS_SQLITE_H_

#include <pthread.h>

#include <string>

#include "crypto/hash.h"
#include "duplex_sqlite3.h"
#include "nfs_maps.h"
#include "shortstring.h"
#include "util/atomic.h"
#include "util/prng.h"

namespace perf {
class Counter;
class Statistics;
}  // namespace perf

class NfsMapsSqlite : public NfsMaps {
 public:
  virtual ~NfsMapsSqlite();
  virtual uint64_t GetInode(const PathString &path);
  virtual bool GetPath(const uint64_t inode, PathString *path);

  static NfsMapsSqlite *Create(const std::string &db_dir,
                               const uint64_t root_inode,
                               const bool rebuild,
                               perf::Statistics *statistics_);

 private:
  static const char *kSqlCreateTable;
  static const char *kSqlAddRoot;
  static const char *kSqlAddInode;
  static const char *kSqlGetInode;
  static const char *kSqlGetPath;

  struct BusyHandlerInfo {
    BusyHandlerInfo() : accumulated_ms(0) { prng.InitLocaltime(); }

    static const unsigned kMaxWaitMs = 60000;
    static const unsigned kMaxBackoffMs = 100;
    unsigned accumulated_ms;
    Prng prng;
  };

  static int BusyHandler(void *data, int attempt);

  NfsMapsSqlite();
  uint64_t FindInode(const PathString &path);
  uint64_t IssueInode(const PathString &path);
  uint64_t RetryGetInode(const PathString &path, int attempt);

  sqlite3 *db_;
  sqlite3_stmt *stmt_get_path_;
  sqlite3_stmt *stmt_get_inode_;
  sqlite3_stmt *stmt_add_;
  pthread_mutex_t *lock_;

  BusyHandlerInfo busy_handler_info_;

  perf::Counter *n_db_seq_;
  perf::Counter *n_db_added_;
  perf::Counter *n_db_path_found_;
  perf::Counter *n_db_inode_found_;
};

#endif  // CVMFS_NFS_MAPS_SQLITE_H_
