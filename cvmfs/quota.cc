/**
 * This file is part of the CernVM File System.
 *
 * This module implements a "managed local cache".
 * This way, we are able to track access times of files in the cache
 * and remove files based on least recently used strategy.
 *
 * We setup another SQLite catalog, a "cache catalog", that helps us
 * in the bookkeeping of files, file sizes and access times.
 *
 * We might choose to not manage the local cache.  This is indicated
 * by limit == 0 and everything succeeds in that case.
 */

#define __STDC_LIMIT_MACROS
#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "quota.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/dir.h>
#include <sys/wait.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <fcntl.h>

#include <cassert>
#include <cstdlib>
#include <cstdio>

#include <string>
#include <vector>
#include <map>
#include <set>

#include "platform.h"
#include "logging.h"
#include "duplex_sqlite3.h"
#include "hash.h"
#include "util.h"
#include "smalloc.h"
#include "cvmfs.h"
#include "monitor.h"

using namespace std;  // NOLINT

namespace quota {

static void GetLimits(uint64_t *limit, uint64_t *cleanup_threshold);

/**
 * Loaded catalogs are pinned in the LRU and have to be treated differently.
 */
enum FileTypes {
  kFileRegular = 0,
  kFileCatalog,
};

enum CommandType {
  kTouch = 0,
  kInsert,
  kReserve,
  kPin,
  kUnpin,
  kRemove,
  kCleanup,
  kList,
  kListPinned,
  kListCatalogs,
  kStatus,
  kLimits,
  kPid,
};

struct LruCommand {
  CommandType command_type;
  uint64_t size;
  int return_pipe;  // For cleanup, listing, and reservations
  unsigned char digest[hash::kMaxDigestSize];
  uint16_t path_length;  // Maximum 512-sizeof(LruCommand) in order to guarantee
                         // atomic pipe operations
};

/**
 * Maximum page cache per thread (Bytes).
 */
const unsigned kSqliteMemPerThread = 2*1024*1024;
const unsigned kCommandBufferSize = 32;
const unsigned kMaxCvmfsPath = 512-sizeof(LruCommand);

pthread_t thread_lru_;
int pipe_lru_[2];
bool shared_;
bool spawned_;
bool initialized_ = false;
map<hash::Any, uint64_t> *pinned_chunks_ = NULL;
int fd_lock_cachedb_;

uint64_t limit_;  /**< If the cache grows above this size,
                      we clean up until cleanup_threshold. */
uint64_t pinned_;  /**< Size of pinned files (file catalogs). */
uint64_t cleanup_threshold_;  /**< When cleaning up, stop when size is below
  cleanup_threshold. This way, the current working set stays in cache. */
uint64_t gauge_;  /**< Current size of cache. */
uint64_t seq_;  /**< Current access sequence number.  Gets increased on every
                     access/insert operation. */
string *cache_dir_ = NULL;

sqlite3 *db_ = NULL;
sqlite3_stmt *stmt_touch_ = NULL;
sqlite3_stmt *stmt_unpin_ = NULL;
sqlite3_stmt *stmt_new_ = NULL;
sqlite3_stmt *stmt_lru_ = NULL;
sqlite3_stmt *stmt_size_ = NULL;
sqlite3_stmt *stmt_rm_ = NULL;
sqlite3_stmt *stmt_list_ = NULL;
sqlite3_stmt *stmt_list_pinned_ = NULL;  /**< Loaded catalogs are pinned. */
sqlite3_stmt *stmt_list_catalogs_ = NULL;


static void MakeReturnPipe(int pipe[2]) {
  if (!shared_) {
    MakePipe(pipe);
    return;
  }

  // Create FIFO in cache directory, store path name (number) in pipe write end
  int i = 0;
  int retval;
  do {
    retval = mkfifo((*cache_dir_ + "/pipe" + StringifyInt(i)).c_str(), 0600);
    pipe[1] = i;
    i++;
  } while ((retval == -1) && (errno == EEXIST));
  assert(retval == 0);

  // Connect reader's end
  pipe[0] = open((*cache_dir_ + "/pipe" + StringifyInt(pipe[1])).c_str(),
                 O_RDONLY | O_NONBLOCK);
  assert(pipe[0] >= 0);
  Nonblock2Block(pipe[0]);
}


// TODO: make sure that a broken return pipe does not kill the cache manager
static int BindReturnPipe(int pipe_wronly) {
  if (!shared_)
    return pipe_wronly;

  // Connect writer's end
  int result = open((*cache_dir_ + "/pipe" + StringifyInt(pipe_wronly)).c_str(),
                    O_WRONLY | O_NONBLOCK);
  assert(result >= 0);
  Nonblock2Block(result);
  return result;
}


static void UnbindReturnPipe(int pipe_wronly) {
  if (shared_)
    close(pipe_wronly);
}


static void CloseReturnPipe(int pipe[2]) {
  if (shared_) {
    close(pipe[0]);
    unlink((*cache_dir_ + "/pipe" + StringifyInt(pipe[1])).c_str());
  } else {
    ClosePipe(pipe);
  }
}


static bool DoCleanup(const uint64_t leave_size) {
  if ((limit_ == 0) || (gauge_ <= leave_size))
    return true;

  // TODO transaction
  LogCvmfs(kLogQuota, kLogSyslog,
           "cleanup cache until %lu KB are free", leave_size/1024);
  LogCvmfs(kLogQuota, kLogDebug, "gauge %"PRIu64, gauge_);

  bool result;
  string hash_str;
  vector<string> trash;

  do {
    sqlite3_reset(stmt_lru_);
    if (sqlite3_step(stmt_lru_) != SQLITE_ROW) {
      LogCvmfs(kLogQuota, kLogDebug, "could not get lru-entry");
      break;
    }

    hash_str = string(reinterpret_cast<const char *>(
                      sqlite3_column_text(stmt_lru_, 0)));
    LogCvmfs(kLogQuota, kLogDebug, "removing %s", hash_str.c_str());
    hash::Any hash(hash::kSha1, hash::HexPtr(
      hash_str.substr(0, 2*hash::kDigestSizes[hash::kSha1])));
    if (pinned_chunks_->find(hash) != pinned_chunks_->end())
      continue;

    trash.push_back((*cache_dir_) + hash.MakePath(1, 2));
    gauge_ -= sqlite3_column_int64(stmt_lru_, 1);
    LogCvmfs(kLogQuota, kLogDebug, "lru cleanup %s, new gauge %"PRIu64,
             hash_str.c_str(), gauge_);

    sqlite3_bind_text(stmt_rm_, 1, &hash_str[0], hash_str.length(),
                      SQLITE_STATIC);
    result = (sqlite3_step(stmt_rm_) == SQLITE_DONE);
    sqlite3_reset(stmt_rm_);

    if (!result) {
      LogCvmfs(kLogQuota, kLogDebug, "could not remove lru-entry");
      return false;
    }
  } while (gauge_ > leave_size);

  // Double fork avoids zombie, forked removal process must not flush file
  // buffers
  if (!trash.empty()) {
    pid_t pid;
    int statloc;
    if ((pid = fork()) == 0) {
      if (fork() == 0) {
        for (unsigned i = 0, iEnd = trash.size(); i < iEnd; ++i) {
          LogCvmfs(kLogQuota, kLogDebug, "unlink %s", trash[i].c_str());
          unlink(trash[i].c_str());
        }
        _exit(0);
      }
      _exit(0);
    } else {
      if (pid > 0)
        waitpid(pid, &statloc, 0);
      else
        return false;
    }
  }

  return gauge_ <= leave_size;
}


static bool Contains(const string &hash_str) {
  bool result = false;

  sqlite3_bind_text(stmt_size_, 1, &hash_str[0], hash_str.length(),
                    SQLITE_STATIC);
  if (sqlite3_step(stmt_size_) == SQLITE_ROW)
    result = true;
  sqlite3_reset(stmt_size_);
  LogCvmfs(kLogQuota, kLogDebug, "contains %s returns %d",
           hash_str.c_str(), result);

  return result;
}


static void ProcessCommandBunch(const unsigned num,
                                const LruCommand *commands, const char *paths)
{
  int retval = sqlite3_exec(db_, "BEGIN", NULL, NULL, NULL);
  assert(retval == SQLITE_OK);

  for (unsigned i = 0; i < num; ++i) {
    const hash::Any hash(hash::kSha1, commands[i].digest,
                         sizeof(commands[i].digest));
    const string hash_str = hash.ToString();
    const unsigned size = commands[i].size;
    LogCvmfs(kLogQuota, kLogDebug, "processing %s (%d)",
             hash_str.c_str(), commands[i].command_type);

    bool exists;
    switch (commands[i].command_type) {
      case kTouch:
        sqlite3_bind_int64(stmt_touch_, 1, seq_++);
        sqlite3_bind_text(stmt_touch_, 2, &hash_str[0], hash_str.length(),
                          SQLITE_STATIC);
        retval = sqlite3_step(stmt_touch_);
        LogCvmfs(kLogQuota, kLogDebug, "touching %s (%ld): %d",
                 hash_str.c_str(), seq_-1, retval);
        errno = retval;
        assert((retval == SQLITE_DONE) || (retval == SQLITE_OK));
        sqlite3_reset(stmt_touch_);
        break;
      case kUnpin:
        sqlite3_bind_text(stmt_unpin_, 1, &hash_str[0], hash_str.length(),
                          SQLITE_STATIC);
        retval = sqlite3_step(stmt_unpin_);
        LogCvmfs(kLogQuota, kLogDebug, "unpinning %s: %d",
                 hash_str.c_str(), retval);
        errno = retval;
        assert((retval == SQLITE_DONE) || (retval == SQLITE_OK));
        sqlite3_reset(stmt_unpin_);
        break;
      case kPin:
      case kInsert:
        // It could already be in, check
        exists = Contains(hash_str);

        // Cleanup, move to trash and unlink
        if (!exists && (gauge_ + size > limit_)) {
          LogCvmfs(kLogQuota, kLogDebug, "over limit, gauge %lu, file size %lu",
                   gauge_, size);
          retval = DoCleanup(cleanup_threshold_);
          assert(retval != 0);
        }

        // Insert or replace
        sqlite3_bind_text(stmt_new_, 1, &hash_str[0], hash_str.length(),
                          SQLITE_STATIC);
        sqlite3_bind_int64(stmt_new_, 2, size);
        sqlite3_bind_int64(stmt_new_, 3, seq_++);
        sqlite3_bind_text(stmt_new_, 4, &paths[i*kMaxCvmfsPath],
                          commands[i].path_length, SQLITE_STATIC);
        sqlite3_bind_int64(stmt_new_, 5, (commands[i].command_type == kPin) ?
                           kFileCatalog : kFileRegular);
        sqlite3_bind_int64(stmt_new_, 6, (commands[i].command_type == kPin) ?
                           1 : 0);
        retval = sqlite3_step(stmt_new_);
        LogCvmfs(kLogQuota, kLogDebug, "insert or replace %s, pin %d: %d",
                 hash_str.c_str(), commands[i].command_type, retval);
        assert((retval == SQLITE_DONE) || (retval == SQLITE_OK));
        sqlite3_reset(stmt_new_);

        if (!exists) gauge_ += size;
        break;
      default:
        abort();  // other types should have been taken care of by event loop
    }
  }

  retval = sqlite3_exec(db_, "COMMIT", NULL, NULL, NULL);
  assert(retval == SQLITE_OK);
}


/**
 * Event loop for processing commands.  Most of them are queued, some have
 * to be executed immediately.
 */
static void *MainCommandServer(void *data __attribute__((unused))) {
  LogCvmfs(kLogQuota, kLogDebug, "starting cache manager");
  sqlite3_soft_heap_limit(kSqliteMemPerThread);

  LruCommand command_buffer[kCommandBufferSize];
  char path_buffer[kCommandBufferSize*kMaxCvmfsPath];
  unsigned num_commands = 0;

  while (read(pipe_lru_[0], &command_buffer[num_commands],
              sizeof(command_buffer[0])) == sizeof(command_buffer[0]))
  {
    const CommandType command_type = command_buffer[num_commands].command_type;
    LogCvmfs(kLogQuota, kLogDebug, "received command %d", command_type);
    const uint64_t size = command_buffer[num_commands].size;

    // Inserts and pins come with a cvmfs path
    if ((command_type == kInsert) || (command_type == kPin)) {
      const int path_length = command_buffer[num_commands].path_length;
      ReadPipe(pipe_lru_[0], &path_buffer[kMaxCvmfsPath*num_commands],
               path_length);
    }

    // Reservations are handled immediately and "out of band"
    if (command_type == kReserve) {
      bool success = true;
      int return_pipe =
        BindReturnPipe(command_buffer[num_commands].return_pipe);
      const hash::Any hash(hash::kSha1, command_buffer[num_commands].digest,
                           sizeof(command_buffer[num_commands].digest));
      const string hash_str(hash.ToString());
      LogCvmfs(kLogQuota, kLogDebug, "reserve %d bytes for %s",
               size, hash_str.c_str());

      if (pinned_chunks_->find(hash) == pinned_chunks_->end()) {
        if ((cleanup_threshold_ > 0) && (pinned_ + size > cleanup_threshold_)) {
          LogCvmfs(kLogQuota, kLogDebug,
                   "failed to insert %s (pinned), no space", hash_str.c_str());
          success = false;
        } else {
          (*pinned_chunks_)[hash] = size;
          pinned_ += size;
        }
      }

      WritePipe(return_pipe, &success, sizeof(success));
      UnbindReturnPipe(return_pipe);
      continue;
    }

    // Unpinnings are also handled immediately with respect to the pinned gauge
    if (command_type == kUnpin) {
      const hash::Any hash(hash::kSha1, command_buffer[num_commands].digest,
                           sizeof(command_buffer[num_commands].digest));
      const string hash_str(hash.ToString());

      map<hash::Any, uint64_t>::iterator iter = pinned_chunks_->find(hash);
      if (iter != pinned_chunks_->end()) {
        pinned_ -= iter->second;
        pinned_chunks_->erase(iter);
      } else {
        LogCvmfs(kLogQuota, kLogDebug, "this chunk was not pinned");
      }
    }

    // Immediate commands trigger flushing of the buffer
    bool immediate_command = (command_type == kCleanup) ||
      (command_type == kList) || (command_type == kListPinned) ||
      (command_type == kListCatalogs) || (command_type == kRemove) ||
      (command_type == kStatus) || (command_type == kLimits) ||
      (command_type == kPid);
    if (!immediate_command) num_commands++;

    if ((num_commands == kCommandBufferSize) || immediate_command)
    {
      ProcessCommandBunch(num_commands, command_buffer, path_buffer);
      if (!immediate_command) num_commands = 0;
    }

    if (immediate_command) {
      // Process cleanup, listings
      int return_pipe =
        BindReturnPipe(command_buffer[num_commands].return_pipe);
      int retval;
      sqlite3_stmt *this_stmt_list = NULL;
      switch (command_type) {
        case kRemove: {
          const hash::Any hash(hash::kSha1, command_buffer[num_commands].digest,
                               sizeof(command_buffer[num_commands].digest));
          const string hash_str = hash.ToString();
          LogCvmfs(kLogQuota, kLogDebug, "manually removing %s",
                   hash_str.c_str());

          sqlite3_bind_text(stmt_size_, 1, &hash_str[0], hash_str.length(),
                            SQLITE_STATIC);
          int retval;
          if ((retval = sqlite3_step(stmt_size_)) == SQLITE_ROW) {
            uint64_t size = sqlite3_column_int64(stmt_size_, 0);
            uint64_t is_pinned = sqlite3_column_int64(stmt_size_, 1);

            sqlite3_bind_text(stmt_rm_, 1, &(hash_str[0]), hash_str.length(),
                              SQLITE_STATIC);
            retval = sqlite3_step(stmt_rm_);
            if ((retval == SQLITE_DONE) || (retval == SQLITE_OK)) {
              gauge_ -= size;
              if (is_pinned) {
                pinned_chunks_->erase(hash);
                pinned_ -= size;
              }
            } else {
              LogCvmfs(kLogQuota, kLogDebug, "could not delete %s, error %d",
                       hash_str.c_str(), retval);
            }
            sqlite3_reset(stmt_rm_);
          }
          sqlite3_reset(stmt_size_);
          break; }
        case kCleanup:
          retval = DoCleanup(size);
          WritePipe(return_pipe, &retval, sizeof(retval));
          break;
        case kList:
          if (!this_stmt_list) this_stmt_list = stmt_list_;
        case kListPinned:
          if (!this_stmt_list) this_stmt_list = stmt_list_pinned_;
        case kListCatalogs:
          if (!this_stmt_list) this_stmt_list = stmt_list_catalogs_;

          // Pipe back the list, one by one
          int length;
          while (sqlite3_step(this_stmt_list) == SQLITE_ROW) {
            string path = "(NULL)";
            if (sqlite3_column_type(this_stmt_list, 0) != SQLITE_NULL) {
              path = string(
                reinterpret_cast<const char *>(
                  sqlite3_column_text(this_stmt_list, 0)));
            }
            length = path.length();
            WritePipe(return_pipe, &length, sizeof(length));
            if (length > 0)
              WritePipe(return_pipe, &path[0], length);
          }
          length = -1;
          WritePipe(return_pipe, &length, sizeof(length));
          sqlite3_reset(this_stmt_list);
          break;
        case kStatus:
          WritePipe(return_pipe, &gauge_, sizeof(gauge_));
          WritePipe(return_pipe, &pinned_, sizeof(pinned_));
          break;
        case kLimits:
          WritePipe(return_pipe, &limit_, sizeof(limit_));
          WritePipe(return_pipe, &cleanup_threshold_,
                    sizeof(cleanup_threshold_));
          break;
        case kPid: {
          pid_t pid = getpid();
          WritePipe(return_pipe, &pid, sizeof(pid));
          break;
        }
        default:
          abort();  // other types are handled by the bunch processor
      }
      UnbindReturnPipe(return_pipe);
      num_commands = 0;
    }
  }

  LogCvmfs(kLogQuota, kLogDebug, "stopping cache manager (%d)", errno);
  close(pipe_lru_[0]);
  ProcessCommandBunch(num_commands, command_buffer, path_buffer);

  // Unpin
  command_buffer[0].command_type = kTouch;
  for (map<hash::Any, uint64_t>::const_iterator i = pinned_chunks_->begin(),
       iEnd = pinned_chunks_->end(); i != iEnd; ++i)
  {
    memcpy(command_buffer[0].digest, i->first.digest, i->first.GetDigestSize());
    ProcessCommandBunch(1, command_buffer, path_buffer);
  }

  return NULL;
}


/**
 * Rebuilds the SQLite cache catalog based on the stat-information of files
 * in the cache directory.
 *
 * \return True on success, false otherwise
 */
bool RebuildDatabase() {
  bool result = false;
  string sql;
  sqlite3_stmt *stmt_select = NULL;
  sqlite3_stmt *stmt_insert = NULL;
  int sqlerr;
  int seq = 0;
  char hex[3];
  struct stat info;
  platform_dirent64 *d;
  DIR *dirp = NULL;
  string path;
  set<string> catalogs;

  LogCvmfs(kLogQuota, kLogSyslog | kLogDebug, "re-building cache-database");

  // Empty cache catalog and fscache
  sql = "DELETE FROM cache_catalog; DELETE FROM fscache;";
  sqlerr = sqlite3_exec(db_, sql.c_str(), NULL, NULL, NULL);
  if (sqlerr != SQLITE_OK) {
    LogCvmfs(kLogQuota, kLogDebug, "could not clear cache database");
    goto build_return;
  }

  gauge_ = 0;

  // Gather file catalog hash values
  // TODO: distiction does not exist anymore
  if ((dirp = opendir(cache_dir_->c_str())) == NULL) {
    LogCvmfs(kLogQuota, kLogDebug, "failed to open directory %s", path.c_str());
    goto build_return;
  }
  while ((d = platform_readdir(dirp)) != NULL) {
    if (d->d_type != DT_REG) continue;

    const string name = d->d_name;
    if (name.substr(0, 14) == "cvmfs.checksum") {
      FILE *f = fopen(((*cache_dir_) + "/" + name).c_str(), "r");
      if (f != NULL) {
        char sha1[40];
        if (fread(sha1, 1, 40, f) == 40) {
          LogCvmfs(kLogQuota, kLogDebug, "added %s to catalog list",
                   string(sha1, 40).c_str());
          catalogs.insert(string(sha1, 40).c_str());
        }
        fclose(f);
      }
    }
  }
  closedir(dirp);

  // Insert files from cache sub-directories 00 - ff
  sqlite3_prepare_v2(db_, "INSERT INTO fscache (sha1, size, actime) "
                     "VALUES (:sha1, :s, :t);", -1, &stmt_insert, NULL);

  for (int i = 0; i <= 0xff; i++) {
    snprintf(hex, sizeof(hex), "%02x", i);
    path = (*cache_dir_) + "/" + string(hex);
    if ((dirp = opendir(path.c_str())) == NULL) {
      LogCvmfs(kLogQuota, kLogDebug | kLogSyslog,
               "failed to open directory %s (tmpwatch interfering?)",
               path.c_str());
      goto build_return;
    }
    while ((d = platform_readdir(dirp)) != NULL) {
      if (d->d_type != DT_REG) continue;

      if (stat((path + "/" + string(d->d_name)).c_str(), &info) == 0) {
        string sha1 = string(hex) + string(d->d_name);
        sqlite3_bind_text(stmt_insert, 1, sha1.c_str(), sha1.length(),
                          SQLITE_STATIC);
        sqlite3_bind_int64(stmt_insert, 2, info.st_size);
        sqlite3_bind_int64(stmt_insert, 3, info.st_atime);
        if (sqlite3_step(stmt_insert) != SQLITE_DONE) {
          LogCvmfs(kLogQuota, kLogDebug, "could not insert into temp table");
          goto build_return;
        }
        sqlite3_reset(stmt_insert);

        gauge_ += info.st_size;
      } else {
        LogCvmfs(kLogQuota, kLogDebug, "could not stat %s/%s",
                 path.c_str(), d->d_name);
      }
    }
    closedir(dirp);
    dirp = NULL;
  }
  sqlite3_finalize(stmt_insert);
  stmt_insert = NULL;

  // Transfer from temp table in cache catalog
  sqlite3_prepare_v2(db_, "SELECT sha1, size FROM fscache ORDER BY actime;",
                     -1, &stmt_select, NULL);
  sqlite3_prepare_v2(db_,
    "INSERT INTO cache_catalog (sha1, size, acseq, path, type, pinned) "
    "VALUES (:sha1, :s, :seq, 'unknown (automatic rebuild)', :t, 0);",
    -1, &stmt_insert, NULL);
  while (sqlite3_step(stmt_select) == SQLITE_ROW) {
    const string sha1 = string(
      reinterpret_cast<const char *>(sqlite3_column_text(stmt_select, 0)));
    sqlite3_bind_text(stmt_insert, 1, &sha1[0], sha1.length(), SQLITE_STATIC);
    sqlite3_bind_int64(stmt_insert, 2, sqlite3_column_int64(stmt_select, 1));
    sqlite3_bind_int64(stmt_insert, 3, seq++);
    if (catalogs.find(sha1) != catalogs.end())
      sqlite3_bind_int64(stmt_insert, 4, kFileCatalog);
    else
      sqlite3_bind_int64(stmt_insert, 4, kFileRegular);

    if (sqlite3_step(stmt_insert) != SQLITE_DONE) {
      LogCvmfs(kLogQuota, kLogDebug, "could not insert into cache catalog");
      goto build_return;
    }
    sqlite3_reset(stmt_insert);
  }

  // Delete temporary table
  sql = "DELETE FROM fscache;";
  sqlerr = sqlite3_exec(db_, sql.c_str(), NULL, NULL, NULL);
  if (sqlerr != SQLITE_OK) {
    LogCvmfs(kLogQuota, kLogDebug, "could not clear temporary table (%d)",
             sqlerr);
    goto build_return;
  }

  seq_ = seq;
  result = true;
  LogCvmfs(kLogQuota, kLogDebug,
           "rebuilding finished, seqence %"PRIu64 ", gauge %"PRIu64,
           seq_, gauge_);

 build_return:
  if (stmt_insert) sqlite3_finalize(stmt_insert);
  if (stmt_select) sqlite3_finalize(stmt_select);
  if (dirp) closedir(dirp);
  return result;
}


static bool InitDatabase(const bool rebuild_database) {
  string sql;
  sqlite3_stmt *stmt;

  fd_lock_cachedb_ = LockFile(*cache_dir_ + "/lock_cachedb");
  if (fd_lock_cachedb_ < 0) {
    LogCvmfs(kLogCvmfs, kLogDebug, "failed to create cachedb lock");
    return false;
  }

  bool retry = false;
init_recover:
  const string db_file = (*cache_dir_) + "/cachedb";
  int err = sqlite3_open(db_file.c_str(), &db_);
  if (err != SQLITE_OK) {
    LogCvmfs(kLogQuota, kLogDebug, "could not open cache database (%d)", err);
    goto init_database_fail;
  }
  sql = "PRAGMA synchronous=0; PRAGMA locking_mode=EXCLUSIVE; "
  "PRAGMA auto_vacuum=1; "
  "CREATE TABLE IF NOT EXISTS cache_catalog (sha1 TEXT, size INTEGER, "
  "  acseq INTEGER, path TEXT, type INTEGER, pinned INTEGER, "
  "CONSTRAINT pk_cache_catalog PRIMARY KEY (sha1)); "
  "CREATE UNIQUE INDEX IF NOT EXISTS idx_cache_catalog_acseq "
  "  ON cache_catalog (acseq); "
  "CREATE TEMP TABLE fscache (sha1 TEXT, size INTEGER, actime INTEGER, "
  "CONSTRAINT pk_fscache PRIMARY KEY (sha1)); "
  "CREATE INDEX idx_fscache_actime ON fscache (actime); "
  "CREATE TABLE IF NOT EXISTS properties (key TEXT, value TEXT, "
  "  CONSTRAINT pk_properties PRIMARY KEY(key));";
  err = sqlite3_exec(db_, sql.c_str(), NULL, NULL, NULL);
  if (err != SQLITE_OK) {
    if (!retry) {
      retry = true;
      sqlite3_close(db_);
      unlink(db_file.c_str());
      unlink((db_file + "-journal").c_str());
      LogCvmfs(kLogQuota, kLogSyslog, "LRU database corrupted, re-building");
      goto init_recover;
    }
    LogCvmfs(kLogQuota, kLogDebug, "could not init cache database (failed: %s)",
             sql.c_str());
    goto init_database_fail;
  }

  // If this an old cache catalog,
  // add and initialize new columns to cache_catalog
  sql = "ALTER TABLE cache_catalog ADD type INTEGER; "
  "ALTER TABLE cache_catalog ADD pinned INTEGER";
  err = sqlite3_exec(db_, sql.c_str(), NULL, NULL, NULL);
  if (err == SQLITE_OK) {
    sql = "UPDATE cache_catalog SET type=" + StringifyInt(kFileRegular) + ";";
    err = sqlite3_exec(db_, sql.c_str(), NULL, NULL, NULL);
    if (err != SQLITE_OK) {
      LogCvmfs(kLogQuota, kLogDebug,
               "could not init cache database (failed: %s)", sql.c_str());
      UnlockFile(fd_lock_cachedb_);
      return false;
    }
  }

  // Set pinned back
  sql = "UPDATE cache_catalog SET pinned=0;";
  err = sqlite3_exec(db_, sql.c_str(), NULL, NULL, NULL);
  if (err != SQLITE_OK) {
    LogCvmfs(kLogQuota, kLogDebug, "could not init cache database (failed: %s)",
             sql.c_str());
    goto init_database_fail;
  }

  // Set schema version
  sql = "INSERT OR REPLACE INTO properties (key, value) "
  "VALUES ('schema', '1.0')";
  err = sqlite3_exec(db_, sql.c_str(), NULL, NULL, NULL);
  if (err != SQLITE_OK) {
    LogCvmfs(kLogQuota, kLogDebug, "could not init cache database (failed: %s)",
             sql.c_str());
    goto init_database_fail;
  }

  // Easy way out, no quota restrictions
  if (limit_ == 0) {
    gauge_ = 0;
    return true;
  }

  // If cache catalog is empty, recreate from file system
  sql = "SELECT count(*) FROM cache_catalog;";
  sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, NULL);
  if (sqlite3_step(stmt) == SQLITE_ROW) {
    if ((sqlite3_column_int64(stmt, 0)) == 0 || rebuild_database) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "CernVM-FS: building lru cache database...");
      if (!RebuildDatabase()) {
        LogCvmfs(kLogQuota, kLogDebug,
                 "could not build cache database from file system");
        goto init_database_fail;
      }
    }
  } else {
    LogCvmfs(kLogQuota, kLogDebug, "could not select on cache catalog");
    sqlite3_finalize(stmt);
    goto init_database_fail;
  }
  sqlite3_finalize(stmt);

  // How many bytes do we already have in cache?
  sql = "SELECT sum(size) FROM cache_catalog;";
  sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, NULL);
  if (sqlite3_step(stmt) == SQLITE_ROW) {
    gauge_ = sqlite3_column_int64(stmt, 0);
  } else {
    LogCvmfs(kLogQuota, kLogDebug, "could not determine cache size");
    sqlite3_finalize(stmt);
    goto init_database_fail;
  }
  sqlite3_finalize(stmt);

  // Highest seq-no?
  sql = "SELECT coalesce(max(acseq), 0) FROM cache_catalog;";
  sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, NULL);
  if (sqlite3_step(stmt) == SQLITE_ROW) {
    seq_ = sqlite3_column_int64(stmt, 0)+1;
  } else {
    LogCvmfs(kLogQuota, kLogDebug, "could not determine highest seq-no");
    sqlite3_finalize(stmt);
    goto init_database_fail;
  }
  sqlite3_finalize(stmt);

  // Prepare touch, new, remove statements
  sqlite3_prepare_v2(db_, "UPDATE cache_catalog SET acseq=:seq "
                     "WHERE sha1=:sha1;", -1, &stmt_touch_, NULL);
  sqlite3_prepare_v2(db_, "UPDATE cache_catalog SET pinned=0 "
                     "WHERE sha1=:sha1;", -1, &stmt_unpin_, NULL);
  sqlite3_prepare_v2(db_,
                     "INSERT OR REPLACE INTO cache_catalog "
                     "(sha1, size, acseq, path, type, pinned) "
                     "VALUES (:sha1, :s, :seq, :p, :t, :pin);", -1, &stmt_new_, NULL);
  sqlite3_prepare_v2(db_,
                     "SELECT size, pinned FROM cache_catalog WHERE sha1=:sha1;",
                     -1, &stmt_size_, NULL);
  sqlite3_prepare_v2(db_, "DELETE FROM cache_catalog WHERE sha1=:sha1;",
                     -1, &stmt_rm_, NULL);
  sqlite3_prepare_v2(db_,
                     "SELECT sha1, size FROM cache_catalog WHERE acseq=(SELECT min(acseq) "
                     "FROM cache_catalog WHERE pinned=0);", -1, &stmt_lru_, NULL);
  sqlite3_prepare_v2(db_,
                     ("SELECT path FROM cache_catalog WHERE type=" + StringifyInt(kFileRegular) +
                      ";").c_str(), -1, &stmt_list_, NULL);
  sqlite3_prepare_v2(db_, "SELECT path FROM cache_catalog WHERE pinned=1;",
                     -1, &stmt_list_pinned_, NULL);
  sqlite3_prepare_v2(db_,
                     ("SELECT path FROM cache_catalog WHERE type=" + StringifyInt(kFileCatalog) +
                      ";").c_str(), -1, &stmt_list_catalogs_, NULL);
  return true;

 init_database_fail:
  UnlockFile(fd_lock_cachedb_);
  sqlite3_close(db_);
  return false;
}


static void CloseDatabase() {
  if (stmt_list_catalogs_) sqlite3_finalize(stmt_list_catalogs_);
  if (stmt_list_pinned_) sqlite3_finalize(stmt_list_pinned_);
  if (stmt_list_) sqlite3_finalize(stmt_list_);
  if (stmt_lru_) sqlite3_finalize(stmt_lru_);
  if (stmt_rm_) sqlite3_finalize(stmt_rm_);
  if (stmt_size_) sqlite3_finalize(stmt_size_);
  if (stmt_touch_) sqlite3_finalize(stmt_touch_);
  if (stmt_unpin_) sqlite3_finalize(stmt_unpin_);
  if (stmt_new_) sqlite3_finalize(stmt_new_);
  if (db_) sqlite3_close(db_);
  UnlockFile(fd_lock_cachedb_);

  stmt_list_catalogs_ = NULL;
  stmt_list_pinned_ = NULL;
  stmt_list_ = NULL;
  stmt_rm_ = NULL;
  stmt_size_ = NULL;
  stmt_touch_ = NULL;
  stmt_new_ = NULL;
  db_ = NULL;

  delete pinned_chunks_;
  pinned_chunks_ = NULL;
}


/**
 * Connects to a running peer server.  Creates a peer server, if necessary.
 */
bool InitShared(const std::string &exe_path, const std::string &cache_dir,
                const uint64_t limit, const uint64_t cleanup_threshold)
{
  shared_ = true;
  spawned_ = true;
  cache_dir_ = new string(cache_dir);

  // Create lock file
  const int fd_lockfile = LockFile(*cache_dir_ + "/lock_cachemgr");
  if (fd_lockfile < 0) {
    LogCvmfs(kLogQuota, kLogDebug, "could not open lock file %s (%d)",
             (*cache_dir_ + "/lock_cachemgr").c_str(), errno);
    return false;
  }

  // Try to connect to pipe
  const string fifo_path = *cache_dir_ + "/cachemgr";
  LogCvmfs(kLogQuota, kLogDebug, "trying to connect to existing pipe");
  pipe_lru_[1] = open(fifo_path.c_str(), O_WRONLY | O_NONBLOCK);
  if (pipe_lru_[1] >= 0) {
    LogCvmfs(kLogQuota, kLogDebug, "connected to existing cache manager pipe");
    initialized_ = true;
    Nonblock2Block(pipe_lru_[1]);
    UnlockFile(fd_lockfile);
    GetLimits(&limit_, &cleanup_threshold_);
    LogCvmfs(kLogQuota, kLogDebug, "received limit %"PRIu64", threshold %"PRIu64,
             limit_, cleanup_threshold_);
    return true;
  }
  if (errno == ENXIO) {
    LogCvmfs(kLogQuota, kLogDebug, "left-over FIFO found, unlinking");
    unlink(fifo_path.c_str());
  }

  // Creating a new FIFO for the cache manager (to be bound later)
  int retval = mkfifo(fifo_path.c_str(), 0600);
  if (retval != 0) {
    LogCvmfs(kLogQuota, kLogDebug, "failed to create cache manager FIFO (%d)",
             errno);
    UnlockFile(fd_lockfile);
    return false;
  }

  // Create new cache manager
  int pipe_boot[2];
  int pipe_handshake[2];
  MakePipe(pipe_boot);
  MakePipe(pipe_handshake);

  vector<string> command_line;
  command_line.push_back(exe_path);
  command_line.push_back("__cachemgr__");
  command_line.push_back(*cache_dir_);
  command_line.push_back(StringifyInt(pipe_boot[1]));
  command_line.push_back(StringifyInt(pipe_handshake[0]));
  command_line.push_back(StringifyInt(limit));
  command_line.push_back(StringifyInt(cleanup_threshold));
  command_line.push_back(StringifyInt(cvmfs::foreground_));
  command_line.push_back(StringifyInt(GetLogSyslogLevel()));
  command_line.push_back(StringifyInt(GetLogSyslogFacility()));
  command_line.push_back(GetLogDebugFile());

  vector<int> preserve_filedes;
  preserve_filedes.push_back(0);
  preserve_filedes.push_back(1);
  preserve_filedes.push_back(2);
  preserve_filedes.push_back(pipe_boot[1]);
  preserve_filedes.push_back(pipe_handshake[0]);

  retval = ManagedExec(command_line, preserve_filedes, map<int, int>());
  if (!retval) {
    UnlockFile(fd_lockfile);
    ClosePipe(pipe_boot);
    ClosePipe(pipe_handshake);
    LogCvmfs(kLogQuota, kLogDebug, "failed to start cache manager");
    return false;
  }

  // Wait for cache manager to be ready
  close(pipe_boot[1]);
  close(pipe_handshake[0]);
  char buf;
  if (read(pipe_boot[0], &buf, 1) != 1) {
    UnlockFile(fd_lockfile);
    close(pipe_boot[0]);
    close(pipe_handshake[1]);
    LogCvmfs(kLogQuota, kLogDebug, "cache manager did not start");
    return false;
  }
  close(pipe_boot[0]);

  // Connect write end
  pipe_lru_[1] = open(fifo_path.c_str(), O_WRONLY | O_NONBLOCK);
  if (pipe_lru_[1] < 0) {
    LogCvmfs(kLogQuota, kLogDebug,
             "failed to connect to newly created FIFO (%d)", errno);
    close(pipe_handshake[1]);
    UnlockFile(fd_lockfile);
    return false;
  }

  // Finalize handshake
  buf = 'C';
  if (write(pipe_handshake[1], &buf, 1) != 1) {
    UnlockFile(fd_lockfile);
    close(pipe_handshake[1]);
    LogCvmfs(kLogQuota, kLogDebug, "could not finalize handshake");
    return false;
  }
  close(pipe_handshake[1]);

  Nonblock2Block(pipe_lru_[1]);
  LogCvmfs(kLogQuota, kLogDebug, "connected to a new cache manager");

  UnlockFile(fd_lockfile);

  initialized_ = true;
  GetLimits(&limit_, &cleanup_threshold_);
  LogCvmfs(kLogQuota, kLogDebug, "received limit %"PRIu64", threshold %"PRIu64,
           limit_, cleanup_threshold_);
  return true;
}


/**
 * Entry point for the shared cache manager process
 */
int MainCacheManager(int argc, char **argv) {
  LogCvmfs(kLogQuota, kLogDebug, "starting cache manager");
  int retval;

  retval = monitor::Init(".", "cachemgr", false);
  assert(retval);
  monitor::Spawn();

  shared_ = true;
  spawned_ = true;
  pinned_ = 0;
  pinned_chunks_ = new map<hash::Any, uint64_t>();

  // Process command line arguments
  cache_dir_ = new string(argv[2]);
  int pipe_boot = String2Int64(argv[3]);
  int pipe_handshake = String2Int64(argv[4]);
  limit_ = String2Int64(argv[5]);
  cleanup_threshold_ = String2Int64(argv[6]);
  int foreground = String2Int64(argv[7]);
  int syslog_level = String2Int64(argv[8]);
  int syslog_facility = String2Int64(argv[9]);
  const string logfile = argv[10];

  SetLogSyslogLevel(syslog_level);
  SetLogSyslogFacility(syslog_facility);
  if (logfile != "")
    SetLogDebugFile(logfile + ".cachemgr");

  if (!foreground)
    Daemonize();

  // Initialize pipe, open non-blocking as cvmfs is not yet connected
  const string crash_guard = *cache_dir_ + "/cachemgr.running";
  const bool rebuild = FileExists(crash_guard);
  retval = open(crash_guard.c_str(), O_RDONLY | O_CREAT, 0600);
  if (retval < 0) {
    LogCvmfs(kLogCvmfs, kLogSyslog | kLogDebug,
             "failed to create shared cache manager crash guard");
    return 1;
  }
  close(retval);
  if (!InitDatabase(rebuild))
    return 1;

  const string fifo_path = *cache_dir_ + "/cachemgr";
  pipe_lru_[0] = open(fifo_path.c_str(), O_RDONLY | O_NONBLOCK);
  if (pipe_lru_[0] < 0) {
    LogCvmfs(kLogQuota, kLogDebug, "failed to listen on FIFO (%d)", errno);
    return 1;
  }
  Nonblock2Block(pipe_lru_[0]);
  LogCvmfs(kLogQuota, kLogDebug, "shared cache manager listening");

  char buf = 'C';
  WritePipe(pipe_boot, &buf, 1);
  close(pipe_boot);

  ReadPipe(pipe_handshake, &buf, 1);
  close(pipe_handshake);
  LogCvmfs(kLogQuota, kLogDebug, "shared cache manager handshake done");

  MainCommandServer(NULL);
  unlink(fifo_path.c_str());
  CloseDatabase();
  unlink(crash_guard.c_str());

  monitor::Fini();

  return 0;
}


/**
 * Sets up parameters and SQL statements.
 * We don't check here whether cache is already too big.
 *
 * @param[in] dont_build Specifies if SQLite cache catalog has to be rebuild
 *    based on chache directory.  This is done anyway, if the catalog is empty.
 *
 * \return True on success, false otherwise.
 */
bool Init(const string &cache_dir, const uint64_t limit,
          const uint64_t cleanup_threshold, const bool rebuild_database)
{
  if ((cleanup_threshold >= limit) && (limit > 0)) {
    LogCvmfs(kLogQuota, kLogDebug,
             "invalid parameters: limit %"PRIu64", cleanup_threshold %"PRIu64"",
             limit, cleanup_threshold_);
    return false;
  }

  shared_ = false;
  spawned_ = false;

  limit_ = limit;
  pinned_ = 0;
  cleanup_threshold_ = cleanup_threshold;
  cache_dir_ = new string(cache_dir);
  pinned_chunks_ = new map<hash::Any, uint64_t>();

  // Initialize cache catalog
  if (!InitDatabase(rebuild_database))
    return false;

  MakePipe(pipe_lru_);
  initialized_ = true;

  return true;
}


/**
 * Spawns the LRU thread
 */
void Spawn() {
  if (spawned_ || (limit_ == 0))
    return;

  if (pthread_create(&thread_lru_, NULL, MainCommandServer, NULL) != 0) {
    LogCvmfs(kLogQuota, kLogDebug, "could not create lru thread");
    abort();
  }

  spawned_ = true;
}


/**
 * Cleanup, closes SQLite connections.
 */
void Fini() {
  if (!initialized_) return;

  delete cache_dir_;
  cache_dir_ = NULL;

  if (shared_) {
    // Most of cleanup is done elsewhen by shared cache manager
    close(pipe_lru_[1]);
    initialized_ = false;
    return;
  }

  if (spawned_) {
    char fin = 0;
    WritePipe(pipe_lru_[1], &fin, 1);
    close(pipe_lru_[1]);
    pthread_join(thread_lru_, NULL);
  } else {
    ClosePipe(pipe_lru_);
  }

  CloseDatabase();
  initialized_ = false;
}


/**
 * Cleans up in data cache, until cache size is below leave_size.
 * The actual unlinking is done in a separate process (fork).
 *
 * \return True on success, false otherwise
 */
bool Cleanup(const uint64_t leave_size) {
  if (!initialized_) return false;
  bool result;

  if (!spawned_) {
    return DoCleanup(leave_size);
  }

  int pipe_cleanup[2];
  MakeReturnPipe(pipe_cleanup);

  LruCommand cmd;
  cmd.command_type = kCleanup;
  cmd.size = leave_size;
  cmd.return_pipe = pipe_cleanup[1];

  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
  ReadHalfPipe(pipe_cleanup[0], &result, sizeof(result));
  CloseReturnPipe(pipe_cleanup);

  return result;
}


static void DoInsert(const hash::Any &hash, const uint64_t size,
                     const string &cvmfs_path, const bool pin)
{
  const string hash_str = hash.ToString();
  LogCvmfs(kLogQuota, kLogDebug, "insert into lru %s, path %s",
           hash_str.c_str(), cvmfs_path.c_str());
  const unsigned path_length = (cvmfs_path.length() > kMaxCvmfsPath) ?
    kMaxCvmfsPath : cvmfs_path.length();

  LruCommand *cmd = reinterpret_cast<LruCommand *>(
                      alloca(sizeof(LruCommand) + path_length));
  cmd->command_type = pin ? kPin : kInsert;
  cmd->size = size;
  memcpy(cmd->digest, hash.digest, hash.GetDigestSize());
  cmd->path_length = path_length;
  memcpy(reinterpret_cast<char *>(cmd)+sizeof(LruCommand),
         &cvmfs_path[0], path_length);
  WritePipe(pipe_lru_[1], cmd, sizeof(LruCommand) + path_length);
}


/**
 * Inserts a new file into cache catalog.  This file gets a new,
 * highest sequence number. Does cache cleanup if necessary.
 */
void Insert(const hash::Any &any_hash, const uint64_t size,
            const string &cvmfs_path)
{
  assert(initialized_);
  if (limit_ == 0) return;
  DoInsert(any_hash, size, cvmfs_path, false);
}


/**
 * Immediately inserts a new pinned catalog.
 * Does cache cleanup if necessary.
 *
 * \return True on success, false otherwise
 */
bool Pin(const hash::Any &hash, const uint64_t size,
         const string &cvmfs_path)
{
  assert(initialized_);
  if (limit_ == 0) return true;

  const string hash_str = hash.ToString();
  LogCvmfs(kLogQuota, kLogDebug, "pin into lru %s, path %s",
           hash_str.c_str(), cvmfs_path.c_str());

  // Has to run when not spawned yet
  if (!spawned_) {
    // Currently code duplication here, not sure if there is a more elegant way
    if (pinned_chunks_->find(hash) == pinned_chunks_->end()) {
      if ((cleanup_threshold_ > 0) && (pinned_ + size > cleanup_threshold_)) {
        LogCvmfs(kLogQuota, kLogDebug, "failed to insert %s (pinned), no space",
                 hash_str.c_str());
        return false;
      } else {
        (*pinned_chunks_)[hash] = size;
        pinned_ += size;
      }
    }
    bool exists = Contains(hash_str);
    if (!exists && (gauge_ + size > limit_)) {
      LogCvmfs(kLogQuota, kLogDebug, "over limit, gauge %lu, file size %lu",
               gauge_, size);
      int retval = DoCleanup(cleanup_threshold_);
      assert(retval != 0);
    }
    sqlite3_bind_text(stmt_new_, 1, &hash_str[0], hash_str.length(),
                      SQLITE_STATIC);
    sqlite3_bind_int64(stmt_new_, 2, size);
    sqlite3_bind_int64(stmt_new_, 3, seq_++);
    sqlite3_bind_text(stmt_new_, 4, &cvmfs_path[0], cvmfs_path.length(),
                      SQLITE_STATIC);
    sqlite3_bind_int64(stmt_new_, 5, kFileCatalog);
    sqlite3_bind_int64(stmt_new_, 6, 1);
    int retval = sqlite3_step(stmt_new_);
    assert((retval == SQLITE_DONE) || (retval == SQLITE_OK));
    sqlite3_reset(stmt_new_);
    if (!exists) gauge_ += size;
    return true;
  }

  int pipe_reserve[2];
  MakeReturnPipe(pipe_reserve);

  LruCommand cmd;
  cmd.command_type = kReserve;
  cmd.size = size;
  memcpy(cmd.digest, hash.digest, hash.GetDigestSize());
  cmd.return_pipe = pipe_reserve[1];
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
  bool result;
  ReadHalfPipe(pipe_reserve[0], &result, sizeof(result));
  CloseReturnPipe(pipe_reserve);

  if (!result) return false;
  DoInsert(hash, size, cvmfs_path, true);

  return true;
}


void Unpin(const hash::Any &hash) {
  if (limit_ == 0) return;
  LogCvmfs(kLogQuota, kLogDebug, "Unpin %s", hash.ToString().c_str());

  LruCommand cmd;
  cmd.command_type = kUnpin;
  memcpy(cmd.digest, hash.digest, hash.GetDigestSize());
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
}


/**
 * Updates the sequence number of the file specified by the hash.
 */
void Touch(const hash::Any &hash) {
  assert(initialized_);
  if (limit_ == 0) return;

  LruCommand cmd;
  cmd.command_type = kTouch;
  memcpy(cmd.digest, hash.digest, hash.GetDigestSize());
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
}


/**
 * Removes a chunk from cache, if it exists.
 */
void Remove(const hash::Any &hash) {
  assert(initialized_);
  string hash_str = hash.ToString();

  if (limit_ != 0) {
    LruCommand cmd;
    cmd.command_type = kRemove;
    memcpy(cmd.digest, hash.digest, hash.GetDigestSize());
    WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
  }

  unlink(((*cache_dir_) + hash.MakePath(1, 2)).c_str());
}


static vector<string> DoList(const CommandType list_command) {
  vector<string> result;
  if (!initialized_) {
    result.push_back("--CACHE UNMANAGED--");
    return result;
  }

  int pipe_list[2];
  MakeReturnPipe(pipe_list);
  char path_buffer[kMaxCvmfsPath];

  LruCommand cmd;
  cmd.command_type = list_command;
  cmd.return_pipe = pipe_list[1];
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));

  int length;
  do {
    ReadHalfPipe(pipe_list[0], &length, sizeof(length));
    if (length > 0) {
      ReadPipe(pipe_list[0], path_buffer, length);
      result.push_back(string(path_buffer, length));
    }
  } while (length >= 0);

  CloseReturnPipe(pipe_list);
  return result;
}


/**
 * Lists all path names from the cache db.
 */
vector<string> List() {
  return DoList(kList);
}


/**
 * Lists all pinned files from the cache db.
 */
vector<string> ListPinned() {
  return DoList(kListPinned);
}


/**
 * Lists all catalog files from the cache db.
 */
vector<string> ListCatalogs() {
  return DoList(kListCatalogs);
}


/**
 * Since we only cleanup until cleanup_threshold, we can only add
 * files smaller than limit-cleanup_threshold.
 */
uint64_t GetMaxFileSize() {
  if (!initialized_) return 0;
  if (limit_ == 0) return INT64_MAX;
  return limit_ - cleanup_threshold_;
}


uint64_t GetCapacity() {
  if (!initialized_) return 0;
  return limit_;
}


static void GetStatus(uint64_t *gauge, uint64_t *pinned) {
  if (!initialized_) {
    *gauge = 0;
    *pinned = 0;
    return;
  }
  int pipe_status[2];
  MakeReturnPipe(pipe_status);

  LruCommand cmd;
  cmd.command_type = kStatus;
  cmd.return_pipe = pipe_status[1];
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
  ReadHalfPipe(pipe_status[0], gauge, sizeof(*gauge));
  ReadPipe(pipe_status[0], pinned, sizeof(*pinned));
  CloseReturnPipe(pipe_status);
}


uint64_t GetSize() {
  if (!spawned_) return gauge_;

  uint64_t gauge, size_pinned;
  GetStatus(&gauge, &size_pinned);
  return gauge;
}


uint64_t GetSizePinned() {
  if (!spawned_) return pinned_;

  uint64_t gauge, size_pinned;
  GetStatus(&gauge, &size_pinned);
  return size_pinned;
}


static void GetLimits(uint64_t *limit, uint64_t *cleanup_threshold) {
  if (!initialized_) {
    *limit = 0;
    *cleanup_threshold = 0;
    return;
  }
  int pipe_limits[2];
  MakeReturnPipe(pipe_limits);

  LruCommand cmd;
  cmd.command_type = kLimits;
  cmd.return_pipe = pipe_limits[1];
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
  ReadHalfPipe(pipe_limits[0], limit, sizeof(*limit));
  ReadPipe(pipe_limits[0], cleanup_threshold, sizeof(*cleanup_threshold));
  CloseReturnPipe(pipe_limits);
}

pid_t GetPid() {
  if (!initialized_ || !shared_ || !spawned_) {
    return cvmfs::pid_;
  }

  pid_t result;
  int pipe_pid[2];
  MakeReturnPipe(pipe_pid);

  LruCommand cmd;
  cmd.command_type = kPid;
  cmd.return_pipe = pipe_pid[1];
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
  ReadHalfPipe(pipe_pid[0], &result, sizeof(result));
  CloseReturnPipe(pipe_pid);
  return result;
}


string GetMemoryUsage() {
  return "TBD\n";
/*  if (limit == 0)
    return "LRU not active\n";

  string result("LRU:\n");
  int current = 0;
  int highwater = 0;

  pthread_mutex_lock(&mutex);

  sqlite3_db_status(db, SQLITE_DBSTATUS_LOOKASIDE_USED,
                    &current, &highwater, 0);
  result += "  Number of lookaside slots used " + StringifyInt(current) +
            " / " + StringifyInt(highwater) + "\n";

  sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_USED, &current, &highwater, 0);
  result += "  Page cache used " + StringifyInt(current/1024) + " KB\n";

  sqlite3_db_status(db, SQLITE_DBSTATUS_SCHEMA_USED, &current, &highwater, 0);
  result += "  Schema memory used " + StringifyInt(current/1024) + " KB\n";

  sqlite3_db_status(db, SQLITE_DBSTATUS_STMT_USED, &current, &highwater, 0);
  result += "  Prepared statements memory used " + StringifyInt(current/1024) +
            " KB\n";

  pthread_mutex_unlock(&mutex);

  return result;*/
}

}  // namespace quota
