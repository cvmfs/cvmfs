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
#include "quota_posix.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <sys/dir.h>
#include <sys/stat.h>
#ifndef __APPLE__
#include <sys/statfs.h>
#endif
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "crypto/hash.h"
#include "duplex_sqlite3.h"
#include "monitor.h"
#include "statistics.h"
#include "util/concurrency.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/smalloc.h"
#include "util/string.h"

using namespace std;  // NOLINT


int PosixQuotaManager::BindReturnPipe(int pipe_wronly) {
  if (!shared_)
    return pipe_wronly;

  // Connect writer's end
  int result =
    open((workspace_dir_ + "/pipe" + StringifyInt(pipe_wronly)).c_str(),
         O_WRONLY | O_NONBLOCK);
  if (result >= 0) {
    Nonblock2Block(result);
  } else {
    LogCvmfs(kLogQuota, kLogDebug | kLogSyslogErr,
             "failed to bind return pipe (%d)", errno);
  }
  return result;
}


void PosixQuotaManager::CheckHighPinWatermark() {
  const uint64_t watermark = kHighPinWatermark*cleanup_threshold_/100;
  if ((cleanup_threshold_ > 0) && (pinned_ > watermark)) {
    LogCvmfs(kLogQuota, kLogDebug | kLogSyslogWarn,
             "high watermark of pinned files (%" PRIu64 "M > %" PRIu64 "M)",
             pinned_/(1024*1024), watermark/(1024*1024));
    BroadcastBackchannels("R");  // clients: please release pinned catalogs
  }
}


void PosixQuotaManager::CleanupPipes() {
  DIR *dirp = opendir(workspace_dir_.c_str());
  assert(dirp != NULL);

  platform_dirent64 *dent;
  bool found_leftovers = false;
  while ((dent = platform_readdir(dirp)) != NULL) {
    const string name = dent->d_name;
    const string path = workspace_dir_ + "/" + name;
    platform_stat64 info;
    int retval = platform_stat(path.c_str(), &info);
    if (retval != 0)
      continue;
    if (S_ISFIFO(info.st_mode) && (name.substr(0, 4) == "pipe")) {
      if (!found_leftovers) {
        LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogWarn,
                 "removing left-over FIFOs from cache directory");
      }
      found_leftovers = true;
      unlink(path.c_str());
    }
  }
  closedir(dirp);
}


/**
 * Cleans up in data cache, until cache size is below leave_size.
 * The actual unlinking is done in a separate process (fork).
 *
 * \return True on success, false otherwise
 */
bool PosixQuotaManager::Cleanup(const uint64_t leave_size) {
  if (!spawned_)
    return DoCleanup(leave_size);

  bool result;
  int pipe_cleanup[2];
  MakeReturnPipe(pipe_cleanup);

  LruCommand cmd;
  cmd.command_type = kCleanup;
  cmd.size = leave_size;
  cmd.return_pipe = pipe_cleanup[1];

  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
  ManagedReadHalfPipe(pipe_cleanup[0], &result, sizeof(result));
  CloseReturnPipe(pipe_cleanup);

  return result;
}


void PosixQuotaManager::CloseDatabase() {
  if (stmt_list_catalogs_) sqlite3_finalize(stmt_list_catalogs_);
  if (stmt_list_pinned_) sqlite3_finalize(stmt_list_pinned_);
  if (stmt_list_volatile_) sqlite3_finalize(stmt_list_volatile_);
  if (stmt_list_) sqlite3_finalize(stmt_list_);
  if (stmt_lru_) sqlite3_finalize(stmt_lru_);
  if (stmt_rm_) sqlite3_finalize(stmt_rm_);
  if (stmt_rm_batch_) sqlite3_finalize(stmt_rm_batch_);
  if (stmt_size_) sqlite3_finalize(stmt_size_);
  if (stmt_touch_) sqlite3_finalize(stmt_touch_);
  if (stmt_unpin_) sqlite3_finalize(stmt_unpin_);
  if (stmt_block_) sqlite3_finalize(stmt_block_);
  if (stmt_unblock_) sqlite3_finalize(stmt_unblock_);
  if (stmt_new_) sqlite3_finalize(stmt_new_);
  if (database_) sqlite3_close(database_);
  UnlockFile(fd_lock_cachedb_);

  stmt_list_catalogs_ = NULL;
  stmt_list_pinned_ = NULL;
  stmt_list_volatile_ = NULL;
  stmt_list_ = NULL;
  stmt_rm_ = NULL;
  stmt_rm_batch_ = NULL;
  stmt_size_ = NULL;
  stmt_touch_ = NULL;
  stmt_unpin_ = NULL;
  stmt_block_ = NULL;
  stmt_unblock_ = NULL;
  stmt_new_ = NULL;
  database_ = NULL;

  pinned_chunks_.clear();
}


void PosixQuotaManager::CloseReturnPipe(int pipe[2]) {
  if (shared_) {
    close(pipe[0]);
    UnlinkReturnPipe(pipe[1]);
  } else {
    ClosePipe(pipe);
  }
}


bool PosixQuotaManager::Contains(const string &hash_str) {
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


void PosixQuotaManager::CheckFreeSpace() {
  if ((limit_ == 0) || (gauge_ >= limit_))
    return;

  struct statvfs vfs_info;
  int retval = statvfs((cache_dir_ + "/cachedb").c_str(), &vfs_info);
  if (retval != 0) {
    LogCvmfs(kLogQuota, kLogDebug | kLogSyslogWarn,
             "failed to query %s for free space (%d)",
             cache_dir_.c_str(), errno);
    return;
  }
  int64_t free_space_byte = vfs_info.f_bavail * vfs_info.f_bsize;
  LogCvmfs(kLogQuota, kLogDebug, "free space: %" PRId64 " MB",
           free_space_byte / (1024 * 1024));

  int64_t required_byte = limit_ - gauge_;
  if (free_space_byte < required_byte) {
    LogCvmfs(kLogQuota, kLogSyslogWarn,
             "too little free space on the file system hosting the cache,"
             " %" PRId64 " MB available",
             free_space_byte / (1024 * 1024));
  }
}


PosixQuotaManager *PosixQuotaManager::Create(
  const string &cache_workspace,
  const uint64_t limit,
  const uint64_t cleanup_threshold,
  const bool rebuild_database)
{
  if (cleanup_threshold >= limit) {
    LogCvmfs(kLogQuota, kLogDebug, "invalid parameters: limit %" PRIu64 ", "
             "cleanup_threshold %" PRIu64, limit, cleanup_threshold);
    return NULL;
  }

  PosixQuotaManager *quota_manager =
    new PosixQuotaManager(limit, cleanup_threshold, cache_workspace);

  // Initialize cache catalog
  if (!quota_manager->InitDatabase(rebuild_database)) {
    delete quota_manager;
    return NULL;
  }
  quota_manager->CheckFreeSpace();
  MakePipe(quota_manager->pipe_lru_);

  quota_manager->protocol_revision_ = kProtocolRevision;
  quota_manager->initialized_ = true;
  return quota_manager;
}


/**
 * Connects to a running shared local quota manager.  Creates one if necessary.
 */
PosixQuotaManager *PosixQuotaManager::CreateShared(
  const std::string &exe_path,
  const std::string &cache_workspace,
  const uint64_t limit,
  const uint64_t cleanup_threshold,
  bool foreground)
{
  string cache_dir;
  string workspace_dir;
  ParseDirectories(cache_workspace, &cache_dir, &workspace_dir);

  pid_t new_cachemgr_pid;

  // Create lock file: only one fuse client at a time
  const int fd_lockfile = LockFile(workspace_dir + "/lock_cachemgr");
  if (fd_lockfile < 0) {
    LogCvmfs(kLogQuota, kLogDebug, "could not open lock file %s (%d)",
             (workspace_dir + "/lock_cachemgr").c_str(), errno);
    return NULL;
  }

  PosixQuotaManager *quota_mgr =
    new PosixQuotaManager(limit, cleanup_threshold, cache_workspace);
  quota_mgr->shared_ = true;
  quota_mgr->spawned_ = true;

  // Try to connect to pipe
  const string fifo_path = workspace_dir + "/cachemgr";
  LogCvmfs(kLogQuota, kLogDebug, "trying to connect to existing pipe");
  quota_mgr->pipe_lru_[1] = open(fifo_path.c_str(), O_WRONLY | O_NONBLOCK);
  if (quota_mgr->pipe_lru_[1] >= 0) {
    const int fd_lockfile_rw = open((workspace_dir + "/lock_cachemgr").c_str(), O_RDWR, 0600);
    unsigned lockfile_magicnumber = 0;
    const ssize_t result_mn = SafeRead(fd_lockfile_rw, &lockfile_magicnumber, sizeof(lockfile_magicnumber));
    const ssize_t result = SafeRead(fd_lockfile_rw, &new_cachemgr_pid, sizeof(new_cachemgr_pid));
    close(fd_lockfile_rw);

    if ((lockfile_magicnumber != kLockFileMagicNumber) || (result < 0) || (result_mn < 0)
         || (static_cast<size_t>(result) < sizeof(new_cachemgr_pid))) {
      if (result != 0) {
        LogCvmfs(kLogQuota, kLogDebug | kLogSyslogErr,
                 "could not read cache manager pid from lockfile");
        UnlockFile(fd_lockfile);
        delete quota_mgr;
        return NULL;
      } else {
        // support reload from old versions of the cache manager
        // lock file is empty in this case, try a plain ReadHalfPipe to get pid
        quota_mgr->SetCacheMgrPid(quota_mgr->GetPid());
      }
    } else {
      quota_mgr->SetCacheMgrPid(new_cachemgr_pid);
    }


    LogCvmfs(kLogQuota, kLogDebug, "connected to existing cache manager pipe");
    quota_mgr->initialized_ = true;
    Nonblock2Block(quota_mgr->pipe_lru_[1]);
    UnlockFile(fd_lockfile);
    quota_mgr->GetLimits(&quota_mgr->limit_, &quota_mgr->cleanup_threshold_);
    LogCvmfs(kLogQuota, kLogDebug,
             "received limit %" PRIu64 ", threshold %" PRIu64,
             quota_mgr->limit_, quota_mgr->cleanup_threshold_);
    if (FileExists(workspace_dir + "/cachemgr.protocol")) {
      quota_mgr->protocol_revision_ = quota_mgr->GetProtocolRevision();
      LogCvmfs(kLogQuota, kLogDebug, "connected protocol revision %u",
               quota_mgr->protocol_revision_);
    } else {
      LogCvmfs(kLogQuota, kLogDebug, "connected to ancient cache manager");
    }
    return quota_mgr;
  }
  const int connect_error = errno;

  // Lock file: let existing cache manager finish first
  const int fd_lockfile_fifo = LockFile(workspace_dir + "/lock_cachemgr.fifo");
  if (fd_lockfile_fifo < 0) {
    LogCvmfs(kLogQuota, kLogDebug, "could not open lock file %s (%d)",
             (workspace_dir + "/lock_cachemgr.fifo").c_str(), errno);
    UnlockFile(fd_lockfile);
    delete quota_mgr;
    return NULL;
  }
  UnlockFile(fd_lockfile_fifo);

  if (connect_error == ENXIO) {
    LogCvmfs(kLogQuota, kLogDebug, "left-over FIFO found, unlinking");
    unlink(fifo_path.c_str());
  }

  // Creating a new FIFO for the cache manager (to be bound later)
  int retval = mkfifo(fifo_path.c_str(), 0600);
  if (retval != 0) {
    LogCvmfs(kLogQuota, kLogDebug, "failed to create cache manager FIFO (%d)",
             errno);
    UnlockFile(fd_lockfile);
    delete quota_mgr;
    return NULL;
  }

  // Create new cache manager
  int pipe_boot[2];
  int pipe_handshake[2];
  MakePipe(pipe_boot);
  MakePipe(pipe_handshake);

  vector<string> command_line;
  command_line.push_back(exe_path);
  command_line.push_back("__cachemgr__");
  command_line.push_back(cache_workspace);
  command_line.push_back(StringifyInt(pipe_boot[1]));
  command_line.push_back(StringifyInt(pipe_handshake[0]));
  command_line.push_back(StringifyInt(limit));
  command_line.push_back(StringifyInt(cleanup_threshold));
  // do not propagate foreground in order to reliably get pid from exec
  // instead, daemonize right here
  command_line.push_back(StringifyInt(true)); //foreground
  command_line.push_back(StringifyInt(GetLogSyslogLevel()));
  command_line.push_back(StringifyInt(GetLogSyslogFacility()));
  command_line.push_back(GetLogDebugFile() + ":" + GetLogMicroSyslog());

  set<int> preserve_filedes;
  preserve_filedes.insert(0);
  preserve_filedes.insert(1);
  preserve_filedes.insert(2);
  preserve_filedes.insert(pipe_boot[1]);
  preserve_filedes.insert(pipe_handshake[0]);

  if (foreground) {
    retval = ManagedExec(command_line, preserve_filedes, map<int, int>(),
                         /*drop_credentials*/ false,
                         /*clear_env*/ false,
                         /*double_fork*/  true,
                         &new_cachemgr_pid);
  } else {
    retval = ExecAsDaemon(command_line, &new_cachemgr_pid);
  }
  if (!retval) {
    UnlockFile(fd_lockfile);
    ClosePipe(pipe_boot);
    ClosePipe(pipe_handshake);
    delete quota_mgr;
    LogCvmfs(kLogQuota, kLogDebug, "failed to start cache manager");
    return NULL;
  }
  LogCvmfs(kLogQuota, kLogDebug, "new cache manager pid: %d", new_cachemgr_pid);
  quota_mgr->SetCacheMgrPid(new_cachemgr_pid);
  const int fd_lockfile_rw = open((workspace_dir + "/lock_cachemgr").c_str(), O_RDWR | O_TRUNC, 0600);
  const unsigned magic_number = PosixQuotaManager::kLockFileMagicNumber;
  const bool result_mn = SafeWrite(fd_lockfile_rw, &magic_number, sizeof(magic_number));
  const bool result = SafeWrite(fd_lockfile_rw, &new_cachemgr_pid, sizeof(new_cachemgr_pid));
  if (!result || !result_mn) {
    PANIC(kLogSyslogErr, "could not write cache manager pid to lockfile");
  }

  close(fd_lockfile_rw);
  // Wait for cache manager to be ready
  close(pipe_boot[1]);
  close(pipe_handshake[0]);
  char buf;
  if (read(pipe_boot[0], &buf, 1) != 1) {
    UnlockFile(fd_lockfile);
    close(pipe_boot[0]);
    close(pipe_handshake[1]);
    delete quota_mgr;
    LogCvmfs(kLogQuota, kLogDebug | kLogSyslogErr,
             "cache manager did not start");
    return NULL;
  }
  close(pipe_boot[0]);

  // Connect write end
  quota_mgr->pipe_lru_[1] = open(fifo_path.c_str(), O_WRONLY | O_NONBLOCK);
  if (quota_mgr->pipe_lru_[1] < 0) {
    LogCvmfs(kLogQuota, kLogDebug,
             "failed to connect to newly created FIFO (%d)", errno);
    close(pipe_handshake[1]);
    UnlockFile(fd_lockfile);
    delete quota_mgr;
    return NULL;
  }

  // Finalize handshake
  buf = 'C';
  if (write(pipe_handshake[1], &buf, 1) != 1) {
    UnlockFile(fd_lockfile);
    close(pipe_handshake[1]);
    LogCvmfs(kLogQuota, kLogDebug, "could not finalize handshake");
    delete quota_mgr;
    return NULL;
  }
  close(pipe_handshake[1]);

  Nonblock2Block(quota_mgr->pipe_lru_[1]);
  LogCvmfs(kLogQuota, kLogDebug, "connected to a new cache manager");
  quota_mgr->protocol_revision_ = kProtocolRevision;

  UnlockFile(fd_lockfile);

  quota_mgr->initialized_ = true;
  quota_mgr->GetLimits(&quota_mgr->limit_, &quota_mgr->cleanup_threshold_);
  LogCvmfs(kLogQuota, kLogDebug, "received limit %" PRIu64 ", "
           "threshold %" PRIu64,
           quota_mgr->limit_, quota_mgr->cleanup_threshold_);
  return quota_mgr;
}


bool PosixQuotaManager::DoCleanup(const uint64_t leave_size) {
  if (gauge_ <= leave_size)
    return true;

  // TODO(jblomer) transaction
  LogCvmfs(kLogQuota, kLogSyslog | kLogDebug,
           "clean up cache until at most %lu KB is used", leave_size/1024);
  LogCvmfs(kLogQuota, kLogDebug, "gauge %" PRIu64, gauge_);
  cleanup_recorder_.Tick();

  bool result;
  vector<string> trash;

  // Note that volatile files start counting from the smallest int64 number:
  // the absolute sequence number with the first bit set in two's complement.
  // So -1 can be a marker that will never appear in the database.
  int64_t max_acseq = -1;
  do {
    sqlite3_reset(stmt_lru_);
    sqlite3_bind_int64(stmt_lru_, 1, (max_acseq == -1) ?
                       std::numeric_limits<int64_t>::min() : (max_acseq + 1));

    std::vector<EvictCandidate> candidates;
    candidates.reserve(kEvictBatchSize);
    string hash_str;
    unsigned i = 0;
    while (sqlite3_step(stmt_lru_) == SQLITE_ROW) {
      hash_str = reinterpret_cast<const char *>(
        sqlite3_column_text(stmt_lru_, 0));
      LogCvmfs(kLogQuota, kLogDebug, "add %s to candidates for eviction",
               hash_str.c_str());
      candidates.push_back(EvictCandidate(
        shash::MkFromHexPtr(shash::HexPtr(hash_str)),
        sqlite3_column_int64(stmt_lru_, 1),
        sqlite3_column_int64(stmt_lru_, 2)));
      i++;
    }
    if (candidates.empty()) {
      LogCvmfs(kLogQuota, kLogDebug, "no more entries to evict");
      break;
    }

    const unsigned N = candidates.size();
    for (i = 0; i < N; ++i) {
      // That's a critical condition.  We must not delete a not yet inserted
      // pinned file as it is already reserved (but will be inserted later).
      // Instead, set the pin bit in the db to not run into an endless loop
      if (pinned_chunks_.find(candidates[i].hash) != pinned_chunks_.end()) {
        hash_str = candidates[i].hash.ToString();
        LogCvmfs(kLogQuota, kLogDebug, "skip %s for eviction",
                 hash_str.c_str());
        sqlite3_bind_text(stmt_block_, 1, &hash_str[0], hash_str.length(),
                          SQLITE_STATIC);
        result = (sqlite3_step(stmt_block_) == SQLITE_DONE);
        sqlite3_reset(stmt_block_);
        assert(result);
        continue;
      }

      trash.push_back(cache_dir_ + "/" +
                      candidates[i].hash.MakePathWithoutSuffix());
      gauge_ -= candidates[i].size;
      max_acseq = candidates[i].acseq;
      LogCvmfs(kLogQuota, kLogDebug, "lru cleanup %s, new gauge %" PRIu64,
               candidates[i].hash.ToString().c_str(), gauge_);

      if (gauge_ <= leave_size)
        break;
    }
  } while (gauge_ > leave_size);

  if (max_acseq != -1) {
    sqlite3_bind_int64(stmt_rm_batch_, 1, max_acseq);
    result = (sqlite3_step(stmt_rm_batch_) == SQLITE_DONE);
    assert(result);
    sqlite3_reset(stmt_rm_batch_);

    result = (sqlite3_step(stmt_unblock_) == SQLITE_DONE);
    sqlite3_reset(stmt_unblock_);
    assert(result);
  }

  if (!EmptyTrash(trash))
    return false;

  if (gauge_ > leave_size) {
    LogCvmfs(kLogQuota, kLogDebug | kLogSyslogWarn,
             "request to clean until %" PRIu64 ", "
             "but effective gauge is %" PRIu64, leave_size, gauge_);
    return false;
  }
  return true;
}

bool PosixQuotaManager::EmptyTrash(const std::vector<std::string> &trash) {
  if (trash.empty())
    return true;

  if (async_delete_) {
    // Double fork avoids zombie, forked removal process must not flush file
    // buffers
    pid_t pid;
    int statloc;
    if ((pid = fork()) == 0) {
      // TODO(jblomer): eviciting files in the cache should perhaps become a
      // thread. This would also allow to block the chunks and prevent the
      // race with re-insertion. Then again, a thread can block umount.
#ifndef DEBUGMSG
      CloseAllFildes(std::set<int>());
#endif
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
  } else {  // !async_delete_
    for (unsigned i = 0, iEnd = trash.size(); i < iEnd; ++i) {
      LogCvmfs(kLogQuota, kLogDebug, "unlink %s", trash[i].c_str());
      unlink(trash[i].c_str());
    }
  }
  return true;
}


void PosixQuotaManager::DoInsert(
  const shash::Any &hash,
  const uint64_t size,
  const string &description,
  const CommandType command_type)
{
  const string hash_str = hash.ToString();
  LogCvmfs(kLogQuota, kLogDebug, "insert into lru %s, path %s, method %d",
           hash_str.c_str(), description.c_str(), command_type);
  const unsigned desc_length = (description.length() > kMaxDescription) ?
    kMaxDescription : description.length();

  LruCommand *cmd =
    reinterpret_cast<LruCommand *>(alloca(sizeof(LruCommand) + desc_length));
  new (cmd) LruCommand;
  cmd->command_type = command_type;
  cmd->SetSize(size);
  cmd->StoreHash(hash);
  cmd->desc_length = desc_length;
  memcpy(reinterpret_cast<char *>(cmd)+sizeof(LruCommand),
         &description[0], desc_length);
  WritePipe(pipe_lru_[1], cmd, sizeof(LruCommand) + desc_length);
}


vector<string> PosixQuotaManager::DoList(const CommandType list_command) {
  vector<string> result;

  int pipe_list[2];
  MakeReturnPipe(pipe_list);
  char description_buffer[kMaxDescription];

  LruCommand cmd;
  cmd.command_type = list_command;
  cmd.return_pipe = pipe_list[1];
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));

  int length;
  do {
    ManagedReadHalfPipe(pipe_list[0], &length, sizeof(length));
    if (length > 0) {
      ReadPipe(pipe_list[0], description_buffer, length);
      result.push_back(string(description_buffer, length));
    }
  } while (length >= 0);

  CloseReturnPipe(pipe_list);
  return result;
}


uint64_t PosixQuotaManager::GetCapacity() {
  if (limit_ != (uint64_t)(-1))
    return limit_;

  // Unrestricted cache, look at free space on cache dir fs
  struct statfs info;
  if (statfs(".", &info) == 0) {
    return info.f_bavail * info.f_bsize;
  } else {
    LogCvmfs(kLogQuota, kLogSyslogErr | kLogDebug,
             "failed to query file system info of cache (%d)", errno);
    return limit_;
  }
}


void PosixQuotaManager::GetLimits(uint64_t *limit, uint64_t *cleanup_threshold)
{
  int pipe_limits[2];
  MakeReturnPipe(pipe_limits);

  LruCommand cmd;
  cmd.command_type = kLimits;
  cmd.return_pipe = pipe_limits[1];
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
  ManagedReadHalfPipe(pipe_limits[0], limit, sizeof(*limit));
  ReadPipe(pipe_limits[0], cleanup_threshold, sizeof(*cleanup_threshold));
  CloseReturnPipe(pipe_limits);
}


/**
 * Since we only cleanup until cleanup_threshold, we can only add
 * files smaller than limit-cleanup_threshold.
 */
uint64_t PosixQuotaManager::GetMaxFileSize() {
  return limit_ - cleanup_threshold_;
}


pid_t PosixQuotaManager::GetPid() {
  if (!shared_ || !spawned_) {
    return getpid();
  }
  if (cachemgr_pid_) {
    return cachemgr_pid_;
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


uint32_t PosixQuotaManager::GetProtocolRevision() {
  int pipe_revision[2];
  MakeReturnPipe(pipe_revision);

  LruCommand cmd;
  cmd.command_type = kGetProtocolRevision;
  cmd.return_pipe = pipe_revision[1];
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));

  uint32_t revision;
  ManagedReadHalfPipe(pipe_revision[0], &revision, sizeof(revision));
  CloseReturnPipe(pipe_revision);
  return revision;
}


/**
 * Queries the shared local hard disk quota manager.
 */
void PosixQuotaManager::GetSharedStatus(uint64_t *gauge, uint64_t *pinned) {
  int pipe_status[2];
  MakeReturnPipe(pipe_status);

  LruCommand cmd;
  cmd.command_type = kStatus;
  cmd.return_pipe = pipe_status[1];
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
  ManagedReadHalfPipe(pipe_status[0], gauge, sizeof(*gauge));
  ReadPipe(pipe_status[0], pinned, sizeof(*pinned));
  CloseReturnPipe(pipe_status);
}

bool PosixQuotaManager::SetSharedLimit(uint64_t limit) {
  int pipe_set_limit[2];
  bool result;
  MakeReturnPipe(pipe_set_limit);

  LruCommand cmd;
  cmd.command_type = kSetLimit;
  cmd.size = limit;
  cmd.return_pipe = pipe_set_limit[1];
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
  ReadHalfPipe(pipe_set_limit[0], &result, sizeof(result));
  CloseReturnPipe(pipe_set_limit);
  return result;
}


bool PosixQuotaManager::SetLimit( uint64_t size) {
  if (!spawned_) {
     limit_ = size;
     cleanup_threshold_ = size/2;
     LogCvmfs(kLogQuota, kLogDebug | kLogSyslog, "Quota limit set to %lu / threshold %lu", limit_, cleanup_threshold_ ); 
     return true;
  }
  return SetSharedLimit(size);
}

uint64_t PosixQuotaManager::GetSize() {
  if (!spawned_) return gauge_;
  uint64_t gauge, size_pinned;
  GetSharedStatus(&gauge, &size_pinned);
  return gauge;
}


uint64_t PosixQuotaManager::GetSizePinned() {
  if (!spawned_) return pinned_;
  uint64_t gauge, size_pinned;
  GetSharedStatus(&gauge, &size_pinned);
  return size_pinned;
}


uint64_t PosixQuotaManager::GetCleanupRate(uint64_t period_s) {
  if (!spawned_ || (protocol_revision_ < 2)) return 0;
  uint64_t cleanup_rate;

  int pipe_cleanup_rate[2];
  MakeReturnPipe(pipe_cleanup_rate);
  LruCommand cmd;
  cmd.command_type = kCleanupRate;
  cmd.size = period_s;
  cmd.return_pipe = pipe_cleanup_rate[1];
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
  ManagedReadHalfPipe(pipe_cleanup_rate[0], &cleanup_rate, sizeof(cleanup_rate));
  CloseReturnPipe(pipe_cleanup_rate);

  return cleanup_rate;
}


bool PosixQuotaManager::InitDatabase(const bool rebuild_database) {
  string sql;
  sqlite3_stmt *stmt;

  fd_lock_cachedb_ = LockFile(workspace_dir_ + "/lock_cachedb");
  if (fd_lock_cachedb_ < 0) {
    LogCvmfs(kLogQuota, kLogDebug, "failed to create cachedb lock");
    return false;
  }

  bool retry = false;
  const string db_file = cache_dir_ + "/cachedb";
  if (rebuild_database) {
    LogCvmfs(kLogQuota, kLogDebug, "rebuild database, unlinking existing (%s)",
             db_file.c_str());
    unlink(db_file.c_str());
    unlink((db_file + "-journal").c_str());
  }

 init_recover:
  int err = sqlite3_open(db_file.c_str(), &database_);
  if (err != SQLITE_OK) {
    LogCvmfs(kLogQuota, kLogDebug, "could not open cache database (%d)", err);
    goto init_database_fail;
  }
  // TODO(reneme): make this a `QuotaDatabase : public sqlite::Database`
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
  err = sqlite3_exec(database_, sql.c_str(), NULL, NULL, NULL);
  if (err != SQLITE_OK) {
    if (!retry) {
      retry = true;
      sqlite3_close(database_);
      unlink(db_file.c_str());
      unlink((db_file + "-journal").c_str());
      LogCvmfs(kLogQuota, kLogSyslogWarn,
               "LRU database corrupted, re-building");
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
  err = sqlite3_exec(database_, sql.c_str(), NULL, NULL, NULL);
  if (err == SQLITE_OK) {
    sql = "UPDATE cache_catalog SET type=" + StringifyInt(kFileRegular) + ";";
    err = sqlite3_exec(database_, sql.c_str(), NULL, NULL, NULL);
    if (err != SQLITE_OK) {
      LogCvmfs(kLogQuota, kLogDebug,
               "could not init cache database (failed: %s)", sql.c_str());
      goto init_database_fail;
    }
  }

  // Set pinned back
  sql = "UPDATE cache_catalog SET pinned=0;";
  err = sqlite3_exec(database_, sql.c_str(), NULL, NULL, NULL);
  if (err != SQLITE_OK) {
    LogCvmfs(kLogQuota, kLogDebug, "could not init cache database (failed: %s)",
             sql.c_str());
    goto init_database_fail;
  }

  // Set schema version
  sql = "INSERT OR REPLACE INTO properties (key, value) "
  "VALUES ('schema', '1.0')";
  err = sqlite3_exec(database_, sql.c_str(), NULL, NULL, NULL);
  if (err != SQLITE_OK) {
    LogCvmfs(kLogQuota, kLogDebug, "could not init cache database (failed: %s)",
             sql.c_str());
    goto init_database_fail;
  }

  // If cache catalog is empty, recreate from file system
  sql = "SELECT count(*) FROM cache_catalog;";
  sqlite3_prepare_v2(database_, sql.c_str(), -1, &stmt, NULL);
  if (sqlite3_step(stmt) == SQLITE_ROW) {
    if ((sqlite3_column_int64(stmt, 0)) == 0 || rebuild_database) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "CernVM-FS: building lru cache database...");
      if (!RebuildDatabase()) {
        LogCvmfs(kLogQuota, kLogDebug,
                 "could not build cache database from file system");
        sqlite3_finalize(stmt);
        goto init_database_fail;
      }
    }
    sqlite3_finalize(stmt);
  } else {
    LogCvmfs(kLogQuota, kLogDebug, "could not select on cache catalog");
    sqlite3_finalize(stmt);
    goto init_database_fail;
  }

  // How many bytes do we already have in cache?
  sql = "SELECT sum(size) FROM cache_catalog;";
  sqlite3_prepare_v2(database_, sql.c_str(), -1, &stmt, NULL);
  if (sqlite3_step(stmt) == SQLITE_ROW) {
    gauge_ = sqlite3_column_int64(stmt, 0);
  } else {
    LogCvmfs(kLogQuota, kLogDebug, "could not determine cache size");
    sqlite3_finalize(stmt);
    goto init_database_fail;
  }
  sqlite3_finalize(stmt);

  // Highest seq-no?
  sql = "SELECT coalesce(max(acseq & (~(1<<63))), 0) FROM cache_catalog;";
  sqlite3_prepare_v2(database_, sql.c_str(), -1, &stmt, NULL);
  if (sqlite3_step(stmt) == SQLITE_ROW) {
    seq_ = sqlite3_column_int64(stmt, 0)+1;
  } else {
    LogCvmfs(kLogQuota, kLogDebug, "could not determine highest seq-no");
    sqlite3_finalize(stmt);
    goto init_database_fail;
  }
  sqlite3_finalize(stmt);

  // Prepare touch, new, remove statements
  sqlite3_prepare_v2(database_,
                     "UPDATE cache_catalog SET acseq=:seq | (acseq&(1<<63)) "
                     "WHERE sha1=:sha1;", -1, &stmt_touch_, NULL);
  sqlite3_prepare_v2(database_, "UPDATE cache_catalog SET pinned=0 "
                     "WHERE sha1=:sha1;", -1, &stmt_unpin_, NULL);
  sqlite3_prepare_v2(database_, "UPDATE cache_catalog SET pinned=2 "
                     "WHERE sha1=:sha1;", -1, &stmt_block_, NULL);
  sqlite3_prepare_v2(database_, "UPDATE cache_catalog SET pinned=1 "
                     "WHERE pinned=2;", -1, &stmt_unblock_, NULL);
  sqlite3_prepare_v2(database_,
                     "INSERT OR REPLACE INTO cache_catalog "
                     "(sha1, size, acseq, path, type, pinned) "
                     "VALUES (:sha1, :s, :seq, :p, :t, :pin);",
                     -1, &stmt_new_, NULL);
  sqlite3_prepare_v2(database_,
                     "SELECT size, pinned FROM cache_catalog WHERE sha1=:sha1;",
                     -1, &stmt_size_, NULL);
  sqlite3_prepare_v2(database_, "DELETE FROM cache_catalog WHERE sha1=:sha1;",
                     -1, &stmt_rm_, NULL);
  sqlite3_prepare_v2(database_,
                     "DELETE FROM cache_catalog WHERE acseq<=:a AND pinned<>2;",
                     -1, &stmt_rm_batch_, NULL);
  sqlite3_prepare_v2(database_, (std::string(
                     "SELECT sha1, size, acseq FROM cache_catalog "
                     "WHERE pinned<>2 AND acseq>=:a "
                     "ORDER BY acseq ASC "
                     "LIMIT ") + StringifyInt(kEvictBatchSize) + ";").c_str(),
                     -1, &stmt_lru_, NULL);
  sqlite3_prepare_v2(database_,
                     ("SELECT path FROM cache_catalog WHERE type=" +
                      StringifyInt(kFileRegular) +
                      ";").c_str(), -1, &stmt_list_, NULL);
  sqlite3_prepare_v2(database_,
                     "SELECT path FROM cache_catalog WHERE pinned<>0;",
                     -1, &stmt_list_pinned_, NULL);
  sqlite3_prepare_v2(database_,
                     "SELECT path FROM cache_catalog WHERE acseq < 0;",
                     -1, &stmt_list_volatile_, NULL);
  sqlite3_prepare_v2(database_,
                     ("SELECT path FROM cache_catalog WHERE type=" +
                      StringifyInt(kFileCatalog) +
                      ";").c_str(), -1, &stmt_list_catalogs_, NULL);
  return true;

 init_database_fail:
  sqlite3_close(database_);
  database_ = NULL;
  UnlockFile(fd_lock_cachedb_);
  return false;
}


/**
 * Inserts a new file into cache catalog.  This file gets a new,
 * highest sequence number. Does cache cleanup if necessary.
 */
void PosixQuotaManager::Insert(
  const shash::Any &any_hash,
  const uint64_t size,
  const string &description)
{
  DoInsert(any_hash, size, description, kInsert);
}


/**
 * Inserts a new file into cache catalog.  This file is marked as volatile
 * and gets a new highest sequence number with the first bit set.  Cache cleanup
 * treats these files with priority.
 */
void PosixQuotaManager::InsertVolatile(
  const shash::Any &any_hash,
  const uint64_t size,
  const string &description)
{
  DoInsert(any_hash, size, description, kInsertVolatile);
}


/**
 * Lists all path names from the cache db.
 */
vector<string> PosixQuotaManager::List() {
  return DoList(kList);
}


/**
 * Lists all pinned files from the cache db.
 */
vector<string> PosixQuotaManager::ListPinned() {
  return DoList(kListPinned);
}


/**
 * Lists all sqlite catalog files from the cache db.
 */
vector<string> PosixQuotaManager::ListCatalogs() {
  return DoList(kListCatalogs);
}


/**
 * Lists only files flagged as volatile (priority removal)
 */
vector<string> PosixQuotaManager::ListVolatile() {
  return DoList(kListVolatile);
}


/**
 * Entry point for the shared cache manager process
 */
int PosixQuotaManager::MainCacheManager(int argc, char **argv) {
	
  LogCvmfs(kLogQuota, kLogDebug, "starting quota manager");
  int retval;

  PosixQuotaManager shared_manager(0, 0, "");
  shared_manager.shared_ = true;
  shared_manager.spawned_ = true;
  shared_manager.pinned_ = 0;

  // Process command line arguments
  ParseDirectories(string(argv[2]),
                   &shared_manager.cache_dir_,
                   &shared_manager.workspace_dir_);
  int pipe_boot = String2Int64(argv[3]);
  int pipe_handshake = String2Int64(argv[4]);
  shared_manager.limit_ = String2Int64(argv[5]);
  shared_manager.cleanup_threshold_ = String2Int64(argv[6]);
  int foreground = String2Int64(argv[7]);
  int syslog_level = String2Int64(argv[8]);
  int syslog_facility = String2Int64(argv[9]);
  vector<string> logfiles = SplitString(argv[10], ':');

  SetLogSyslogLevel(syslog_level);
  SetLogSyslogFacility(syslog_facility);
  if ((logfiles.size() > 0) && (logfiles[0] != ""))
    SetLogDebugFile(logfiles[0] + ".cachemgr");
  if (logfiles.size() > 1)
    SetLogMicroSyslog(logfiles[1]);

  if (!foreground)
    Daemonize();

  UniquePtr<Watchdog> watchdog(Watchdog::Create(NULL));
  assert(watchdog.IsValid());
  watchdog->Spawn("./stacktrace.cachemgr");

  // Initialize pipe, open non-blocking as cvmfs is not yet connected
  const int fd_lockfile_fifo =
    LockFile(shared_manager.workspace_dir_ + "/lock_cachemgr.fifo");
  if (fd_lockfile_fifo < 0) {
    LogCvmfs(kLogQuota, kLogDebug | kLogSyslogErr, "could not open lock file "
             "%s (%d)",
             (shared_manager.workspace_dir_ + "/lock_cachemgr.fifo").c_str(),
             errno);
    return 1;
  }
  const string crash_guard = shared_manager.cache_dir_ + "/cachemgr.running";
  const bool rebuild = FileExists(crash_guard);
  retval = open(crash_guard.c_str(), O_RDONLY | O_CREAT, 0600);
  if (retval < 0) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
             "failed to create shared cache manager crash guard");
    UnlockFile(fd_lockfile_fifo);
    return 1;
  }
  close(retval);

  // Redirect SQlite temp directory to cache (global variable)
  const string tmp_dir = shared_manager.workspace_dir_;
  sqlite3_temp_directory =
    static_cast<char *>(sqlite3_malloc(tmp_dir.length() + 1));
  snprintf(sqlite3_temp_directory, tmp_dir.length() + 1, "%s", tmp_dir.c_str());

  // Cleanup leftover named pipes
  shared_manager.CleanupPipes();

  if (!shared_manager.InitDatabase(rebuild)) {
    UnlockFile(fd_lockfile_fifo);
    return 1;
  }
  shared_manager.CheckFreeSpace();

  // Save protocol revision to file.  If the file is not found, it indicates
  // to the client that the cache manager is from times before the protocol
  // was versioned.
  const string protocol_revision_path =
    shared_manager.workspace_dir_ + "/cachemgr.protocol";
  retval = open(protocol_revision_path.c_str(), O_WRONLY | O_CREAT, 0600);
  if (retval < 0) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
             "failed to open protocol revision file (%d)", errno);
    UnlockFile(fd_lockfile_fifo);
    return 1;
  }
  const string revision = StringifyInt(kProtocolRevision);
  int written = write(retval, revision.data(), revision.length());
  close(retval);
  if ((written < 0) || static_cast<unsigned>(written) != revision.length()) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
             "failed to write protocol revision (%d)", errno);
    UnlockFile(fd_lockfile_fifo);
    return 1;
  }

  const string fifo_path = shared_manager.workspace_dir_ + "/cachemgr";
  shared_manager.pipe_lru_[0] = open(fifo_path.c_str(), O_RDONLY | O_NONBLOCK);
  if (shared_manager.pipe_lru_[0] < 0) {
    LogCvmfs(kLogQuota, kLogDebug, "failed to listen on FIFO %s (%d)",
             fifo_path.c_str(), errno);
    UnlockFile(fd_lockfile_fifo);
    return 1;
  }
  Nonblock2Block(shared_manager.pipe_lru_[0]);
  LogCvmfs(kLogQuota, kLogDebug, "shared cache manager listening");

  char buf = 'C';
  WritePipe(pipe_boot, &buf, 1);
  close(pipe_boot);

  ReadPipe(pipe_handshake, &buf, 1);
  close(pipe_handshake);
  LogCvmfs(kLogQuota, kLogDebug, "shared cache manager handshake done");

  // Ensure that broken pipes from clients do not kill the cache manager
  signal(SIGPIPE, SIG_IGN);
  // Don't let Ctrl-C ungracefully kill interactive session
  signal(SIGINT, SIG_IGN);

  shared_manager.MainCommandServer(&shared_manager);
  unlink(fifo_path.c_str());
  unlink(protocol_revision_path.c_str());
  shared_manager.CloseDatabase();
  unlink(crash_guard.c_str());
  UnlockFile(fd_lockfile_fifo);

  if (sqlite3_temp_directory) {
    sqlite3_free(sqlite3_temp_directory);
    sqlite3_temp_directory = NULL;
  }

  return 0;
}


void *PosixQuotaManager::MainCommandServer(void *data) {
  PosixQuotaManager *quota_mgr = static_cast<PosixQuotaManager *>(data);

  LogCvmfs(kLogQuota, kLogDebug, "starting quota manager");
  sqlite3_soft_heap_limit(quota_mgr->kSqliteMemPerThread);

  LruCommand command_buffer[kCommandBufferSize];
  char description_buffer[kCommandBufferSize*kMaxDescription];
  unsigned num_commands = 0;

  while (read(quota_mgr->pipe_lru_[0], &command_buffer[num_commands],
              sizeof(command_buffer[0])) == sizeof(command_buffer[0]))
  {
    const CommandType command_type = command_buffer[num_commands].command_type;
    LogCvmfs(kLogQuota, kLogDebug, "received command %d", command_type);
    const uint64_t size = command_buffer[num_commands].GetSize();

    // Inserts and pins come with a description (usually a path)
    if ((command_type == kInsert) || (command_type == kInsertVolatile) ||
        (command_type == kPin) || (command_type == kPinRegular))
    {
      const int desc_length = command_buffer[num_commands].desc_length;
      ReadPipe(quota_mgr->pipe_lru_[0],
               &description_buffer[kMaxDescription*num_commands], desc_length);
    }

    // The protocol revision is returned immediately
    if (command_type == kGetProtocolRevision) {
      int return_pipe =
        quota_mgr->BindReturnPipe(command_buffer[num_commands].return_pipe);
      if (return_pipe < 0)
        continue;
      WritePipe(return_pipe, &quota_mgr->kProtocolRevision,
                sizeof(quota_mgr->kProtocolRevision));
      quota_mgr->UnbindReturnPipe(return_pipe);
      continue;
    }

    // The cleanup rate is returned immediately
    if (command_type == kCleanupRate) {
      int return_pipe =
        quota_mgr->BindReturnPipe(command_buffer[num_commands].return_pipe);
      if (return_pipe < 0)
        continue;
      uint64_t period_s = size;  // use the size field to transmit the period
      uint64_t rate = quota_mgr->cleanup_recorder_.GetNoTicks(period_s);
      WritePipe(return_pipe, &rate, sizeof(rate));
      quota_mgr->UnbindReturnPipe(return_pipe);
      continue;
    }

    if (command_type == kSetLimit) {
      const int return_pipe =
        quota_mgr->BindReturnPipe(command_buffer[num_commands].return_pipe);
      if (return_pipe < 0)
        continue;
      quota_mgr->limit_ = size; // use the size field to transmit the size
      quota_mgr->cleanup_threshold_ = size/2;
      LogCvmfs(kLogQuota, kLogDebug | kLogSyslog, "Quota limit set to %lu / threshold %lu", quota_mgr->limit_, quota_mgr->cleanup_threshold_ );
      bool ret = true;
      WritePipe(return_pipe, &ret, sizeof(ret));
      quota_mgr->UnbindReturnPipe(return_pipe);
      continue;
    }

    // Reservations are handled immediately and "out of band"
    if (command_type == kReserve) {
      bool success = true;
      int return_pipe =
        quota_mgr->BindReturnPipe(command_buffer[num_commands].return_pipe);
      if (return_pipe < 0)
        continue;

      const shash::Any hash = command_buffer[num_commands].RetrieveHash();
      const string hash_str(hash.ToString());
      LogCvmfs(kLogQuota, kLogDebug, "reserve %lu bytes for %s",
               size, hash_str.c_str());

      if (quota_mgr->pinned_chunks_.find(hash) ==
          quota_mgr->pinned_chunks_.end())
      {
        if ((quota_mgr->pinned_ + size) > quota_mgr->cleanup_threshold_) {
          LogCvmfs(kLogQuota, kLogDebug,
                   "failed to insert %s (pinned), no space", hash_str.c_str());
          success = false;
        } else {
          quota_mgr->pinned_chunks_[hash] = size;
          quota_mgr->pinned_ += size;
          quota_mgr->CheckHighPinWatermark();
        }
      }

      WritePipe(return_pipe, &success, sizeof(success));
      quota_mgr->UnbindReturnPipe(return_pipe);
      continue;
    }

    // Back channels are also handled out of band
    if (command_type == kRegisterBackChannel) {
      int return_pipe =
        quota_mgr->BindReturnPipe(command_buffer[num_commands].return_pipe);
      if (return_pipe < 0)
        continue;

      quota_mgr->UnlinkReturnPipe(command_buffer[num_commands].return_pipe);
      Block2Nonblock(return_pipe);  // back channels are opportunistic
      shash::Md5 hash;
      memcpy(hash.digest, command_buffer[num_commands].digest,
             shash::kDigestSizes[shash::kMd5]);

      quota_mgr->LockBackChannels();
      map<shash::Md5, int>::const_iterator iter =
        quota_mgr->back_channels_.find(hash);
      if (iter != quota_mgr->back_channels_.end()) {
        LogCvmfs(kLogQuota, kLogDebug | kLogSyslogWarn,
                 "closing left-over back channel %s", hash.ToString().c_str());
        close(iter->second);
      }
      quota_mgr->back_channels_[hash] = return_pipe;
      quota_mgr->UnlockBackChannels();

      char success = 'S';
      WritePipe(return_pipe, &success, sizeof(success));
      LogCvmfs(kLogQuota, kLogDebug, "register back channel %s on fd %d",
               hash.ToString().c_str(), return_pipe);

      continue;
    }

    if (command_type == kUnregisterBackChannel) {
      shash::Md5 hash;
      memcpy(hash.digest, command_buffer[num_commands].digest,
             shash::kDigestSizes[shash::kMd5]);

      quota_mgr->LockBackChannels();
      map<shash::Md5, int>::iterator iter =
        quota_mgr->back_channels_.find(hash);
      if (iter != quota_mgr->back_channels_.end()) {
        LogCvmfs(kLogQuota, kLogDebug,
                 "closing back channel %s", hash.ToString().c_str());
        close(iter->second);
        quota_mgr->back_channels_.erase(iter);
      } else {
        LogCvmfs(kLogQuota, kLogDebug | kLogSyslogWarn,
                 "did not find back channel %s", hash.ToString().c_str());
      }
      quota_mgr->UnlockBackChannels();

      continue;
    }

    // Unpinnings are also handled immediately with respect to the pinned gauge
    if (command_type == kUnpin) {
      const shash::Any hash = command_buffer[num_commands].RetrieveHash();
      const string hash_str(hash.ToString());

      map<shash::Any, uint64_t>::iterator iter =
        quota_mgr->pinned_chunks_.find(hash);
      if (iter != quota_mgr->pinned_chunks_.end()) {
        quota_mgr->pinned_ -= iter->second;
        quota_mgr->pinned_chunks_.erase(iter);
        // It can happen that files get pinned that were removed from the cache
        // (see cache.cc).  We fix this at this point, where we remove such
        // entries from the cache database.
        if (!FileExists(quota_mgr->cache_dir_ + "/" +
                        hash.MakePathWithoutSuffix()))
        {
          LogCvmfs(kLogQuota, kLogDebug,
                   "remove orphaned pinned hash %s from cache database",
                   hash_str.c_str());
          sqlite3_bind_text(quota_mgr->stmt_size_, 1, &hash_str[0],
                            hash_str.length(), SQLITE_STATIC);
          int retval;
          if ((retval = sqlite3_step(quota_mgr->stmt_size_)) == SQLITE_ROW) {
            uint64_t size = sqlite3_column_int64(quota_mgr->stmt_size_, 0);
            sqlite3_bind_text(quota_mgr->stmt_rm_, 1, &(hash_str[0]),
                              hash_str.length(), SQLITE_STATIC);
            retval = sqlite3_step(quota_mgr->stmt_rm_);
            if ((retval == SQLITE_DONE) || (retval == SQLITE_OK)) {
              quota_mgr->gauge_ -= size;
            } else {
              LogCvmfs(kLogQuota, kLogDebug | kLogSyslogErr,
                       "failed to delete %s (%d)", hash_str.c_str(), retval);
            }
            sqlite3_reset(quota_mgr->stmt_rm_);
          }
          sqlite3_reset(quota_mgr->stmt_size_);
        }
      } else {
        LogCvmfs(kLogQuota, kLogDebug, "this chunk was not pinned");
      }
    }

    // Immediate commands trigger flushing of the buffer
    bool immediate_command = (command_type == kCleanup) ||
      (command_type == kList) || (command_type == kListPinned) ||
      (command_type == kListCatalogs) || (command_type == kListVolatile) ||
      (command_type == kRemove) || (command_type == kStatus) ||
      (command_type == kLimits) || (command_type == kPid);
    if (!immediate_command) num_commands++;

    if ((num_commands == kCommandBufferSize) || immediate_command)
    {
      quota_mgr->ProcessCommandBunch(num_commands, command_buffer,
                                     description_buffer);
      if (!immediate_command) num_commands = 0;
    }

    if (immediate_command) {
      // Process cleanup, listings
      int return_pipe =
        quota_mgr->BindReturnPipe(command_buffer[num_commands].return_pipe);
      if (return_pipe < 0) {
        num_commands = 0;
        continue;
      }

      int retval;
      sqlite3_stmt *this_stmt_list = NULL;
      switch (command_type) {
        case kRemove: {
          const shash::Any hash = command_buffer[num_commands].RetrieveHash();
          const string hash_str = hash.ToString();
          LogCvmfs(kLogQuota, kLogDebug, "manually removing %s",
                   hash_str.c_str());
          bool success = false;

          sqlite3_bind_text(quota_mgr->stmt_size_, 1, &hash_str[0],
                            hash_str.length(), SQLITE_STATIC);
          int retval;
          if ((retval = sqlite3_step(quota_mgr->stmt_size_)) == SQLITE_ROW) {
            uint64_t size = sqlite3_column_int64(quota_mgr->stmt_size_, 0);
            uint64_t is_pinned = sqlite3_column_int64(quota_mgr->stmt_size_, 1);

            sqlite3_bind_text(quota_mgr->stmt_rm_, 1, &(hash_str[0]),
                              hash_str.length(), SQLITE_STATIC);
            retval = sqlite3_step(quota_mgr->stmt_rm_);
            if ((retval == SQLITE_DONE) || (retval == SQLITE_OK)) {
              success = true;
              quota_mgr->gauge_ -= size;
              if (is_pinned) {
                quota_mgr->pinned_chunks_.erase(hash);
                quota_mgr->pinned_ -= size;
              }
            } else {
              LogCvmfs(kLogQuota, kLogDebug | kLogSyslogErr,
                       "failed to delete %s (%d)", hash_str.c_str(), retval);
            }
            sqlite3_reset(quota_mgr->stmt_rm_);
          } else {
            // File does not exist
            success = true;
          }
          sqlite3_reset(quota_mgr->stmt_size_);

          WritePipe(return_pipe, &success, sizeof(success));
          break; }
        case kCleanup:
          retval = quota_mgr->DoCleanup(size);
          WritePipe(return_pipe, &retval, sizeof(retval));
          break;
        case kList:
          if (!this_stmt_list) this_stmt_list = quota_mgr->stmt_list_;
        case kListPinned:
          if (!this_stmt_list) this_stmt_list = quota_mgr->stmt_list_pinned_;
        case kListCatalogs:
          if (!this_stmt_list) this_stmt_list = quota_mgr->stmt_list_catalogs_;
        case kListVolatile:
          if (!this_stmt_list) this_stmt_list = quota_mgr->stmt_list_volatile_;

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
          WritePipe(return_pipe, &quota_mgr->gauge_, sizeof(quota_mgr->gauge_));
          WritePipe(return_pipe, &quota_mgr->pinned_,
                    sizeof(quota_mgr->pinned_));
          break;
        case kLimits:
          WritePipe(return_pipe, &quota_mgr->limit_, sizeof(quota_mgr->limit_));
          WritePipe(return_pipe, &quota_mgr->cleanup_threshold_,
                    sizeof(quota_mgr->cleanup_threshold_));
          break;
        case kPid: {
          pid_t pid = getpid();
          WritePipe(return_pipe, &pid, sizeof(pid));
          break;
        }
        default:
          PANIC(NULL);  // other types are handled by the bunch processor
      }
      quota_mgr->UnbindReturnPipe(return_pipe);
      num_commands = 0;
    }
  }

  LogCvmfs(kLogQuota, kLogDebug, "stopping cache manager (%d)", errno);
  close(quota_mgr->pipe_lru_[0]);
  quota_mgr->ProcessCommandBunch(num_commands, command_buffer,
                                 description_buffer);

  // Unpin
  command_buffer[0].command_type = kTouch;
  for (map<shash::Any, uint64_t>::const_iterator i =
       quota_mgr->pinned_chunks_.begin(),
       iEnd = quota_mgr->pinned_chunks_.end(); i != iEnd; ++i)
  {
    command_buffer[0].StoreHash(i->first);
    quota_mgr->ProcessCommandBunch(1, command_buffer, description_buffer);
  }

  return NULL;
}


void PosixQuotaManager::MakeReturnPipe(int pipe[2]) {
  if (!shared_) {
    MakePipe(pipe);
    return;
  }

  // Create FIFO in cache directory, store path name (number) in pipe write end
  int i = 0;
  int retval;
  do {
    retval = mkfifo((workspace_dir_ + "/pipe" + StringifyInt(i)).c_str(), 0600);
    pipe[1] = i;
    i++;
  } while ((retval == -1) && (errno == EEXIST));
  assert(retval == 0);

  // Connect reader's end
  pipe[0] = open((workspace_dir_ + "/pipe" + StringifyInt(pipe[1])).c_str(),
                 O_RDONLY | O_NONBLOCK);
  assert(pipe[0] >= 0);
  Nonblock2Block(pipe[0]);
}


void PosixQuotaManager::ParseDirectories(
  const std::string cache_workspace,
  std::string *cache_dir,
  std::string *workspace_dir)
{
  vector<string> dir_tokens(SplitString(cache_workspace, ':'));
  switch (dir_tokens.size()) {
    case 1:
      *cache_dir = *workspace_dir = dir_tokens[0];
      break;
    case 2:
      *cache_dir = dir_tokens[0];
      *workspace_dir = dir_tokens[1];
      break;
    default:
      PANIC(NULL);
  }
}


/**
 * Immediately inserts a new pinned catalog. Does cache cleanup if necessary.
 *
 * \return True on success, false otherwise
 */
bool PosixQuotaManager::Pin(
  const shash::Any &hash,
  const uint64_t size,
  const string &description,
  const bool is_catalog)
{
  assert((size > 0) || !is_catalog);

  const string hash_str = hash.ToString();
  LogCvmfs(kLogQuota, kLogDebug, "pin into lru %s, path %s",
           hash_str.c_str(), description.c_str());

  // Has to run when not yet spawned (cvmfs initialization)
  if (!spawned_) {
    // Code duplication here
    if (pinned_chunks_.find(hash) == pinned_chunks_.end()) {
      if (pinned_ + size > cleanup_threshold_) {
        LogCvmfs(kLogQuota, kLogDebug, "failed to insert %s (pinned), no space",
                 hash_str.c_str());
        return false;
      } else {
        pinned_chunks_[hash] = size;
        pinned_ += size;
        CheckHighPinWatermark();
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
    sqlite3_bind_text(stmt_new_, 4, &description[0], description.length(),
                      SQLITE_STATIC);
    sqlite3_bind_int64(stmt_new_, 5, is_catalog ? kFileCatalog : kFileRegular);
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
  cmd.SetSize(size);
  cmd.StoreHash(hash);
  cmd.return_pipe = pipe_reserve[1];
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
  bool result;
  ManagedReadHalfPipe(pipe_reserve[0], &result, sizeof(result));
  CloseReturnPipe(pipe_reserve);

  if (!result) return false;
  DoInsert(hash, size, description, is_catalog ? kPin : kPinRegular);

  return true;
}


PosixQuotaManager::PosixQuotaManager(
  const uint64_t limit,
  const uint64_t cleanup_threshold,
  const string &cache_workspace)
  : shared_(false)
  , spawned_(false)
  , limit_(limit)
  , cleanup_threshold_(cleanup_threshold)
  , gauge_(0)
  , pinned_(0)
  , seq_(0)
  , cache_dir_()  // initialized in body
  , workspace_dir_()  // initialized in body
  , fd_lock_cachedb_(-1)
  , async_delete_(true)
  , cachemgr_pid_(0)
  , database_(NULL)
  , stmt_touch_(NULL)
  , stmt_unpin_(NULL)
  , stmt_block_(NULL)
  , stmt_unblock_(NULL)
  , stmt_new_(NULL)
  , stmt_lru_(NULL)
  , stmt_size_(NULL)
  , stmt_rm_(NULL)
  , stmt_rm_batch_(NULL)
  , stmt_list_(NULL)
  , stmt_list_pinned_(NULL)
  , stmt_list_catalogs_(NULL)
  , stmt_list_volatile_(NULL)
  , initialized_(false)
{
  ParseDirectories(cache_workspace, &cache_dir_, &workspace_dir_);
  pipe_lru_[0] = pipe_lru_[1] = -1;
  cleanup_recorder_.AddRecorder(1, 90);  // last 1.5 min with second resolution
  // last 1.5 h with minute resolution
  cleanup_recorder_.AddRecorder(60, 90*60);
  // last 18 hours with 20 min resolution
  cleanup_recorder_.AddRecorder(20*60, 60*60*18);
  // last 4 days with hour resolution
  cleanup_recorder_.AddRecorder(60*60, 60*60*24*4);
}


PosixQuotaManager::~PosixQuotaManager() {
  if (!initialized_) return;

  if (shared_) {
    // Most of cleanup is done elsewhen by shared cache manager
    close(pipe_lru_[1]);
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
}


void PosixQuotaManager::ProcessCommandBunch(
  const unsigned num,
  const LruCommand *commands,
  const char *descriptions)
{
  int retval = sqlite3_exec(database_, "BEGIN", NULL, NULL, NULL);
  assert(retval == SQLITE_OK);

  for (unsigned i = 0; i < num; ++i) {
    const shash::Any hash = commands[i].RetrieveHash();
    const string hash_str = hash.ToString();
    const unsigned size = commands[i].GetSize();
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
        if ((retval != SQLITE_DONE) && (retval != SQLITE_OK)) {
          PANIC(kLogSyslogErr, "failed to update %s in cachedb, error %d",
                hash_str.c_str(), retval);
        }
        sqlite3_reset(stmt_touch_);
        break;
      case kUnpin:
        sqlite3_bind_text(stmt_unpin_, 1, &hash_str[0], hash_str.length(),
                          SQLITE_STATIC);
        retval = sqlite3_step(stmt_unpin_);
        LogCvmfs(kLogQuota, kLogDebug, "unpinning %s: %d",
                 hash_str.c_str(), retval);
        if ((retval != SQLITE_DONE) && (retval != SQLITE_OK)) {
          PANIC(kLogSyslogErr, "failed to unpin %s in cachedb, error %d",
                hash_str.c_str(), retval);
        }
        sqlite3_reset(stmt_unpin_);
        break;
      case kPin:
      case kPinRegular:
      case kInsert:
      case kInsertVolatile:
        // It could already be in, check
        exists = Contains(hash_str);

        // Cleanup, move to trash and unlink
        if (!exists && (gauge_ + size > limit_)) {
          LogCvmfs(kLogQuota, kLogDebug, "over limit, gauge %lu, file size %u",
                   gauge_, size);
          retval = DoCleanup(cleanup_threshold_);
          assert(retval != 0);
        }

        // Insert or replace
        sqlite3_bind_text(stmt_new_, 1, &hash_str[0], hash_str.length(),
                          SQLITE_STATIC);
        sqlite3_bind_int64(stmt_new_, 2, size);
        if (commands[i].command_type == kInsertVolatile) {
          sqlite3_bind_int64(stmt_new_, 3, (seq_++) | kVolatileFlag);
        } else {
          sqlite3_bind_int64(stmt_new_, 3, seq_++);
        }
        sqlite3_bind_text(stmt_new_, 4, &descriptions[i*kMaxDescription],
                          commands[i].desc_length, SQLITE_STATIC);
        sqlite3_bind_int64(stmt_new_, 5, (commands[i].command_type == kPin) ?
                           kFileCatalog : kFileRegular);
        sqlite3_bind_int64(stmt_new_, 6,
          ((commands[i].command_type == kPin) ||
           (commands[i].command_type == kPinRegular)) ? 1 : 0);
        retval = sqlite3_step(stmt_new_);
        LogCvmfs(kLogQuota, kLogDebug, "insert or replace %s, method %d: %d",
                 hash_str.c_str(), commands[i].command_type, retval);
        if ((retval != SQLITE_DONE) && (retval != SQLITE_OK)) {
          PANIC(kLogSyslogErr, "failed to insert %s in cachedb, error %d",
                hash_str.c_str(), retval);
        }
        sqlite3_reset(stmt_new_);

        if (!exists) gauge_ += size;
        break;
      default:
        // other types should have been taken care of by event loop
        PANIC(NULL);
    }
  }

  retval = sqlite3_exec(database_, "COMMIT", NULL, NULL, NULL);
  if (retval != SQLITE_OK) {
    PANIC(kLogSyslogErr, "failed to commit to cachedb, error %d", retval);
  }
}


bool PosixQuotaManager::RebuildDatabase() {
  bool result = false;
  string sql;
  sqlite3_stmt *stmt_select = NULL;
  sqlite3_stmt *stmt_insert = NULL;
  int sqlerr;
  int seq = 0;
  char hex[4];
  struct stat info;
  platform_dirent64 *d;
  DIR *dirp = NULL;
  string path;

  LogCvmfs(kLogQuota, kLogSyslog | kLogDebug, "re-building cache database");

  // Empty cache catalog and fscache
  sql = "DELETE FROM cache_catalog; DELETE FROM fscache;";
  sqlerr = sqlite3_exec(database_, sql.c_str(), NULL, NULL, NULL);
  if (sqlerr != SQLITE_OK) {
    LogCvmfs(kLogQuota, kLogDebug, "could not clear cache database");
    goto build_return;
  }

  gauge_ = 0;

  // Insert files from cache sub-directories 00 - ff
  // TODO(jblomer): fs_traversal
  sqlite3_prepare_v2(database_, "INSERT INTO fscache (sha1, size, actime) "
                     "VALUES (:sha1, :s, :t);", -1, &stmt_insert, NULL);

  for (int i = 0; i <= 0xff; i++) {
    snprintf(hex, sizeof(hex), "%02x", i);
    path = cache_dir_ + "/" + string(hex);
    if ((dirp = opendir(path.c_str())) == NULL) {
      LogCvmfs(kLogQuota, kLogDebug | kLogSyslogErr,
               "failed to open directory %s (tmpwatch interfering?)",
               path.c_str());
      goto build_return;
    }
    while ((d = platform_readdir(dirp)) != NULL) {
      string file_path = path + "/" + string(d->d_name);
      if (stat(file_path.c_str(), &info) == 0) {
        if (!S_ISREG(info.st_mode))
          continue;
        if (info.st_size == 0) {
          LogCvmfs(kLogQuota, kLogSyslog | kLogDebug,
                   "removing empty file %s during automatic cache db rebuild",
                   file_path.c_str());
          unlink(file_path.c_str());
          continue;
        }

        string hash = string(hex) + string(d->d_name);
        sqlite3_bind_text(stmt_insert, 1, hash.data(), hash.length(),
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
        LogCvmfs(kLogQuota, kLogDebug, "could not stat %s", file_path.c_str());
      }
    }
    closedir(dirp);
    dirp = NULL;
  }
  sqlite3_finalize(stmt_insert);
  stmt_insert = NULL;

  // Transfer from temp table in cache catalog
  sqlite3_prepare_v2(database_,
                     "SELECT sha1, size FROM fscache ORDER BY actime;",
                     -1, &stmt_select, NULL);
  sqlite3_prepare_v2(database_,
    "INSERT INTO cache_catalog (sha1, size, acseq, path, type, pinned) "
    "VALUES (:sha1, :s, :seq, 'unknown (automatic rebuild)', :t, 0);",
    -1, &stmt_insert, NULL);
  while (sqlite3_step(stmt_select) == SQLITE_ROW) {
    const string hash = string(
      reinterpret_cast<const char *>(sqlite3_column_text(stmt_select, 0)));
    sqlite3_bind_text(stmt_insert, 1, &hash[0], hash.length(), SQLITE_STATIC);
    sqlite3_bind_int64(stmt_insert, 2, sqlite3_column_int64(stmt_select, 1));
    sqlite3_bind_int64(stmt_insert, 3, seq++);
    // Might also be a catalog (information is lost)
    sqlite3_bind_int64(stmt_insert, 4, kFileRegular);

    int retval = sqlite3_step(stmt_insert);
    if (retval != SQLITE_DONE) {
      // If the file system hosting the cache is full, we'll likely notice here
      LogCvmfs(kLogQuota, kLogDebug | kLogSyslogErr,
               "could not insert into cache catalog (%d - %s)",
               retval, sqlite3_errstr(retval));
      goto build_return;
    }
    sqlite3_reset(stmt_insert);
  }

  // Delete temporary table
  sql = "DELETE FROM fscache;";
  sqlerr = sqlite3_exec(database_, sql.c_str(), NULL, NULL, NULL);
  if (sqlerr != SQLITE_OK) {
    LogCvmfs(kLogQuota, kLogDebug, "could not clear temporary table (%d)",
             sqlerr);
    goto build_return;
  }

  seq_ = seq;
  result = true;
  LogCvmfs(kLogQuota, kLogDebug,
           "rebuilding finished, sequence %" PRIu64 ", gauge %" PRIu64,
           seq_, gauge_);

 build_return:
  if (stmt_insert) sqlite3_finalize(stmt_insert);
  if (stmt_select) sqlite3_finalize(stmt_select);
  if (dirp) closedir(dirp);
  return result;
}


/**
 * Register a channel that allows the cache manager to trigger action to its
 * clients.  Currently used for releasing pinned catalogs.
 */
void PosixQuotaManager::RegisterBackChannel(
  int back_channel[2],
  const string &channel_id)
{
  if (protocol_revision_ >= 1) {
    shash::Md5 hash = shash::Md5(shash::AsciiPtr(channel_id));
    MakeReturnPipe(back_channel);

    LruCommand cmd;
    cmd.command_type = kRegisterBackChannel;
    cmd.return_pipe = back_channel[1];
    // Not StoreHash().  This is an MD5 hash.
    memcpy(cmd.digest, hash.digest, hash.GetDigestSize());
    WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));

    char success;
    ManagedReadHalfPipe(back_channel[0], &success, sizeof(success));
    // At this point, the named FIFO is unlinked, so don't use CloseReturnPipe
    if (success != 'S') {
      PANIC(kLogDebug | kLogSyslogErr,
            "failed to register quota back channel (%c)", success);
    }
  } else {
    // Dummy pipe to return valid file descriptors
    MakePipe(back_channel);
  }
}


/**
 * Removes a chunk from cache, if it exists.
 */
void PosixQuotaManager::Remove(const shash::Any &hash) {
  string hash_str = hash.ToString();

  int pipe_remove[2];
  MakeReturnPipe(pipe_remove);

  LruCommand cmd;
  cmd.command_type = kRemove;
  cmd.return_pipe = pipe_remove[1];
  cmd.StoreHash(hash);
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));

  bool success;
  ManagedReadHalfPipe(pipe_remove[0], &success, sizeof(success));
  CloseReturnPipe(pipe_remove);

  unlink((cache_dir_ + "/" + hash.MakePathWithoutSuffix()).c_str());
}


void PosixQuotaManager::Spawn() {
  if (spawned_)
    return;

  if (pthread_create(&thread_lru_, NULL, MainCommandServer,
      static_cast<void *>(this)) != 0)
  {
    PANIC(kLogDebug, "could not create lru thread");
  }

  spawned_ = true;
}


/**
 * Updates the sequence number of the file specified by the hash.
 */
void PosixQuotaManager::Touch(const shash::Any &hash) {
  LruCommand cmd;
  cmd.command_type = kTouch;
  cmd.StoreHash(hash);
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
}


void PosixQuotaManager::UnbindReturnPipe(int pipe_wronly) {
  if (shared_)
    close(pipe_wronly);
}


void PosixQuotaManager::UnlinkReturnPipe(int pipe_wronly) {
  if (shared_)
    unlink((workspace_dir_ + "/pipe" + StringifyInt(pipe_wronly)).c_str());
}


void PosixQuotaManager::Unpin(const shash::Any &hash) {
  LogCvmfs(kLogQuota, kLogDebug, "Unpin %s", hash.ToString().c_str());

  LruCommand cmd;
  cmd.command_type = kUnpin;
  cmd.StoreHash(hash);
  WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));
}


void PosixQuotaManager::UnregisterBackChannel(
  int back_channel[2],
  const string &channel_id)
{
  if (protocol_revision_ >= 1) {
    shash::Md5 hash = shash::Md5(shash::AsciiPtr(channel_id));

    LruCommand cmd;
    cmd.command_type = kUnregisterBackChannel;
    // Not StoreHash().  This is an MD5 hash.
    memcpy(cmd.digest, hash.digest, hash.GetDigestSize());
    WritePipe(pipe_lru_[1], &cmd, sizeof(cmd));

    // Writer's end will be closed by cache manager, FIFO is already unlinked
    close(back_channel[0]);
  } else {
    ClosePipe(back_channel);
  }
}

void PosixQuotaManager::ManagedReadHalfPipe(int fd, void *buf, size_t nbyte) {
  const unsigned timeout_ms = cachemgr_pid_ ? 1000 : 0;
  bool result = false;
  do {
    result = ReadHalfPipe(fd, buf, nbyte, timeout_ms);
    // try only as long as the cachemgr is still alive
  } while (!result && getpgid(cachemgr_pid_) >= 0);
  if (!result) {
    PANIC(kLogStderr, "Error: quota manager could not read from cachemanager pipe");
  }

}
