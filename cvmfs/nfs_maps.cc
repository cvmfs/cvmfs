/**
 * This file is part of the CernVM File System.
 *
 * The NFS maps module maintains inode -- path relations.  An inode that is
 * issued once by an NFS exported file system might be asked for
 * any time later by clients.
 *
 * In "NFS mode", cvmfs will issue inodes consequtively and reuse inodes
 * based on path name.  The inode --> path and path --> inode maps are
 * handled by leveldb.  This workaround is comparable to the Fuse "noforget"
 * option, except that the mappings are persistent and thus consistent during
 * cvmfs restarts.  Also, leveldb allows for restricting the memory consumption.
 *
 * The maps are not accounted for by the cache quota.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "nfs_maps.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>

#include <cassert>
#include <cstdlib>

#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "logging.h"
#include "nfs_shared_maps.h"
#include "util.h"

using namespace std;  // NOLINT

namespace nfs_maps {

leveldb::DB *db_inode2path_ = NULL;
leveldb::DB *db_path2inode_ = NULL;
leveldb::Cache *cache_inode2path_ = NULL;
leveldb::Cache *cache_path2inode_ = NULL;
const leveldb::FilterPolicy *filter_inode2path_ = NULL;
const leveldb::FilterPolicy *filter_path2inode_ = NULL;
leveldb::ReadOptions leveldb_read_options_;
leveldb::WriteOptions leveldb_write_options_;
uint64_t root_inode_;
uint64_t seq_;
pthread_mutex_t lock_ = PTHREAD_MUTEX_INITIALIZER;
bool spawned_ = false;  // Set to true after fork()
// If true, use sqlite db that can be put on a shared NFS volume.
// See nfs_shared_maps
bool use_shared_db_ = false;


// Leveldb's background threads must not be started before cvmfs has forked.
// Before forking, we run the processes in specially created threads.
// We make sure, these threads are terminated before forking.

struct FuncArg {
  void (*function)(void*);
  void *arg;
};

static void *MainFakeThread(void *data) {
  FuncArg *funcarg = reinterpret_cast<FuncArg *>(data);
  funcarg->function(funcarg->arg);
  delete funcarg;
  return NULL;
}

class ForkAwareEnv : public leveldb::EnvWrapper {
 public:
  ForkAwareEnv() : leveldb::EnvWrapper(leveldb::Env::Default()) {
    fake_thread_running_ = false;
  }

  void StartThread(void (*f)(void*), void* a) {
    if (spawned_) {
      leveldb::Env::Default()->StartThread(f, a);
      return;
    }
    LogCvmfs(kLogNfsMaps, kLogDebug,
             "single threaded leveldb::StartThread called");
    // Unclear how to handle this because caller assumes that thread is started
    abort();
  }

  void Schedule(void (*function)(void*), void* arg) {
    if (spawned_) {
      leveldb::Env::Default()->Schedule(function, arg);
      return;
    }

    LogCvmfs(kLogNfsMaps, kLogDebug,
             "single threaded leveldb::Schedule called");
    WaitForBGThreads();
    FuncArg *funcarg = new FuncArg();
    funcarg->function = function;
    funcarg->arg = arg;
    int retval = pthread_create(&fake_thread_, NULL, MainFakeThread, funcarg);
    assert(retval == 0);
    fake_thread_running_ = true;
  }

  void WaitForBGThreads() {
    if (fake_thread_running_)
      pthread_join(fake_thread_, NULL);
    fake_thread_running_ = false;
  }

  // Leveldb's usleep might collide with the ALARM timer
  void SleepForMicroseconds(int micros) {
    SafeSleepMs(micros/1000);
  }

 private:
  pthread_t fake_thread_;  // A real thread is required to prevent deadlocks.
  bool fake_thread_running_;
};

ForkAwareEnv *fork_aware_env_ = NULL;


static void PutPath2Inode(const shash::Md5 &path, const uint64_t inode) {
  leveldb::Status status;
  leveldb::Slice key(reinterpret_cast<const char *>(path.digest),
                     path.GetDigestSize());
  leveldb::Slice value(reinterpret_cast<const char *>(&inode), sizeof(inode));

  status = db_path2inode_->Put(leveldb_write_options_, key, value);
  if (!status.ok()) {
    LogCvmfs(kLogNfsMaps, kLogSyslogErr,
             "failed to write path2inode entry (%s --> %"PRIu64"): %s",
             path.ToString().c_str(), inode, status.ToString().c_str());
    abort();
  }
  LogCvmfs(kLogNfsMaps, kLogDebug, "stored path %s --> inode %"PRIu64,
           path.ToString().c_str(), inode);
}


static void PutInode2Path(const uint64_t inode, const PathString &path) {
  leveldb::Status status;
  leveldb::Slice key(reinterpret_cast<const char *>(&inode), sizeof(inode));
  leveldb::Slice value(path.GetChars(), path.GetLength());

  status = db_inode2path_->Put(leveldb_write_options_, key, value);
  if (!status.ok()) {
    LogCvmfs(kLogNfsMaps, kLogSyslogErr,
             "failed to write inode2path entry (%"PRIu64" --> %s): %s",
             inode, path.c_str(), status.ToString().c_str());
    abort();
  }
  LogCvmfs(kLogNfsMaps, kLogDebug, "stored inode %"PRIu64" --> path %s",
           inode, path.c_str());
}


/**
 * \return 0 if path is not found, the stored inode otherwise
 */
static uint64_t FindInode(const shash::Md5 &path) {
  leveldb::Status status;
  leveldb::Slice key(reinterpret_cast<const char *>(path.digest),
                     path.GetDigestSize());
  string result;

  status = db_path2inode_->Get(leveldb_read_options_, key, &result);
  if (!status.ok() && !status.IsNotFound()) {
    LogCvmfs(kLogNfsMaps, kLogSyslogErr,
             "failed to read from path2inode db (path %s): %s",
             path.ToString().c_str(), status.ToString().c_str());
    abort();
  }

  if (status.IsNotFound()) {
    LogCvmfs(kLogNfsMaps, kLogDebug, "path %s not found",
             path.ToString().c_str());
    return 0;
  } else {
    const uint64_t *inode = reinterpret_cast<const uint64_t *>(result.data());
    LogCvmfs(kLogNfsMaps, kLogDebug, "path %s maps to inode %"PRIu64,
             path.ToString().c_str(), *inode);
    return *inode;
  }
}


/**
 * Finds the inode for path or issues a new inode.
 */
uint64_t GetInode(const PathString &path) {
  if (use_shared_db_)
    return nfs_shared_maps::GetInode(path);

  const shash::Md5 md5_path(path.GetChars(), path.GetLength());
  uint64_t inode = FindInode(md5_path);
  if (inode != 0)
    return inode;

  pthread_mutex_lock(&lock_);
  // Search again to avoid race
  inode = FindInode(md5_path);
  if (inode != 0) {
    pthread_mutex_unlock(&lock_);
    return inode;
  }

  // Issue new inode
  inode = seq_++;
  PutPath2Inode(md5_path, inode);
  PutInode2Path(inode, path);
  pthread_mutex_unlock(&lock_);

  return inode;
}


/**
 * Finds the path that belongs to an inode.  This must be successful.  The
 * inode input comes from the file system, i.e. it must have been issued
 * before.
 * \return false if not found
 */
bool GetPath(const uint64_t inode, PathString *path) {
  if (use_shared_db_)
    return nfs_shared_maps::GetPath(inode, path);

  leveldb::Status status;
  leveldb::Slice key(reinterpret_cast<const char *>(&inode), sizeof(inode));
  string result;

  status = db_inode2path_->Get(leveldb_read_options_, key, &result);
  if (status.IsNotFound()) {
    LogCvmfs(kLogNfsMaps, kLogDebug,
             "failed to find inode %"PRIu64" in NFS maps, returning ESTALE",
             inode);
    return false;
  }
  if (!status.ok()) {
    LogCvmfs(kLogNfsMaps, kLogSyslogErr,
             "failed to read from inode2path db inode %"PRIu64": %s",
             inode, status.ToString().c_str());
    abort();
  }

  path->Assign(result.data(), result.length());
  LogCvmfs(kLogNfsMaps, kLogDebug, "inode %"PRIu64" maps to path %s",
           inode, path->c_str());
  return true;
}


string GetStatistics() {
  if (use_shared_db_)
    return nfs_shared_maps::GetStatistics();

  string result = "Total number of issued inodes: " +
                  StringifyInt(seq_-root_inode_) + "\n";

  string stats;
  db_inode2path_->GetProperty(leveldb::Slice("leveldb.stats"), &stats);
  result += "inode --> path database:\n" + stats + "\n";

  db_path2inode_->GetProperty(leveldb::Slice("leveldb.stats"), &stats);
  result += "path --> inode database:\n" + stats + "\n";

  return result;
}


bool Init(const string &leveldb_dir, const uint64_t root_inode,
          const bool rebuild, const bool shared_db)
{
  use_shared_db_ = shared_db;
  if (shared_db)
    return nfs_shared_maps::Init(leveldb_dir, root_inode, rebuild);

  assert(root_inode > 0);
  root_inode_ = root_inode;
  fork_aware_env_ = new ForkAwareEnv();
  leveldb::Status status;
  leveldb::Options leveldb_options;
  leveldb_options.create_if_missing = true;
  leveldb_options.env = fork_aware_env_;

  // Remove previous database traces
  if (rebuild) {
    LogCvmfs(kLogNfsMaps, kLogSyslogWarn,
             "rebuilding NFS maps, might result in stale entries");
    bool retval = RemoveTree(leveldb_dir + "/inode2path") &&
                  RemoveTree(leveldb_dir + "/path2inode");
    if (!retval) {
      LogCvmfs(kLogNfsMaps, kLogDebug, "failed to remove previous databases");
      return false;
    }
  }

  // Open databases
  cache_inode2path_ = leveldb::NewLRUCache(32 * 1024*1024);
  leveldb_options.block_cache = cache_inode2path_;
  filter_inode2path_ = leveldb::NewBloomFilterPolicy(10);
  leveldb_options.filter_policy = filter_inode2path_;
  status = leveldb::DB::Open(leveldb_options, leveldb_dir + "/inode2path",
                             &db_inode2path_);
  if (!status.ok()) {
    LogCvmfs(kLogNfsMaps, kLogDebug, "failed to create inode2path db: %s",
             status.ToString().c_str());
    return false;
  }
  LogCvmfs(kLogNfsMaps, kLogDebug, "inode2path opened");

  // Hashes and inodes, no compression here
  leveldb_options.compression = leveldb::kNoCompression;
  // Random order, small block size to not trash caches
  leveldb_options.block_size = 512;
  cache_path2inode_ = leveldb::NewLRUCache(8 * 1024*1024);
  leveldb_options.block_cache = cache_path2inode_;
  filter_path2inode_ = leveldb::NewBloomFilterPolicy(10);
  leveldb_options.filter_policy = filter_path2inode_;
  status = leveldb::DB::Open(leveldb_options, leveldb_dir + "/path2inode",
                             &db_path2inode_);
  if (!status.ok()) {
    LogCvmfs(kLogNfsMaps, kLogDebug, "failed to create path2inode db: %s",
             status.ToString().c_str());
    return false;
  }
  LogCvmfs(kLogNfsMaps, kLogDebug, "path2inode opened");

  // Fetch highest issued inode
  seq_ = FindInode(shash::Md5(shash::AsciiPtr("?seq")));
  LogCvmfs(kLogNfsMaps, kLogDebug, "Sequence number is %"PRIu64, seq_);
  if (seq_ == 0) {
    seq_ = root_inode_;
    // Insert root inode
    PathString root_path;
    nfs_maps::GetInode(root_path);
  }

  fork_aware_env_->WaitForBGThreads();

  return true;
}


/**
 * Start real work only after fork() because leveldb has background threads.
 */
void Spawn() {
  if (use_shared_db_) {
    nfs_shared_maps::Spawn();
    return;
  }
  spawned_ = true;
}


void Fini() {
  if (use_shared_db_)
    return nfs_shared_maps::Fini();

  // Write highest issued sequence number
  PutPath2Inode(shash::Md5(shash::AsciiPtr("?seq")), seq_);

  delete db_path2inode_;
  delete cache_path2inode_;
  delete filter_path2inode_;
  LogCvmfs(kLogNfsMaps, kLogDebug, "path2inode closed");
  delete db_inode2path_;
  delete cache_inode2path_;
  delete filter_inode2path_;
  LogCvmfs(kLogNfsMaps, kLogDebug, "inode2path closed");
  delete fork_aware_env_;
  db_inode2path_ = NULL;
  db_path2inode_ = NULL;
  cache_inode2path_ = NULL;
  cache_path2inode_ = NULL;
  filter_inode2path_ = NULL;
  filter_path2inode_ = NULL;
  fork_aware_env_ = NULL;
}

}  // namespace nfs_maps
