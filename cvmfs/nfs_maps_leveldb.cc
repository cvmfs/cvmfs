/**
 * This file is part of the CernVM File System.
 *
 * The NFS maps module maintains inode -- path relations.  An inode that is
 * issued once by an NFS exported file system might be asked for
 * any time later by clients.
 *
 * In "NFS mode", cvmfs will issue inodes consecutively and reuse inodes
 * based on path name.  The inode --> path and path --> inode maps are
 * handled by leveldb.  This workaround is comparable to the Fuse "noforget"
 * option, except that the mappings are persistent and thus consistent during
 * cvmfs restarts.  Also, leveldb allows for restricting the memory consumption.
 *
 * The maps are not accounted for by the cache quota.
 */

#include "nfs_maps_leveldb.h"

#include <unistd.h>

#include <cassert>
#include <cstddef>

#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "statistics.h"
#include "util/concurrency.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/pointer.h"
#include "util/posix.h"
#include "util/smalloc.h"

using namespace std;  // NOLINT


NfsMapsLeveldb::ForkAwareEnv::ForkAwareEnv(NfsMapsLeveldb *maps)
    : leveldb::EnvWrapper(leveldb::Env::Default()), maps_(maps) {
  atomic_init32(&num_bg_threads_);
}


void NfsMapsLeveldb::ForkAwareEnv::StartThread(void (*f)(void *), void *a) {
  if (maps_->spawned_) {
    leveldb::Env::Default()->StartThread(f, a);
    return;
  }
  PANIC(kLogDebug | kLogSyslogErr,
        "single threaded leveldb::StartThread called");
  // Unclear how to handle this because caller assumes that thread is started
}


void NfsMapsLeveldb::ForkAwareEnv::Schedule(void (*function)(void *),
                                            void *arg) {
  if (maps_->spawned_) {
    leveldb::Env::Default()->Schedule(function, arg);
    return;
  }
  LogCvmfs(kLogNfsMaps, kLogDebug, "single threaded leveldb::Schedule called");
  FuncArg *funcarg = new FuncArg();
  funcarg->function = function;
  funcarg->arg = arg;
  funcarg->env = this;
  atomic_inc32(&num_bg_threads_);
  pthread_t bg_thread;
  int retval = pthread_create(&bg_thread, NULL, MainFakeThread, funcarg);
  assert(retval == 0);
  retval = pthread_detach(bg_thread);
  assert(retval == 0);
}


void NfsMapsLeveldb::ForkAwareEnv::WaitForBGThreads() {
  while (atomic_read32(&num_bg_threads_) > 0)
    SafeSleepMs(100);
}


/**
 * Leveldb's usleep might collide with the ALARM timer
 */
void NfsMapsLeveldb::ForkAwareEnv::SleepForMicroseconds(int micros) {
  SafeSleepMs(micros / 1000);
}


void *NfsMapsLeveldb::ForkAwareEnv::MainFakeThread(void *data) {
  FuncArg *funcarg = reinterpret_cast<FuncArg *>(data);
  funcarg->function(funcarg->arg);
  atomic_dec32(&(funcarg->env->num_bg_threads_));
  delete funcarg;
  return NULL;
}


//------------------------------------------------------------------------------


NfsMapsLeveldb *NfsMapsLeveldb::Create(const string &leveldb_dir,
                                       const uint64_t root_inode,
                                       const bool rebuild,
                                       perf::Statistics *statistics) {
  assert(root_inode > 0);
  UniquePtr<NfsMapsLeveldb> maps(new NfsMapsLeveldb());
  maps->n_db_added_ = statistics->Register("nfs.leveldb.n_added",
                                           "total number of issued inode");

  maps->root_inode_ = root_inode;
  maps->fork_aware_env_ = new ForkAwareEnv(maps.weak_ref());
  leveldb::Status status;
  leveldb::Options leveldb_options;
  leveldb_options.create_if_missing = true;
  leveldb_options.env = maps->fork_aware_env_;

  // Remove previous database traces
  if (rebuild) {
    LogCvmfs(kLogNfsMaps, kLogSyslogWarn,
             "rebuilding NFS maps, might result in stale entries");
    const bool retval = RemoveTree(leveldb_dir + "/inode2path") &&
                        RemoveTree(leveldb_dir + "/path2inode");
    if (!retval) {
      LogCvmfs(kLogNfsMaps, kLogDebug, "failed to remove previous databases");
      return NULL;
    }
  }

  // Open databases
  maps->cache_inode2path_ = leveldb::NewLRUCache(32 * 1024 * 1024);
  leveldb_options.block_cache = maps->cache_inode2path_;
  maps->filter_inode2path_ = leveldb::NewBloomFilterPolicy(10);
  leveldb_options.filter_policy = maps->filter_inode2path_;
  status = leveldb::DB::Open(leveldb_options, leveldb_dir + "/inode2path",
                             &maps->db_inode2path_);
  if (!status.ok()) {
    LogCvmfs(kLogNfsMaps, kLogDebug, "failed to create inode2path db: %s",
             status.ToString().c_str());
    return NULL;
  }
  LogCvmfs(kLogNfsMaps, kLogDebug, "inode2path opened");

  // Hashes and inodes, no compression here
  leveldb_options.compression = leveldb::kNoCompression;
  // Random order, small block size to not trash caches
  leveldb_options.block_size = 512;
  maps->cache_path2inode_ = leveldb::NewLRUCache(8 * 1024 * 1024);
  leveldb_options.block_cache = maps->cache_path2inode_;
  maps->filter_path2inode_ = leveldb::NewBloomFilterPolicy(10);
  leveldb_options.filter_policy = maps->filter_path2inode_;
  status = leveldb::DB::Open(leveldb_options, leveldb_dir + "/path2inode",
                             &maps->db_path2inode_);
  if (!status.ok()) {
    LogCvmfs(kLogNfsMaps, kLogDebug, "failed to create path2inode db: %s",
             status.ToString().c_str());
    return NULL;
  }
  LogCvmfs(kLogNfsMaps, kLogDebug, "path2inode opened");

  // Fetch highest issued inode
  maps->seq_ = maps->FindInode(shash::Md5(shash::AsciiPtr("?seq")));
  LogCvmfs(kLogNfsMaps, kLogDebug, "Sequence number is %" PRIu64, maps->seq_);
  if (maps->seq_ == 0) {
    maps->seq_ = maps->root_inode_;
    // Insert root inode
    const PathString root_path;
    maps->GetInode(root_path);
  }

  maps->fork_aware_env_->WaitForBGThreads();

  return maps.Release();
}

void NfsMapsLeveldb::SetInodeResidue(unsigned residue_class,
                                     unsigned remainder) {
  const MutexLockGuard lock_guard(lock_);
  if (residue_class < 2) {
    inode_residue_class_ = 1;
    inode_remainder_ = 0;
  } else {
    inode_residue_class_ = residue_class;
    inode_remainder_ = remainder % residue_class;
    seq_ = ((seq_ / inode_residue_class_) + 1) * inode_residue_class_
           + inode_remainder_;
  }
}


uint64_t NfsMapsLeveldb::FindInode(const shash::Md5 &path) {
  leveldb::Status status;
  const leveldb::Slice key(reinterpret_cast<const char *>(path.digest),
                           path.GetDigestSize());
  string result;

  status = db_path2inode_->Get(leveldb::ReadOptions(), key, &result);
  if (!status.ok() && !status.IsNotFound()) {
    PANIC(kLogSyslogErr, "failed to read from path2inode db (path %s): %s",
          path.ToString().c_str(), status.ToString().c_str());
  }

  if (status.IsNotFound()) {
    LogCvmfs(kLogNfsMaps, kLogDebug, "path %s not found",
             path.ToString().c_str());
    return 0;
  } else {
    const uint64_t *inode = reinterpret_cast<const uint64_t *>(result.data());
    LogCvmfs(kLogNfsMaps, kLogDebug, "path %s maps to inode %" PRIu64,
             path.ToString().c_str(), *inode);
    return *inode;
  }
}


uint64_t NfsMapsLeveldb::GetInode(const PathString &path) {
  const shash::Md5 md5_path(path.GetChars(), path.GetLength());
  uint64_t inode = FindInode(md5_path);
  if (inode != 0)
    return inode;

  const MutexLockGuard m(lock_);
  // Search again to avoid race
  inode = FindInode(md5_path);
  if (inode != 0) {
    return inode;
  }

  // Issue new inode
  inode = seq_;
  seq_ += inode_residue_class_;
  PutPath2Inode(md5_path, inode);
  PutInode2Path(inode, path);
  perf::Inc(n_db_added_);
  return inode;
}


/**
 * Finds the path that belongs to an inode.  This must be successful.  The
 * inode input comes from the file system, i.e. it must have been issued
 * before.
 * \return false if not found
 */
bool NfsMapsLeveldb::GetPath(const uint64_t inode, PathString *path) {
  leveldb::Status status;
  const leveldb::Slice key(reinterpret_cast<const char *>(&inode),
                           sizeof(inode));
  string result;

  status = db_inode2path_->Get(leveldb::ReadOptions(), key, &result);
  if (status.IsNotFound()) {
    LogCvmfs(kLogNfsMaps, kLogDebug,
             "failed to find inode %" PRIu64 " in NFS maps, returning ESTALE",
             inode);
    return false;
  }
  if (!status.ok()) {
    PANIC(kLogSyslogErr,
          "failed to read from inode2path db inode %" PRIu64 ": %s", inode,
          status.ToString().c_str());
  }

  path->Assign(result.data(), result.length());
  LogCvmfs(kLogNfsMaps, kLogDebug, "inode %" PRIu64 " maps to path %s", inode,
           path->c_str());
  return true;
}


string NfsMapsLeveldb::GetStatistics() {
  string stats;

  db_inode2path_->GetProperty(leveldb::Slice("leveldb.stats"), &stats);
  stats += "inode --> path database:\n" + stats + "\n";

  db_path2inode_->GetProperty(leveldb::Slice("leveldb.stats"), &stats);
  stats += "path --> inode database:\n" + stats + "\n";

  return stats;
}


NfsMapsLeveldb::NfsMapsLeveldb()
    : db_inode2path_(NULL)
    , db_path2inode_(NULL)
    , cache_inode2path_(NULL)
    , cache_path2inode_(NULL)
    , filter_inode2path_(NULL)
    , filter_path2inode_(NULL)
    , fork_aware_env_(NULL)
    , root_inode_(0)
    , seq_(0)
    , lock_(NULL)
    , spawned_(false)
    , inode_residue_class_(1)
    , inode_remainder_(0)
    , n_db_added_(NULL) {
  lock_ = reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  const int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


NfsMapsLeveldb::~NfsMapsLeveldb() {
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
  pthread_mutex_destroy(lock_);
  free(lock_);
}


void NfsMapsLeveldb::PutInode2Path(const uint64_t inode,
                                   const PathString &path) {
  leveldb::Status status;
  const leveldb::Slice key(reinterpret_cast<const char *>(&inode),
                           sizeof(inode));
  const leveldb::Slice value(path.GetChars(), path.GetLength());

  status = db_inode2path_->Put(leveldb::WriteOptions(), key, value);
  if (!status.ok()) {
    PANIC(kLogSyslogErr,
          "failed to write inode2path entry (%" PRIu64 " --> %s): %s", inode,
          path.c_str(), status.ToString().c_str());
  }
  LogCvmfs(kLogNfsMaps, kLogDebug, "stored inode %" PRIu64 " --> path %s",
           inode, path.c_str());
}


void NfsMapsLeveldb::PutPath2Inode(const shash::Md5 &path,
                                   const uint64_t inode) {
  leveldb::Status status;
  const leveldb::Slice key(reinterpret_cast<const char *>(path.digest),
                           path.GetDigestSize());
  const leveldb::Slice value(reinterpret_cast<const char *>(&inode),
                             sizeof(inode));

  status = db_path2inode_->Put(leveldb::WriteOptions(), key, value);
  if (!status.ok()) {
    PANIC(kLogSyslogErr,
          "failed to write path2inode entry (%s --> %" PRIu64 "): %s",
          path.ToString().c_str(), inode, status.ToString().c_str());
  }
  LogCvmfs(kLogNfsMaps, kLogDebug, "stored path %s --> inode %" PRIu64,
           path.ToString().c_str(), inode);
}
