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
 * TODO: recreate database if previous crash is detected.
 */

#define __STDC_FORMAT_MACROS

#include "nfs_maps.h"

#include <stdint.h>
#include <inttypes.h>
#include <pthread.h>

#include <cassert>
#include <cstdlib>

#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"

#include "logging.h"
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


static void PutPath2Inode(const hash::Md5 &path, const uint64_t inode) {
  leveldb::Status status;
  leveldb::Slice key(reinterpret_cast<const char *>(path.digest),
                     path.GetDigestSize());
  leveldb::Slice value(reinterpret_cast<const char *>(&inode), sizeof(inode));

  status = db_path2inode_->Put(leveldb_write_options_, key, value);
  if (!status.ok()) {
    LogCvmfs(kLogNfsMaps, kLogSyslog,
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
    LogCvmfs(kLogNfsMaps, kLogSyslog,
             "failed to write inode2path entry (%"PRIu64" --> %s): %s",
             inode, path.c_str(), status.ToString().c_str());
    abort();
  }
  LogCvmfs(kLogNfsMaps, kLogDebug, "stored inode %"PRIu64" --> path  %s",
           inode, path.c_str());
}


/**
 * \return 0 if path is not found, the stored inode otherwise
 */
static uint64_t FindInode(const hash::Md5 &path) {
  leveldb::Status status;
  leveldb::Slice key(reinterpret_cast<const char *>(path.digest),
                     path.GetDigestSize());
  string result;

  status = db_path2inode_->Get(leveldb_read_options_, key, &result);
  if (!status.ok() && !status.IsNotFound()) {
    LogCvmfs(kLogNfsMaps, kLogSyslog,
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
  const hash::Md5 md5_path(path.GetChars(), path.GetLength());
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
 */
void GetPath(const uint64_t inode, PathString *path) {
  leveldb::Status status;
  leveldb::Slice key(reinterpret_cast<const char *>(&inode), sizeof(inode));
  string result;

  status = db_inode2path_->Get(leveldb_read_options_, key, &result);
  if (!status.ok()) {
    LogCvmfs(kLogNfsMaps, kLogSyslog,
             "failed to read from inode2path db inode %"PRIu64": %s",
             inode, status.ToString().c_str());
    abort();
  }

  path->Assign(result.data(), result.length());
  LogCvmfs(kLogNfsMaps, kLogDebug, "inode %"PRIu64" maps to path %s",
           inode, path->c_str());
}


string GetStatistics() {
  string result = "Total number of issued inodes: " +
                  StringifyInt(seq_-root_inode_) + "\n";

  string stats;
  db_inode2path_->GetProperty(leveldb::Slice("leveldb.stats"), &stats);
  result += "inode --> path database:\n" + stats + "\n";

  db_path2inode_->GetProperty(leveldb::Slice("leveldb.stats"), &stats);
  result += "path --> inode database:\n" + stats + "\n";

  return result;
}


bool Init(const string &leveldb_dir, const uint64_t root_inode) {
  assert(root_inode > 0);
  root_inode_ = root_inode;
  leveldb::Status status;
  leveldb::Options leveldb_options;
  leveldb_options.create_if_missing = true;

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
  seq_ = FindInode(hash::Md5(hash::AsciiPtr("?seq")));
  if (seq_ == 0) {
    seq_ = root_inode;
    // Insert root inode
    PathString root_path;
    nfs_maps::GetInode(root_path);
  }

  return true;
}


void Fini() {
  // Write highst issued sequence number
  PutPath2Inode(hash::Md5(hash::AsciiPtr("?seq")), seq_-1);

  delete db_inode2path_;
  delete cache_inode2path_;
  delete filter_inode2path_;
  LogCvmfs(kLogNfsMaps, kLogDebug, "inode2path closed");
  delete db_path2inode_;
  delete cache_path2inode_;
  delete filter_path2inode_;
  LogCvmfs(kLogNfsMaps, kLogDebug, "path2inode closed");
  db_inode2path_ = NULL;
  db_path2inode_ = NULL;
  cache_inode2path_ = NULL;
  cache_path2inode_ = NULL;
  filter_inode2path_ = NULL;
  filter_path2inode_ = NULL;
}


}  // namespace nfs_maps
