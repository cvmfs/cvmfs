/**
 * This file is part of the CernVM File System.
 *
 * NFS maps module maintains inode -- path relations.  An inode that is
 * issued once by an NFS exported file system might be asked for
 * any time later by clients.
 *
 * In "NFS mode", cvmfs will issue inodes consequtively and reuse inodes
 * based on path name.  The inode --> path and path --> inode maps are
 * handled by leveldb.  This workaround is comparable to the Fuse "noforget"
 * option, except that the mappings are persistent and consistent during
 * cvmfs restarts.
 *
 * The maps are not accounted for by the cache quota.
 */

#include "nfs_maps.h"

#include <cassert>
#include "leveldb/db.h"
#include "logging.h"

using namespace std;  // NOLINT

namespace nfs_maps {

leveldb::DB *db_inode2path_;
leveldb::DB *db_path2inode_;
leveldb::Options leveldb_options_;

bool Init(const string &leveldb_dir) {
  leveldb::Status status;
  leveldb_options_.create_if_missing = true;

  status = leveldb::DB::Open(leveldb_options_, leveldb_dir + "/inode2path",
                             &db_inode2path_);
  if (!status.ok()) {
    LogCvmfs(kLogNfsMaps, kLogDebug, "failed to create inode2path db: %s",
             status.ToString().c_str());
    return false;
  }
  LogCvmfs(kLogNfsMaps, kLogDebug, "inode2path opened");

  status = leveldb::DB::Open(leveldb_options_, leveldb_dir + "/path2inode",
                             &db_path2inode_);
  if (!status.ok()) {
    LogCvmfs(kLogNfsMaps, kLogDebug, "failed to create path2inode db: %s",
             status.ToString().c_str());
    return false;
  }
  LogCvmfs(kLogNfsMaps, kLogDebug, "path2inode opened");

  return true;
}


void Fini() {
  delete db_inode2path_;
  LogCvmfs(kLogNfsMaps, kLogDebug, "inode2path closed");
  delete db_path2inode_;
  LogCvmfs(kLogNfsMaps, kLogDebug, "path2inode closed");
  db_inode2path_ = NULL;
  db_path2inode_ = NULL;
}


}  // namespace nfs_maps
