/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NFS_MAPS_LEVELDB_H_
#define CVMFS_NFS_MAPS_LEVELDB_H_

#include <pthread.h>

#include <string>

#include "crypto/hash.h"
#include "leveldb/env.h"
#include "nfs_maps.h"
#include "util/atomic.h"


namespace leveldb {
class DB;
class Cache;
class FilterPolicy;
}  // namespace leveldb
namespace perf {
class Counter;
class Statistics;
}  // namespace perf


class NfsMapsLeveldb : public NfsMaps {
 public:
  virtual ~NfsMapsLeveldb();
  virtual uint64_t GetInode(const PathString &path);
  virtual bool GetPath(const uint64_t inode, PathString *path);
  virtual void SetInodeResidue(unsigned residue_class, unsigned remainder);
  virtual void Spawn() { spawned_ = true; }
  virtual std::string GetStatistics();

  static NfsMapsLeveldb *Create(const std::string &leveldb_dir,
                                const uint64_t root_inode,
                                const bool rebuild,
                                perf::Statistics *statistics);

 private:
  class ForkAwareEnv;
  struct FuncArg {
    void (*function)(void *);
    void *arg;
    class ForkAwareEnv *env;
  };

  /**
   * Leveldb's background threads must not be started before cvmfs has forked.
   * Before forking, we run the processes in specially created threads.
   * We make sure, these threads are terminated before forking.
   */
  class ForkAwareEnv : public leveldb::EnvWrapper {
   public:
    explicit ForkAwareEnv(NfsMapsLeveldb *maps);
    void StartThread(void (*f)(void *), void *a);
    void Schedule(void (*function)(void *), void *arg);
    void WaitForBGThreads();
    void SleepForMicroseconds(int micros);

   private:
    static void *MainFakeThread(void *data);

    NfsMapsLeveldb *maps_;
    atomic_int32 num_bg_threads_;
  };

  NfsMapsLeveldb();
  void PutInode2Path(const uint64_t inode, const PathString &path);
  void PutPath2Inode(const shash::Md5 &path, const uint64_t inode);
  uint64_t FindInode(const shash::Md5 &path);

  leveldb::DB *db_inode2path_;
  leveldb::DB *db_path2inode_;
  leveldb::Cache *cache_inode2path_;
  leveldb::Cache *cache_path2inode_;
  const leveldb::FilterPolicy *filter_inode2path_;
  const leveldb::FilterPolicy *filter_path2inode_;
  ForkAwareEnv *fork_aware_env_;
  uint64_t root_inode_;
  uint64_t seq_;
  pthread_mutex_t *lock_;
  bool spawned_;

  unsigned inode_residue_class_;
  unsigned inode_remainder_;

  perf::Counter *n_db_added_;
};

#endif  // CVMFS_NFS_MAPS_LEVELDB_H_
