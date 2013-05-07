/**
 * This file is part of the CernVM File System.
 *
 * This module provides the inode tracker in order to remember inodes
 * and their parents that are in use by the kernel.
 *
 * These objects have to survive reloading of the library, so no virtual
 * functions.
 */

#include <stdint.h>
#include <pthread.h>
#include <sched.h>

#include <cassert>
#include <string>
#include <map>
#include <vector>

#include <google/sparse_hash_map>

#include "shortstring.h"
#include "atomic.h"
#include "dirent.h"
#include "catalog_mgr.h"
#include "util.h"
#include "hash.h"
#include "smallhash.h"

#ifndef CVMFS_GLUE_BUFFER_H_
#define CVMFS_GLUE_BUFFER_H_

namespace glue {

static inline uint32_t hasher_md5(const hash::Md5 &key) {
  // Don't start with the first bytes, because == is using them as well
  return (uint32_t) *((uint32_t *)key.digest + 1);
}


static inline uint32_t hasher_inode(const uint64_t &inode) {
  return MurmurHash2(&inode, sizeof(inode), 0x07387a4f);
}


class PathMap {
 public:
  PathMap() {
    map_.Init(16, hash::Md5(), hasher_md5);
  }

  bool LookupPath(const hash::Md5 &md5path, PathString *path) {
    PathInfo value;
    bool found = map_.Lookup(md5path, &value);
    path->Assign(value.path);
    return found;
  }

  uint64_t LookupInode(const PathString &path) {
    PathInfo value;
    bool found = map_.Lookup(hash::Md5(path.GetChars(), path.GetLength()),
                             &value);
    if (found) return value.inode;
    return 0;
  }

  hash::Md5 Insert(const PathString &path, const uint64_t inode) {
    hash::Md5 md5path(path.GetChars(), path.GetLength());
    map_.Insert(md5path, PathInfo(inode, path));
    return md5path;
  }

  void Erase(const hash::Md5 &md5path) {
    map_.Erase(md5path);
  }

  void Clear() { map_.Clear(); }
 private:
  struct PathInfo {
    PathInfo() { inode = 0; }
    PathInfo(const uint64_t i, const PathString &p) { inode = i; path = p; }
    uint64_t inode;
    PathString path;
  };

  SmallHashDynamic<hash::Md5, PathInfo> map_;
};


class InodeMap {
 public:
  InodeMap() {
    map_.Init(16, 0, hasher_inode);
  }

  bool LookupMd5Path(const uint64_t inode, hash::Md5 *md5path) {
    bool found = map_.Lookup(inode, md5path);
    return found;
  }

  void Insert(const uint64_t inode, const hash::Md5 &md5path) {
    map_.Insert(inode, md5path);
  }

  void Erase(const uint64_t inode) {
    map_.Erase(inode);
  }

  void Clear() { map_.Clear(); }
 private:
  SmallHashDynamic<uint64_t, hash::Md5> map_;
};


class InodeReferences {
 public:
  InodeReferences() {
    map_.Init(16, 0, hasher_inode);
  }

  bool Get(const uint64_t inode) {
    uint32_t refcounter = 0;
    const bool found = map_.Lookup(inode, &refcounter);
    const bool new_inode = !found;
    refcounter++;  // This is 0 if the inode is not found
    map_.Insert(inode, refcounter);
    return new_inode;
  }

  bool Put(const uint64_t inode, const uint32_t by) {
    uint32_t refcounter;
    bool found = map_.Lookup(inode, &refcounter);
    assert(found);
    assert(refcounter >= by);
    if (refcounter == by) {
      map_.Erase(inode);
      return true;
    }
    refcounter -= by;
    map_.Insert(inode, refcounter);
    return false;
  }

  void Clear() {
    map_.Clear();
  }
 private:
  SmallHashDynamic<uint64_t, uint32_t> map_;
};


/**
 * Tracks inode reference counters as given by Fuse.
 */
class InodeTracker {
public:
  struct Statistics {
    Statistics() {
      atomic_init64(&num_inserts);
      atomic_init64(&num_removes);
      atomic_init64(&num_references);
      atomic_init64(&num_hits_inode);
      atomic_init64(&num_hits_path);
      atomic_init64(&num_misses_path);
    }
    std::string Print() {
      return
      "inserts: " + StringifyInt(atomic_read64(&num_inserts)) +
      "  removes: " + StringifyInt(atomic_read64(&num_removes)) +
      "  references: " + StringifyInt(atomic_read64(&num_references)) +
      "  hits(inode): " + StringifyInt(atomic_read64(&num_hits_inode)) +
      "  hits(path): " + StringifyInt(atomic_read64(&num_hits_path)) +
      "  misses(path): " + StringifyInt(atomic_read64(&num_misses_path));
    }
    atomic_int64 num_inserts;
    atomic_int64 num_removes;
    atomic_int64 num_references;
    atomic_int64 num_hits_inode;
    atomic_int64 num_hits_path;
    atomic_int64 num_misses_path;
  };
  Statistics GetStatistics() { return statistics_; }

  InodeTracker();
  explicit InodeTracker(const InodeTracker &other);
  InodeTracker &operator= (const InodeTracker &other);
  ~InodeTracker();

  void VfsGet(const uint64_t inode, const PathString &path) {
    Lock();
    bool new_inode = inode_references_.Get(inode);
    hash::Md5 md5path = path_map_.Insert(path, inode);
    inode_map_.Insert(inode, md5path);
    Unlock();

    atomic_inc64(&statistics_.num_references);
    if (new_inode) atomic_inc64(&statistics_.num_inserts);
  }

  void VfsPut(const uint64_t inode, const uint32_t by) {
    Lock();
    bool removed = inode_references_.Put(inode, by);
    if (removed) {
      // TODO: pop operation (Lookup+Erase)
      hash::Md5 md5path;
      bool found = inode_map_.LookupMd5Path(inode, &md5path);
      assert(found);
      inode_map_.Erase(inode);
      path_map_.Erase(md5path);
      atomic_inc64(&statistics_.num_removes);
    }
    Unlock();
    atomic_xadd64(&statistics_.num_references, -int32_t(by));
  }

  bool FindPath(const uint64_t inode, PathString *path) {
    Lock();
    hash::Md5 md5path;
    bool found = inode_map_.LookupMd5Path(inode, &md5path);
    if (found) {
      found = path_map_.LookupPath(md5path, path);
      assert(found);
    }
    Unlock();

    if (found) atomic_inc64(&statistics_.num_hits_path);
    else atomic_inc64(&statistics_.num_misses_path);
    return found;
  }

  uint64_t FindInode(const PathString &path) {
    Lock();
    uint64_t inode = path_map_.LookupInode(path);
    Unlock();
    atomic_inc64(&statistics_.num_hits_inode);
    return inode;
  }


private:
  static const unsigned kVersion = 2;

  void InitLock();
  void CopyFrom(const InodeTracker &other);
  inline void Lock() const {
    int retval = pthread_mutex_lock(lock_);
    assert(retval == 0);
  }
  inline void Unlock() const {
    int retval = pthread_mutex_unlock(lock_);
    assert(retval == 0);
  }

  unsigned version_;
  pthread_mutex_t *lock_;
  PathMap path_map_;
  InodeMap inode_map_;
  InodeReferences inode_references_;
  Statistics statistics_;
};


}  // namespace glue

#endif  // CVMFS_GLUE_BUFFER_H_
