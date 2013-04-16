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

#ifndef CVMFS_GLUE_BUFFER_H_
#define CVMFS_GLUE_BUFFER_H_

namespace glue {

struct Dirent {
  Dirent() { parent_inode = 0; }
  Dirent(const uint64_t p, const NameString &n) { 
    parent_inode = p;
    name = n;
    references = 1;
  }
  uint32_t references;
  uint64_t parent_inode;
  NameString name;
};


class InodeContainer {
 public:
  typedef google::sparse_hash_map<uint64_t, glue::Dirent, 
          hash_murmur<uint64_t> >
          InodeMap;
  
  InodeContainer() {
    map_.set_deleted_key(0);
  }
  void Add(const uint64_t inode, const uint64_t parent_inode, 
           const NameString &name);
  bool Get(const uint64_t inode, const uint64_t parent_inode, 
           const NameString &name);
  uint32_t Put(const uint64_t inode, const uint32_t by);  
  bool ConstructPath(const uint64_t inode, PathString *path);
  bool Contains(const uint64_t inode) {
    return map_.find(inode) != map_.end();
  }
  inline size_t Size() { return map_.size(); }
 private:
  std::string DebugPrint();
  InodeMap map_;
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
      atomic_init64(&num_ancient_hits);
      atomic_init64(&num_ancient_misses);
    }
    std::string Print() {
      return 
      "inserts: " + StringifyInt(atomic_read64(&num_inserts)) +
      "  removes: " + StringifyInt(atomic_read64(&num_removes)) +
      "  references: " + StringifyInt(atomic_read64(&num_references)) +
      "  ancient(hits): " + StringifyInt(atomic_read64(&num_ancient_hits)) +
      "  ancient(misses): " + StringifyInt(atomic_read64(&num_ancient_misses));
    }
    atomic_int64 num_inserts;
    atomic_int64 num_removes;
    atomic_int64 num_references;
    atomic_int64 num_ancient_hits;
    atomic_int64 num_ancient_misses;
  };
  Statistics GetStatistics() { return statistics_; }
  
  InodeTracker();
  explicit InodeTracker(const InodeTracker &other);
  InodeTracker &operator= (const InodeTracker &other);
  ~InodeTracker();
  
  bool VfsGet(const uint64_t inode, const uint64_t parent_inode,
              const NameString &name);
  void VfsAdd(const uint64_t inode, const uint64_t parent_inode,
              const NameString &name);
  void VfsPut(const uint64_t inode, const uint32_t by);
  bool Find(const uint64_t inode, PathString *path);
  
private:
  static const unsigned kVersion = 1;

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
  InodeContainer inode2path_;
  Statistics statistics_;
};


}  // namespace glue

#endif  // CVMFS_GLUE_BUFFER_H_
