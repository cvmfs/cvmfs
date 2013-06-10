/**
 * This file is part of the CernVM File System.
 *
 * A mediator to transform old data structures into new one on reload
 */

#ifndef CVMFS_COMPAT_H_
#define CVMFS_COMPAT_H_

#include <stdint.h>
#include <pthread.h>
#include <sched.h>

#include <cassert>

#include <google/sparse_hash_map>

#include "shortstring.h"
#include "atomic.h"
#include "directory_entry.h"
#include "catalog_mgr.h"
#include "util.h"
#include "glue_buffer.h"

namespace compat {
namespace inode_tracker{

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
  typedef google::sparse_hash_map<uint64_t, Dirent,
          hash_murmur<uint64_t> >
          InodeMap;

  InodeContainer() {
    assert(false);
  }
  bool Add(const uint64_t inode, const uint64_t parent_inode,
           const NameString &name)
  {
    assert(false); return false;
  }
  bool Get(const uint64_t inode, const uint64_t parent_inode,
           const NameString &name)
  {
    assert(false); return false;
  }
  uint32_t Put(const uint64_t inode, const uint32_t by) {
    assert(false); return false;
  }
  bool ConstructPath(const uint64_t inode, PathString *path);
  bool Contains(const uint64_t inode) {
    return map_.find(inode) != map_.end();
  }
  inline size_t Size() { return map_.size(); }
//private:
  std::string DebugPrint() { assert(false); return ""; };
  InodeMap map_;
};


/**
 * Tracks inode reference counters as given by Fuse.
 */
class InodeTracker {
public:
  struct Statistics {
    Statistics() {
      assert(false);
    }
    std::string Print() { assert(false); return ""; }
    atomic_int64 num_inserts;
    atomic_int64 num_dangling_try;
    atomic_int64 num_double_add;
    atomic_int64 num_removes;
    atomic_int64 num_references;
    atomic_int64 num_ancient_hits;
    atomic_int64 num_ancient_misses;
  };
  Statistics GetStatistics() { return statistics_; }

  InodeTracker() { assert(false); }
  explicit InodeTracker(const InodeTracker &other) { assert(false); }
  InodeTracker &operator= (const InodeTracker &other) { assert(false); }
  ~InodeTracker();

  bool VfsGet(const uint64_t inode, const uint64_t parent_inode,
              const NameString &name)
  {
    assert(false); return false;
  }
  bool VfsAdd(const uint64_t inode, const uint64_t parent_inode,
              const NameString &name)
  {
    assert(false); return false;
  }
  void VfsPut(const uint64_t inode, const uint32_t by) { assert(false); }
  bool Find(const uint64_t inode, PathString *path) { assert(false); }

//private:
  static const unsigned kVersion = 1;

  void InitLock() { assert(false); }
  void CopyFrom(const InodeTracker &other) { assert(false); }
  inline void Lock() const {
    // NOT NEEDED
  }
  inline void Unlock() const {
    // NOT NEEDED
  }

  unsigned version_;
  pthread_mutex_t *lock_;
  InodeContainer inode2path_;
  Statistics statistics_;
};

void Migrate(InodeTracker *old_tracker, glue::InodeTracker *new_tracker);

}  // namespace inode_tracker

}  // namespace compat

#endif  // CVMFS_COMPAT_H_
