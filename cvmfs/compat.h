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


namespace inode_tracker_v2{

template<class Key, class Value, class Derived>
class SmallHashBase {
public:
  static const double kLoadFactor;  // mainly useless for the dynamic version
  static const double kThresholdGrow;  // only used for resizable version
  static const double kThresholdShrink;  // only used for resizable version

  SmallHashBase() { assert(false); }
  ~SmallHashBase() {
    delete[] keys_;
    delete[] values_;
  }
  void Init(uint32_t expected_size, Key empty,
            uint32_t (*hasher)(const Key &key))
  {
    assert(false);
  }
  bool Lookup(const Key &key, Value *value) const {
    uint32_t bucket;
    uint32_t collisions;
    const bool found = DoLookup(key, &bucket, &collisions);
    if (found)
      *value = values_[bucket];
    return found;
  }
  bool Contains(const Key &key) const {
    uint32_t bucket;
    uint32_t collisions;
    const bool found = DoLookup(key, &bucket, &collisions);
    return found;
  }
  void Insert(const Key &key, const Value &value) {
    assert(false);
  }
  void Erase(const Key &key) { assert(false); }
  void Clear() { assert(false); }
  uint64_t bytes_allocated() const { return bytes_allocated_; }
  static double GetEntrySize() {
    assert(false);
  }
  void GetCollisionStats(uint64_t *num_collisions,
                         uint32_t *max_collisions) const
  {
    assert(false);
  }

public:
  uint32_t ScaleHash(const Key &key) const {
    double bucket = (double(hasher_(key)) * double(capacity_) /
                     double((uint32_t)(-1)));
    return (uint32_t)bucket % capacity_;
  }
  void InitMemory() { assert(false); }
  bool DoInsert(const Key &key, const Value &value,
                const bool count_collisions)
  {
    assert(false);
  }
  bool DoLookup(const Key &key, uint32_t *bucket, uint32_t *collisions) const {
    *bucket = ScaleHash(key);
    *collisions = 0;
    while (!(keys_[*bucket] == empty_key_)) {
      if (keys_[*bucket] == key)
        return true;
      *bucket = (*bucket+1) % capacity_;
      (*collisions)++;
    }
    return false;
  }
  void DoClear(const bool reset_capacity) {
    assert(false);
  }
  // Methods for resizable version
  void SetThresholds() { }
  void Grow() { }
  void Shrink() { }
  void ResetCapacity() { }

  // Separate key and value arrays for better locality
  Key *keys_;
  Value *values_;
  uint32_t capacity_;
  uint32_t initial_capacity_;
  uint32_t size_;
  uint32_t (*hasher_)(const Key &key);
  uint64_t bytes_allocated_;
  uint64_t num_collisions_;
  uint32_t max_collisions_;  /**< maximum collisions for a single insert */
  Key empty_key_;
};

template<class Key, class Value>
class SmallHashDynamic :
public SmallHashBase< Key, Value, SmallHashDynamic<Key, Value> >
{
  friend class SmallHashBase< Key, Value, SmallHashDynamic<Key, Value> >;
public:
  typedef SmallHashBase< Key, Value, SmallHashDynamic<Key, Value> > Base;
  static const double kThresholdGrow;
  static const double kThresholdShrink;

  SmallHashDynamic() : Base() {
    assert(false);
  }
  explicit SmallHashDynamic(const SmallHashDynamic<Key, Value> &other) : Base()
  {
    assert(false);
  }
  SmallHashDynamic<Key, Value> &operator= (
    const SmallHashDynamic<Key, Value> &other)
  {
    assert(false);
  }

  uint32_t capacity() const { return Base::capacity_; }
  uint32_t size() const { return Base::size_; }
  uint32_t num_migrates() const { assert(false); }
protected:
  void SetThresholds() {
    assert(false);
  }
  void Grow() { assert(false); }
  void Shrink() { assert(false); }
  void ResetCapacity() { assert(false); }

private:
  void Migrate(const uint32_t new_capacity) {
    assert(false);
  }
  void CopyFrom(const SmallHashDynamic<Key, Value> &other) {
    assert(false);
  }
  uint32_t num_migrates_;
  uint32_t threshold_grow_;
  uint32_t threshold_shrink_;
};


class PathMap {
public:
  PathMap() {
    assert(false);
  }
  bool LookupPath(const shash::Md5 &md5path, PathString *path) {
    PathInfo value;
    bool found = map_.Lookup(md5path, &value);
    path->Assign(value.path);
    return found;
  }
  uint64_t LookupInode(const PathString &path) {
    PathInfo value;
    bool found = map_.Lookup(shash::Md5(path.GetChars(), path.GetLength()),
                             &value);
    if (found) return value.inode;
    return 0;
  }
  shash::Md5 Insert(const PathString &path, const uint64_t inode) {
    assert(false);
  }
  void Erase(const shash::Md5 &md5path) {
    assert(false);
  }
  void Clear() { assert(false); }
 public:
  struct PathInfo {
    PathInfo() { inode = 0; }
    PathInfo(const uint64_t i, const PathString &p) { inode = i; path = p; }
    uint64_t inode;
    PathString path;
  };
  SmallHashDynamic<shash::Md5, PathInfo> map_;
};

class InodeMap {
public:
  InodeMap() {
    assert(false);
  }
  bool LookupMd5Path(const uint64_t inode, shash::Md5 *md5path) {
    bool found = map_.Lookup(inode, md5path);
    return found;
  }
  void Insert(const uint64_t inode, const shash::Md5 &md5path) {
    assert(false);
  }
  void Erase(const uint64_t inode) {
    assert(false);
  }
  void Clear() { assert(false); }
 public:
  SmallHashDynamic<uint64_t, shash::Md5> map_;
};


class InodeReferences {
public:
  InodeReferences() {
    assert(false);
  }
  bool Get(const uint64_t inode, const uint32_t by) {
    assert(false);
  }
  bool Put(const uint64_t inode, const uint32_t by) {
    assert(false);
  }
  void Clear() { assert(false); }
 public:
  SmallHashDynamic<uint64_t, uint32_t> map_;
};

class InodeTracker {
public:
  struct Statistics {
    Statistics() { assert(false); }
    std::string Print() { assert(false); }
    atomic_int64 num_inserts;
    atomic_int64 num_removes;
    atomic_int64 num_references;
    atomic_int64 num_hits_inode;
    atomic_int64 num_hits_path;
    atomic_int64 num_misses_path;
  };
  Statistics GetStatistics() { assert(false); }

  InodeTracker() { assert(false); }
  explicit InodeTracker(const InodeTracker &other) { assert(false); }
  InodeTracker &operator= (const InodeTracker &other) { assert(false); }
  ~InodeTracker() {
    pthread_mutex_destroy(lock_);
    free(lock_);
  };
  void VfsGetBy(const uint64_t inode, const uint32_t by, const PathString &path)
  {
    assert(false);
  }
  void VfsGet(const uint64_t inode, const PathString &path) {
    assert(false);
  }
  void VfsPut(const uint64_t inode, const uint32_t by) {
    assert(false);
  }
  bool FindPath(const uint64_t inode, PathString *path) {
    //Lock();
    shash::Md5 md5path;
    bool found = inode_map_.LookupMd5Path(inode, &md5path);
    if (found) {
      found = path_map_.LookupPath(md5path, path);
      assert(found);
    }
    //Unlock();
    //if (found) atomic_inc64(&statistics_.num_hits_path);
    //else atomic_inc64(&statistics_.num_misses_path);
    return found;
  }

  uint64_t FindInode(const PathString &path) {
    assert(false);
  }

 public:
  static const unsigned kVersion = 2;

  void InitLock() { assert(false); }
  void CopyFrom(const InodeTracker &other) { assert(false); }
  inline void Lock() const { assert(false); }
  inline void Unlock() const { assert(false); }

  unsigned version_;
  pthread_mutex_t *lock_;
  PathMap path_map_;
  InodeMap inode_map_;
  InodeReferences inode_references_;
  Statistics statistics_;
};

void Migrate(InodeTracker *old_tracker, glue::InodeTracker *new_tracker);

}  // namespace inode_tracker_v2

}  // namespace compat

#endif  // CVMFS_COMPAT_H_
