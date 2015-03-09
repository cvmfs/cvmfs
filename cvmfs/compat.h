/**
 * This file is part of the CernVM File System.
 *
 * A mediator to transform old data structures into new one on reload
 */

#ifndef CVMFS_COMPAT_H_
#define CVMFS_COMPAT_H_

#include <google/sparse_hash_map>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>

#include <cassert>
#include <string>

#include "atomic.h"
#include "bigvector.h"
#include "catalog_mgr.h"
#include "file_chunk.h"
#include "glue_buffer.h"
#include "hash.h"
#include "shortstring.h"
#include "util.h"

namespace compat {

namespace shash_v1 {

enum Algorithms {
  kMd5 = 0,
  kSha1,
  kRmd160,
  kAny,
};
const unsigned kDigestSizes[] = {16, 20, 20, 20};
const unsigned kMaxDigestSize = 20;
extern const char *kSuffixes[];
const unsigned kSuffixLengths[] = {0, 0, 7, 0};
const unsigned kMaxSuffixLength = 7;

template<unsigned digest_size_, Algorithms algorithm_>
struct Digest {
  unsigned char digest[digest_size_];
  Algorithms algorithm;

  unsigned GetDigestSize() const { return kDigestSizes[algorithm]; }
  unsigned GetHexSize() const {
    return 2*kDigestSizes[algorithm] + kSuffixLengths[algorithm];
  }

  Digest() {
    algorithm = algorithm_;
    memset(digest, 0, digest_size_);
  }

  Digest(const Algorithms a,
         const unsigned char *digest_buffer, const unsigned buffer_size)
  {
    algorithm = a;
    assert(buffer_size <= digest_size_);
    memcpy(digest, digest_buffer, buffer_size);
  }

  std::string ToString() const {
    const unsigned string_length = GetHexSize();
    std::string result(string_length, 0);

    unsigned i;
    for (i = 0; i < kDigestSizes[algorithm]; ++i) {
      char dgt1 = (unsigned)digest[i] / 16;
      char dgt2 = (unsigned)digest[i] % 16;
      dgt1 += (dgt1 <= 9) ? '0' : 'a' - 10;
      dgt2 += (dgt2 <= 9) ? '0' : 'a' - 10;
      result[i*2] = dgt1;
      result[i*2+1] = dgt2;
    }
    unsigned pos = i*2;
    for (const char *s = kSuffixes[algorithm]; *s != '\0'; ++s) {
      result[pos] = *s;
      pos++;
    }
    return result;
  }

  bool IsNull() const {
    for (unsigned i = 0; i < kDigestSizes[algorithm]; ++i)
      if (digest[i] != 0)
        return false;
    return true;
  }

  bool operator ==(const Digest<digest_size_, algorithm_> &other) const {
    if (this->algorithm != other.algorithm)
      return false;
    for (unsigned i = 0; i < kDigestSizes[algorithm]; ++i)
      if (this->digest[i] != other.digest[i])
        return false;
    return true;
  }

  bool operator !=(const Digest<digest_size_, algorithm_> &other) const {
    return !(*this == other);
  }

  bool operator <(const Digest<digest_size_, algorithm_> &other) const {
    if (this->algorithm != other.algorithm)
      return (this->algorithm < other.algorithm);
    for (unsigned i = 0; i < kDigestSizes[algorithm]; ++i) {
      if (this->digest[i] > other.digest[i])
        return false;
      if (this->digest[i] < other.digest[i])
        return true;
    }
    return false;
  }

  bool operator >(const Digest<digest_size_, algorithm_> &other) const {
    if (this->algorithm != other.algorithm)
      return (this->algorithm > other.algorithm);
    for (int i = 0; i < kDigestSizes[algorithm]; ++i) {
      if (this->digest[i] < other.digest[i])
        return false;
      if (this->digest[i] > other.digest[i])
        return true;
    }
    return false;
  }
};

struct Md5 : public Digest<16, kMd5> {
  Md5() : Digest<16, kMd5>() { }
  Md5(const char *chars, const unsigned length);
};

struct Any : public Digest<20, kAny> {
  Any() : Digest<20, kAny>() { }
};

void MigrateAny(const Any *old_hash, shash::Any *new_hash);

}  // namespace shash_v1


//------------------------------------------------------------------------------


namespace inode_tracker {

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
// private:
  std::string DebugPrint() { assert(false); return ""; }
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

// private:
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


//------------------------------------------------------------------------------


namespace inode_tracker_v2 {

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

// private:
  uint32_t ScaleHash(const Key &key) const {
    double bucket = (double(hasher_(key)) * double(capacity_) /  // NOLINT
                     double((uint32_t)(-1)));  // NOLINT
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
  bool LookupPath(const shash_v1::Md5 &md5path, PathString *path) {
    PathInfo value;
    bool found = map_.Lookup(md5path, &value);
    path->Assign(value.path);
    return found;
  }
  uint64_t LookupInode(const PathString &path) {
    PathInfo value;
    bool found = map_.Lookup(shash_v1::Md5(path.GetChars(), path.GetLength()),
                             &value);
    if (found) return value.inode;
    return 0;
  }
  shash_v1::Md5 Insert(const PathString &path, const uint64_t inode) {
    assert(false);
  }
  void Erase(const shash_v1::Md5 &md5path) {
    assert(false);
  }
  void Clear() { assert(false); }

// private:
  struct PathInfo {
    PathInfo() { inode = 0; }
    PathInfo(const uint64_t i, const PathString &p) { inode = i; path = p; }
    uint64_t inode;
    PathString path;
  };
  SmallHashDynamic<shash_v1::Md5, PathInfo> map_;
};

class InodeMap {
 public:
  InodeMap() {
    assert(false);
  }
  bool LookupMd5Path(const uint64_t inode, shash_v1::Md5 *md5path) {
    bool found = map_.Lookup(inode, md5path);
    return found;
  }
  void Insert(const uint64_t inode, const shash_v1::Md5 &md5path) {
    assert(false);
  }
  void Erase(const uint64_t inode) {
    assert(false);
  }
  void Clear() { assert(false); }
// private:
  SmallHashDynamic<uint64_t, shash_v1::Md5> map_;
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
// private:
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
  }
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
    // Lock();
    shash_v1::Md5 md5path;
    bool found = inode_map_.LookupMd5Path(inode, &md5path);
    if (found) {
      found = path_map_.LookupPath(md5path, path);
      assert(found);
    }
    // Unlock();
    // if (found) atomic_inc64(&statistics_.num_hits_path);
    // else atomic_inc64(&statistics_.num_misses_path);
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


//------------------------------------------------------------------------------


namespace inode_tracker_v3 {

class StringRef {
 public:
  StringRef() { length_ = NULL; }
  uint16_t length() const { return *length_; }
  uint16_t size() const { return sizeof(uint16_t) + *length_; }
  static uint16_t size(const uint16_t length) {
    return sizeof(uint16_t) + length;
  }
  char *data() const { return reinterpret_cast<char *>(length_ + 1); }
  static StringRef Place(const uint16_t length, const char *str,
                         void *addr)
  {
    assert(false);
  }
 private:
  uint16_t *length_;
};

class StringHeap : public SingleCopy {
 public:
  StringHeap() { assert(false); }
  explicit StringHeap(const uint32_t minimum_size) { assert(false); }
  void Init(const uint32_t minimum_size) { assert(false); }

  ~StringHeap() {
    for (unsigned i = 0; i < bins_.size(); ++i) {
      smunmap(bins_.At(i));
    }
  }

  StringRef AddString(const uint16_t length, const char *str) {
    assert(false);
  }
  void RemoveString(const StringRef str_ref) { assert(false); }
  double GetUsage() const { assert(false); }
  uint64_t used() const { assert(false); }

 private:
  void AddBin(const uint64_t size) { assert(false); }

  uint64_t size_;
  uint64_t used_;
  uint64_t bin_size_;
  uint64_t bin_used_;
  BigVector<void *> bins_;
};


class PathStore {
 public:
  PathStore() { assert(false); }
  ~PathStore() {
    delete string_heap_;
  }
  explicit PathStore(const PathStore &other) { assert(false); }
  PathStore &operator= (const PathStore &other) { assert(false); }

  void Insert(const shash_v1::Md5 &md5path, const PathString &path) {
    assert(false);
  }

  bool Lookup(const shash_v1::Md5 &md5path, PathString *path) {
    PathInfo info;
    bool retval = map_.Lookup(md5path, &info);
    if (!retval)
      return false;

    if (info.parent.IsNull()) {
      return true;
    }

    retval = Lookup(info.parent, path);
    assert(retval);
    path->Append("/", 1);
    path->Append(info.name.data(), info.name.length());
    return true;
  }

  void Erase(const shash_v1::Md5 &md5path) { assert(false); }
  void Clear() { assert(false); }

// private:
  struct PathInfo {
    PathInfo() {
      refcnt = 1;
    }
    shash_v1::Md5 parent;
    uint32_t refcnt;
    StringRef name;
  };
  void CopyFrom(const PathStore &other) { assert(false); }
  SmallHashDynamic<shash_v1::Md5, PathInfo> map_;
  StringHeap *string_heap_;
};


class PathMap {
 public:
  PathMap() {
    assert(false);
  }
  bool LookupPath(const shash_v1::Md5 &md5path, PathString *path) {
    bool found = path_store_.Lookup(md5path, path);
    return found;
  }
  uint64_t LookupInode(const PathString &path) { assert(false); }
  shash_v1::Md5 Insert(const PathString &path, const uint64_t inode) {
    assert(false);
  }
  void Erase(const shash_v1::Md5 &md5path) {
    assert(false);
  }
  void Clear() { assert(false); }
 public:
  SmallHashDynamic<shash_v1::Md5, uint64_t> map_;
  PathStore path_store_;
};

class InodeMap {
 public:
  InodeMap() {
    assert(false);
  }
  bool LookupMd5Path(const uint64_t inode, shash_v1::Md5 *md5path) {
    bool found = map_.Lookup(inode, md5path);
    return found;
  }
  void Insert(const uint64_t inode, const shash_v1::Md5 &md5path) {
    assert(false);
  }
  void Erase(const uint64_t inode) {
    assert(false);
  }
  void Clear() { assert(false); }
// private:
  SmallHashDynamic<uint64_t, shash_v1::Md5> map_;
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
// private:
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
  }
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
    // Lock();
    shash_v1::Md5 md5path;
    bool found = inode_map_.LookupMd5Path(inode, &md5path);
    if (found) {
      found = path_map_.LookupPath(md5path, path);
      assert(found);
    }
    // Unlock();
    // if (found) atomic_inc64(&statistics_.num_hits_path);
    // else atomic_inc64(&statistics_.num_misses_path);
    return found;
  }

  uint64_t FindInode(const PathString &path) {
    assert(false);
  }

// private:
  static const unsigned kVersion = 3;

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

}  // namespace inode_tracker_v3


namespace chunk_tables {

class FileChunk {
 public:
  FileChunk() { assert(false); }
  FileChunk(const shash_v1::Any &hash, const off_t offset, const size_t size) {
    assert(false);
  }
  inline const shash_v1::Any& content_hash() const { return content_hash_; }
  inline off_t offset() const { return offset_; }
  inline size_t size() const { return size_; }

// protected:
  shash_v1::Any content_hash_;  //!< content hash of the compressed file chunk
  off_t offset_;                //!< byte offset in the uncompressed input file
  size_t size_;                 //!< uncompressed size of the data chunk
};

struct FileChunkReflist {
  FileChunkReflist() { assert(false); }
  FileChunkReflist(BigVector<FileChunk> *l, const PathString &p) {
    assert(false);
  }
  BigVector<FileChunk> *list;
  PathString path;
};

struct ChunkTables {
  ChunkTables() { assert(false); }
  ~ChunkTables();
  ChunkTables(const ChunkTables &other) { assert(false); }
  ChunkTables &operator= (const ChunkTables &other) { assert(false); }
  void CopyFrom(const ChunkTables &other) { assert(false); }
  void InitLocks() { assert(false); }
  void InitHashmaps() { assert(false); }
  pthread_mutex_t *Handle2Lock(const uint64_t handle) const { assert(false); }
  inline void Lock() { assert(false); }
  inline void Unlock() { assert(false); }

  int version;
  static const unsigned kNumHandleLocks = 128;
  SmallHashDynamic<uint64_t, ::ChunkFd> handle2fd;
  // The file descriptors attached to handles need to be locked.
  // Using a hash map to survive with a small, fixed number of locks
  BigVector<pthread_mutex_t *> handle_locks;
  SmallHashDynamic<uint64_t, FileChunkReflist> inode2chunks;
  SmallHashDynamic<uint64_t, uint32_t> inode2references;
  uint64_t next_handle;
  pthread_mutex_t *lock;
};

void Migrate(ChunkTables *old_tables, ::ChunkTables *new_tables);

}  // namespace chunk_tables

}  // namespace compat

#endif  // CVMFS_COMPAT_H_
