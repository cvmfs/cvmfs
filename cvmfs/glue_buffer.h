/**
 * This file is part of the CernVM File System.
 *
 * This module provides the inode tracker in order to remember inodes
 * and their parents that are in use by the kernel.
 *
 * These objects have to survive reloading of the library, so no virtual
 * functions.
 */

#include <google/sparse_hash_map>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>

#include <cassert>
#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "atomic.h"
#include "catalog_mgr.h"
#include "hash.h"
#include "shortstring.h"
#include "smallhash.h"
#include "smalloc.h"
#include "util.h"

#ifndef CVMFS_GLUE_BUFFER_H_
#define CVMFS_GLUE_BUFFER_H_

namespace glue {

static inline uint32_t hasher_md5(const shash::Md5 &key) {
  // Don't start with the first bytes, because == is using them as well
  return (uint32_t) *(reinterpret_cast<const uint32_t *>(key.digest) + 1);
}


static inline uint32_t hasher_inode(const uint64_t &inode) {
  return MurmurHash2(&inode, sizeof(inode), 0x07387a4f);
}


//------------------------------------------------------------------------------


/**
 * Pointer to a 2 byte length information followed by the characters
 */
class StringRef {
 public:
  StringRef() {
    length_ = NULL;
  }

  uint16_t length() const { return *length_; }
  uint16_t size() const { return sizeof(uint16_t) + *length_; }
  static uint16_t size(const uint16_t length) {
    return sizeof(uint16_t) + length;
  }
  char *data() const { return reinterpret_cast<char *>(length_ + 1); }
  static StringRef Place(const uint16_t length, const char *str,
                         void *addr)
  {
    StringRef result;
    result.length_ = reinterpret_cast<uint16_t *>(addr);
    *result.length_ = length;
    if (length > 0)
      memcpy(result.length_ + 1, str, length);
    return result;
  }
 private:
  uint16_t *length_;
};


//------------------------------------------------------------------------------


/**
 * Manages memory bins with immutable strings (deleting is a no-op).
 * When the fraction of garbage is too large, the user of the StringHeap
 * can copy the entire contents to a new heap.
 */
class StringHeap : public SingleCopy {
 public:
  StringHeap() {
    Init(128*1024);  // 128kB (should be >= 64kB+2B which is largest string)
  }

  explicit StringHeap(const uint32_t minimum_size) {
    Init(minimum_size);
  }

  void Init(const uint32_t minimum_size) {
    size_ = 0;
    used_ = 0;

    // Initial bin: 128kB or smallest power of 2 >= minimum size
    uint32_t pow2_size = minimum_size < 128*1024 ? 128*1024 : minimum_size;
    pow2_size--;
    pow2_size |= pow2_size >> 1;
    pow2_size |= pow2_size >> 2;
    pow2_size |= pow2_size >> 4;
    pow2_size |= pow2_size >> 8;
    pow2_size |= pow2_size >> 16;
    pow2_size++;
    AddBin(pow2_size);
  }

  ~StringHeap() {
    for (unsigned i = 0; i < bins_.size(); ++i) {
      smunmap(bins_.At(i));
    }
  }

  StringRef AddString(const uint16_t length, const char *str) {
    const uint16_t str_size = StringRef::size(length);
    const uint64_t remaining_bin_size = bin_size_ - bin_used_;
    // May require opening of new bin
    if (remaining_bin_size < str_size) {
      size_ += remaining_bin_size;
      AddBin(2*bin_size_);
    }
    StringRef result =
      StringRef::Place(length, str,
                       static_cast<char *>(bins_.At(bins_.size()-1))+bin_used_);
    size_ += str_size;
    used_ += str_size;
    bin_used_ += str_size;
    return result;
  }

  void RemoveString(const StringRef str_ref) {
    used_ -= str_ref.size();
  }

  double GetUsage() const {
    if (size_ == 0) return 1.0;
    return static_cast<double>(used_) / static_cast<double>(size_);
  }

  uint64_t used() const { return used_; }

 private:
  void AddBin(const uint64_t size) {
    void *bin = smmap(size);
    bins_.PushBack(bin);
    bin_size_ = size;
    bin_used_ = 0;
  }

  uint64_t size_;
  uint64_t used_;
  uint64_t bin_size_;
  uint64_t bin_used_;
  BigVector<void *> bins_;
};


//------------------------------------------------------------------------------


class PathStore {
 public:
  PathStore() {
    map_.Init(16, shash::Md5(shash::AsciiPtr("!")), hasher_md5);
    string_heap_ = new StringHeap();
  }

  ~PathStore() {
    delete string_heap_;
  }

  explicit PathStore(const PathStore &other);
  PathStore &operator= (const PathStore &other);

  void Insert(const shash::Md5 &md5path, const PathString &path) {
    PathInfo info;
    bool found = map_.Lookup(md5path, &info);
    if (found) {
      info.refcnt++;
      map_.Insert(md5path, info);
      return;
    }

    PathInfo new_entry;
    if (path.IsEmpty()) {
      new_entry.name = string_heap_->AddString(0, "");
      map_.Insert(md5path, new_entry);
      return;
    }

    PathString parent_path = GetParentPath(path);
    new_entry.parent = shash::Md5(parent_path.GetChars(),
                                  parent_path.GetLength());
    Insert(new_entry.parent, parent_path);

    const uint16_t name_length = path.GetLength() - parent_path.GetLength() - 1;
    const char *name_str = path.GetChars() + parent_path.GetLength() + 1;
    new_entry.name = string_heap_->AddString(name_length, name_str);
    map_.Insert(md5path, new_entry);
  }

  bool Lookup(const shash::Md5 &md5path, PathString *path) {
    PathInfo info;
    bool retval = map_.Lookup(md5path, &info);
    if (!retval)
      return false;

    if (info.parent.IsNull())
      return true;

    retval = Lookup(info.parent, path);
    assert(retval);
    path->Append("/", 1);
    path->Append(info.name.data(), info.name.length());
    return true;
  }

  void Erase(const shash::Md5 &md5path) {
    PathInfo info;
    bool found = map_.Lookup(md5path, &info);
    if (!found)
      return;

    info.refcnt--;
    if (info.refcnt == 0) {
      map_.Erase(md5path);
      string_heap_->RemoveString(info.name);
      if (string_heap_->GetUsage() < 0.75) {
        StringHeap *new_string_heap = new StringHeap(string_heap_->used());
        shash::Md5 empty_path = map_.empty_key();
        for (unsigned i = 0; i < map_.capacity(); ++i) {
          if (map_.keys()[i] != empty_path) {
            (map_.values() + i)->name =
              new_string_heap->AddString(map_.values()[i].name.length(),
                                         map_.values()[i].name.data());
          }
        }
        delete string_heap_;
        string_heap_ = new_string_heap;
      }
      Erase(info.parent);
    } else {
      map_.Insert(md5path, info);
    }
  }

  void Clear() {
    map_.Clear();
    delete string_heap_;
    string_heap_ = new StringHeap();
  }

 private:
  struct PathInfo {
    PathInfo() {
      refcnt = 1;
    }
    shash::Md5 parent;
    uint32_t refcnt;
    StringRef name;
  };

  void CopyFrom(const PathStore &other);

  SmallHashDynamic<shash::Md5, PathInfo> map_;
  StringHeap *string_heap_;
};


//------------------------------------------------------------------------------


class PathMap {
 public:
  PathMap() {
    map_.Init(16, shash::Md5(shash::AsciiPtr("!")), hasher_md5);
  }

  bool LookupPath(const shash::Md5 &md5path, PathString *path) {
    bool found = path_store_.Lookup(md5path, path);
    return found;
  }

  uint64_t LookupInode(const PathString &path) {
    uint64_t inode;
    bool found = map_.Lookup(shash::Md5(path.GetChars(), path.GetLength()),
                             &inode);
    if (found) return inode;
    return 0;
  }

  shash::Md5 Insert(const PathString &path, const uint64_t inode) {
    shash::Md5 md5path(path.GetChars(), path.GetLength());
    if (!map_.Contains(md5path)) {
      path_store_.Insert(md5path, path);
      map_.Insert(md5path, inode);
    }
    return md5path;
  }

  void Erase(const shash::Md5 &md5path) {
    bool found = map_.Contains(md5path);
    if (found) {
      path_store_.Erase(md5path);
      map_.Erase(md5path);
    }
  }

  void Clear() {
    map_.Clear();
    path_store_.Clear();
  }

 private:
  SmallHashDynamic<shash::Md5, uint64_t> map_;
  PathStore path_store_;
};


//------------------------------------------------------------------------------


class InodeMap {
 public:
  InodeMap() {
    map_.Init(16, 0, hasher_inode);
  }

  bool LookupMd5Path(const uint64_t inode, shash::Md5 *md5path) {
    bool found = map_.Lookup(inode, md5path);
    return found;
  }

  void Insert(const uint64_t inode, const shash::Md5 &md5path) {
    map_.Insert(inode, md5path);
  }

  void Erase(const uint64_t inode) {
    map_.Erase(inode);
  }

  void Clear() { map_.Clear(); }

 private:
  SmallHashDynamic<uint64_t, shash::Md5> map_;
};


//------------------------------------------------------------------------------


class InodeReferences {
 public:
  InodeReferences() {
    map_.Init(16, 0, hasher_inode);
  }

  bool Get(const uint64_t inode, const uint32_t by) {
    uint32_t refcounter = 0;
    const bool found = map_.Lookup(inode, &refcounter);
    const bool new_inode = !found;
    refcounter += by;  // This is 0 if the inode is not found
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


//------------------------------------------------------------------------------


/**
 * Tracks inode reference counters as given by Fuse.
 */
class InodeTracker {
 public:
  // Cannot be moved to the statistics manager because it has to survive
  // reloads.  Added manually in the fuse module initialization and in talk.cc.
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

  void VfsGetBy(const uint64_t inode, const uint32_t by, const PathString &path)
  {
    Lock();
    bool new_inode = inode_references_.Get(inode, by);
    shash::Md5 md5path = path_map_.Insert(path, inode);
    inode_map_.Insert(inode, md5path);
    Unlock();

    atomic_xadd64(&statistics_.num_references, by);
    if (new_inode) atomic_inc64(&statistics_.num_inserts);
  }

  void VfsGet(const uint64_t inode, const PathString &path) {
    VfsGetBy(inode, 1, path);
  }

  void VfsPut(const uint64_t inode, const uint32_t by) {
    Lock();
    bool removed = inode_references_.Put(inode, by);
    if (removed) {
      // TODO(jblomer): pop operation (Lookup+Erase)
      shash::Md5 md5path;
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
    shash::Md5 md5path;
    bool found = inode_map_.LookupMd5Path(inode, &md5path);
    if (found) {
      found = path_map_.LookupPath(md5path, path);
      assert(found);
    }
    Unlock();

    if (found) {
      atomic_inc64(&statistics_.num_hits_path);
    } else {
      atomic_inc64(&statistics_.num_misses_path);
    }
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
  static const unsigned kVersion = 4;

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
