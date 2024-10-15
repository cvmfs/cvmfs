/**
 * This file is part of the CernVM File System.
 *
 * This module provides the inode tracker in order to remember inodes
 * and their parents that are in use by the kernel.
 *
 * These objects have to survive reloading of the library, so no virtual
 * functions.
 */

#include <gtest/gtest_prod.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>

#include <cassert>
#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "bigqueue.h"
#include "bigvector.h"
#include "crypto/hash.h"
#include "directory_entry.h"
#include "shortstring.h"
#include "smallhash.h"
#include "util/atomic.h"
#include "util/exception.h"
#include "util/mutex.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/smalloc.h"
#include "util/string.h"

#ifndef CVMFS_GLUE_BUFFER_H_
#define CVMFS_GLUE_BUFFER_H_

namespace glue {

/**
 * Inode + file type. Stores the file type in the 4 most significant bits
 * of the 64 bit unsigned integer representing the inode.  That makes the class
 * compatible with a pure 64bit inode used in previous cvmfs versions in the
 * inode tracker.  The file type is stored using the POSIX representation in
 * the inode's mode field.
 * Note that InodeEx, used as a hash table key, hashes only over the inode part.
 */
class InodeEx {
 private:
  // Extracts the file type bits from the POSIX mode field and shifts them to
  // the right so that they align with EFileType constants.
  static inline uint64_t ShiftMode(unsigned mode) { return (mode >> 12) & 017; }

 public:
  enum EFileType {
    kUnknownType = 0,
    kRegular     = 010,
    kSymlink     = 012,
    kDirectory   = 004,
    kFifo        = 001,
    kSocket      = 014,
    kCharDev     = 002,
    kBulkDev     = 006,
  };

  InodeEx() : inode_ex_(0) { }
  InodeEx(uint64_t inode, EFileType type)
    : inode_ex_(inode | (static_cast<uint64_t>(type) << 60))
  { }
  InodeEx(uint64_t inode, unsigned mode)
    : inode_ex_(inode | (ShiftMode(mode) << 60))
  { }

  inline uint64_t GetInode() const { return inode_ex_ & ~(uint64_t(15) << 60); }
  inline EFileType GetFileType() const {
    return static_cast<EFileType>(inode_ex_ >> 60);
  }

  inline bool operator==(const InodeEx &other) const {
    return GetInode() == other.GetInode();
  }
  inline bool operator!=(const InodeEx &other) const {
    return GetInode() != other.GetInode();
  }

  inline bool IsCompatibleFileType(unsigned mode) const {
    return (static_cast<uint64_t>(GetFileType()) == ShiftMode(mode)) ||
           (GetFileType() == kUnknownType);
  }

 private:
  uint64_t inode_ex_;
};

static inline uint32_t hasher_md5(const shash::Md5 &key) {
  // Don't start with the first bytes, because == is using them as well
  return (uint32_t) *(reinterpret_cast<const uint32_t *>(key.digest) + 1);
}

static inline uint32_t hasher_inode(const uint64_t &inode) {
  return MurmurHash2(&inode, sizeof(inode), 0x07387a4f);
}

static inline uint32_t hasher_inode_ex(const InodeEx &inode_ex) {
  return hasher_inode(inode_ex.GetInode());
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

  explicit StringHeap(const uint64_t minimum_size) {
    Init(minimum_size);
  }

  void Init(const uint64_t minimum_size) {
    size_ = 0;
    used_ = 0;

    // Initial bin: 128kB or smallest power of 2 >= minimum size
    uint64_t pow2_size = 128 * 1024;
    while (pow2_size < minimum_size)
      pow2_size *= 2;
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

  // mmap'd bytes, used for testing
  uint64_t GetSizeAlloc() const {
    uint64_t s = bin_size_;
    uint64_t result = 0;
    for (unsigned i = 0; i < bins_.size(); ++i) {
      result += s;
      s /= 2;
    }
    return result;
  }

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
  /**
   * Used to enumerate all paths
   */
  struct Cursor {
    Cursor() : idx(0) { }
    uint32_t idx;
  };


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

  Cursor BeginEnumerate() {
    return Cursor();
  }

  bool Next(Cursor *cursor, shash::Md5 *parent, StringRef *name) {
    shash::Md5 empty_key = map_.empty_key();
    while (cursor->idx < map_.capacity()) {
      if (map_.keys()[cursor->idx] == empty_key) {
        cursor->idx++;
        continue;
      }
      *parent = map_.values()[cursor->idx].parent;
      *name = map_.values()[cursor->idx].name;
      cursor->idx++;
      return true;
    }
    return false;
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


/**
 * A vector of stat structs. When removing items, the empty slot is swapped
 * with the last element so that there are no gaps in the vector.  The memory
 * allocation of the vector grows and shrinks with the size.
 * Removal of items returns the inode of the element swapped with the gap so
 * that the page entry tracker can update its index.
 */
class StatStore {
 public:
  int32_t Add(const struct stat &info) {
    // We don't support more that 2B open files
    assert(store_.size() < (1LU << 31));
    int32_t index = static_cast<int>(store_.size());
    store_.PushBack(info);
    return index;
  }

  // Note that that if the last element is removed, no swap has taken place
  uint64_t Erase(int32_t index) {
    struct stat info_back = store_.At(store_.size() - 1);
    store_.Replace(index, info_back);
    store_.SetSize(store_.size() - 1);
    store_.ShrinkIfOversized();
    return info_back.st_ino;
  }

  struct stat Get(int32_t index) const { return store_.At(index); }

 private:
  BigVector<struct stat> store_;
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

  uint64_t LookupInodeByPath(const PathString &path) {
    uint64_t inode;
    bool found = map_.Lookup(shash::Md5(path.GetChars(), path.GetLength()),
                             &inode);
    if (found) return inode;
    return 0;
  }

  uint64_t LookupInodeByMd5Path(const shash::Md5 &md5path) {
    uint64_t inode;
    bool found = map_.Lookup(md5path, &inode);
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

  void Replace(const shash::Md5 &md5path, uint64_t new_inode) {
    map_.Insert(md5path, new_inode);
  }

  void Clear() {
    map_.Clear();
    path_store_.Clear();
  }

  // For enumerating
  PathStore *path_store() { return &path_store_; }

 private:
  SmallHashDynamic<shash::Md5, uint64_t> map_;
  PathStore path_store_;
};


//------------------------------------------------------------------------------


/**
 * This class has the same memory layout than the previous "InodeMap" class,
 * therefore there is no data structure migration during reload required.
 */
class InodeExMap {
 public:
  InodeExMap() {
    map_.Init(16, InodeEx(), hasher_inode_ex);
  }

  bool LookupMd5Path(InodeEx *inode_ex, shash::Md5 *md5path) {
    bool found = map_.LookupEx(inode_ex, md5path);
    return found;
  }

  void Insert(const InodeEx inode_ex, const shash::Md5 &md5path) {
    map_.Insert(inode_ex, md5path);
  }

  void Erase(const uint64_t inode) {
    map_.Erase(InodeEx(inode, InodeEx::kUnknownType));
  }

  void Clear() { map_.Clear(); }

 private:
  SmallHashDynamic<InodeEx, shash::Md5> map_;
};


//------------------------------------------------------------------------------


class InodeReferences {
 public:
  /**
   * Used to enumerate all inodes
   */
  struct Cursor {
    Cursor() : idx(0) { }
    uint32_t idx;
  };

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
    if (!found) {
      // May happen if a retired inode is cleared, i.e. if a file with
      // outdated content is closed
      return false;
    }

    if (refcounter < by) {
      PANIC(kLogSyslogErr | kLogDebug,
            "inode tracker refcount mismatch, inode % " PRIu64
            ", refcounts %u / %u", inode, refcounter, by);
    }

    if (refcounter == by) {
      map_.Erase(inode);
      return true;
    }
    refcounter -= by;
    map_.Insert(inode, refcounter);
    return false;
  }

  void Replace(const uint64_t old_inode, const uint64_t new_inode) {
    map_.Erase(old_inode);
    map_.Insert(new_inode, 0);
  }

  void Clear() {
    map_.Clear();
  }

  Cursor BeginEnumerate() {
    return Cursor();
  }

  bool Next(Cursor *cursor, uint64_t *inode) {
    uint64_t empty_key = map_.empty_key();
    while (cursor->idx < map_.capacity()) {
      if (map_.keys()[cursor->idx] == empty_key) {
        cursor->idx++;
        continue;
      }
      *inode = map_.keys()[cursor->idx];
      cursor->idx++;
      return true;
    }
    return false;
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
  /**
   * Used to actively evict all known paths from kernel caches
   */
  struct Cursor {
    explicit Cursor(
      const PathStore::Cursor &p,
      const InodeReferences::Cursor &i)
      : csr_paths(p)
      , csr_inos(i)
    { }
    PathStore::Cursor csr_paths;
    InodeReferences::Cursor csr_inos;
  };

  /**
   * To avoid taking the InodeTracker mutex multiple times, the fuse
   * forget_multi callback releases inodes references through this RAII object.
   * Copy and assign operator should be deleted but that would require
   * all compilers to use RVO. TODO(jblomer): fix with C++11
   */
  class VfsPutRaii {
   public:
    explicit VfsPutRaii(InodeTracker *t) : tracker_(t) {
      tracker_->Lock();
    }
    ~VfsPutRaii() { tracker_->Unlock(); }

    bool VfsPut(const uint64_t inode, const uint32_t by) {
      bool removed = tracker_->inode_references_.Put(inode, by);
      if (removed) {
        // TODO(jblomer): pop operation (Lookup+Erase)
        shash::Md5 md5path;
        InodeEx inode_ex(inode, InodeEx::kUnknownType);
        bool found = tracker_->inode_ex_map_.LookupMd5Path(&inode_ex, &md5path);
        if (!found) {
          PANIC(kLogSyslogErr | kLogDebug,
                "inode tracker ref map and path map out of sync: %" PRIu64,
                inode);
        }
        tracker_->inode_ex_map_.Erase(inode);
        tracker_->path_map_.Erase(md5path);
        atomic_inc64(&tracker_->statistics_.num_removes);
      }
      atomic_xadd64(&tracker_->statistics_.num_references, -int32_t(by));
      return removed;
    }

   private:
    InodeTracker *tracker_;
  };

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

  void VfsGetBy(const InodeEx inode_ex, const uint32_t by,
                const PathString &path)
  {
    uint64_t inode = inode_ex.GetInode();
    Lock();
    bool is_new_inode = inode_references_.Get(inode, by);
    shash::Md5 md5path = path_map_.Insert(path, inode);
    inode_ex_map_.Insert(inode_ex, md5path);
    Unlock();

    atomic_xadd64(&statistics_.num_references, by);
    if (is_new_inode) atomic_inc64(&statistics_.num_inserts);
  }

  void VfsGet(const InodeEx inode_ex, const PathString &path) {
    VfsGetBy(inode_ex, 1, path);
  }

  VfsPutRaii GetVfsPutRaii() { return VfsPutRaii(this); }

  bool FindPath(InodeEx *inode_ex, PathString *path) {
    Lock();
    shash::Md5 md5path;
    bool found = inode_ex_map_.LookupMd5Path(inode_ex, &md5path);
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
    uint64_t inode = path_map_.LookupInodeByPath(path);
    Unlock();
    atomic_inc64(&statistics_.num_hits_inode);
    return inode;
  }

  bool FindDentry(uint64_t ino, uint64_t *parent_ino, NameString *name) {
    PathString path;
    InodeEx inodex(ino, InodeEx::kUnknownType);
    shash::Md5 md5path;

    Lock();
    bool found = inode_ex_map_.LookupMd5Path(&inodex, &md5path);
    if (found) {
      found = path_map_.LookupPath(md5path, &path);
      assert(found);
      *name = GetFileName(path);
      path = GetParentPath(path);
      *parent_ino = path_map_.LookupInodeByPath(path);
    }
    Unlock();
    return found;
  }

  /**
   * The new inode has reference counter 0. Returns true if the inode was
   * found and replaced
   */
  bool ReplaceInode(uint64_t old_inode, const InodeEx &new_inode) {
    shash::Md5 md5path;
    InodeEx old_inode_ex(old_inode, InodeEx::kUnknownType);
    Lock();
    bool found = inode_ex_map_.LookupMd5Path(&old_inode_ex, &md5path);
    if (found) {
      inode_references_.Replace(old_inode, new_inode.GetInode());
      path_map_.Replace(md5path, new_inode.GetInode());
      inode_ex_map_.Erase(old_inode);
      inode_ex_map_.Insert(new_inode, md5path);
    }
    Unlock();
    return found;
  }

  Cursor BeginEnumerate() {
    Lock();
    return Cursor(path_map_.path_store()->BeginEnumerate(),
                  inode_references_.BeginEnumerate());
  }

  bool NextEntry(Cursor *cursor, uint64_t *inode_parent, NameString *name) {
    shash::Md5 parent_md5;
    StringRef name_ref;
    bool result = path_map_.path_store()->Next(
      &(cursor->csr_paths), &parent_md5, &name_ref);
    if (!result)
      return false;
    if (parent_md5.IsNull())
      *inode_parent = 0;
    else
      *inode_parent = path_map_.LookupInodeByMd5Path(parent_md5);
    name->Assign(name_ref.data(), name_ref.length());
    return true;
  }

  bool NextInode(Cursor *cursor, uint64_t *inode) {
    return inode_references_.Next(&(cursor->csr_inos), inode);
  }

  void EndEnumerate(Cursor *cursor) {
    Unlock();
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
  InodeExMap inode_ex_map_;
  InodeReferences inode_references_;
  Statistics statistics_;
};  // class InodeTracker


/**
 * Tracks fuse name lookup replies for active cache eviction.
 * Class renamed from previous name NentryTracker
 */
class DentryTracker {
  FRIEND_TEST(T_GlueBuffer, DentryTracker);

 private:
  struct Entry {
    Entry() : expiry(0), inode_parent(0) {}
    Entry(uint64_t e, uint64_t p, const char *n)
      : expiry(e)
      , inode_parent(p)
      , name(n, strlen(n))
    {}
    uint64_t expiry;
    uint64_t inode_parent;
    NameString name;
  };

 public:
  struct Cursor {
    explicit Cursor(Entry *h) : head(h), pos(0) {}
    Entry *head;
    size_t pos;
  };

  // Cannot be moved to the statistics manager because it has to survive
  // reloads.  Added manually in the fuse module initialization and in talk.cc.
  struct Statistics {
    Statistics() : num_insert(0), num_remove(0), num_prune(0) {}
    int64_t num_insert;
    int64_t num_remove;
    int64_t num_prune;
  };
  Statistics GetStatistics() { return statistics_; }

  static void *MainCleaner(void *data);

  DentryTracker();
  DentryTracker(const DentryTracker &other);
  DentryTracker &operator= (const DentryTracker &other);
  ~DentryTracker();

  /**
   * Lock object during copy
   */
  DentryTracker *Move();

  void Add(const uint64_t inode_parent, const char *name, uint64_t timeout_s) {
    if (!is_active_) return;
    if (timeout_s == 0) return;

    uint64_t now = platform_monotonic_time();
    Lock();
    entries_.PushBack(Entry(now + timeout_s, inode_parent, name));
    statistics_.num_insert++;
    DoPrune(now);
    Unlock();
  }

  void Prune();
  /**
   * The nentry tracker is only needed for active cache eviction and can
   * otherwise ignore new entries.
   */
  void Disable() { is_active_ = false; }
  bool is_active() const { return is_active_; }

  void SpawnCleaner(unsigned interval_s);

  Cursor BeginEnumerate();
  bool NextEntry(Cursor *cursor, uint64_t *inode_parent, NameString *name);
  void EndEnumerate(Cursor *cursor);

 private:
  static const unsigned kVersion = 0;

  void CopyFrom(const DentryTracker &other);

  void InitLock();
  inline void Lock() const {
    int retval = pthread_mutex_lock(lock_);
    assert(retval == 0);
  }
  inline void Unlock() const {
    int retval = pthread_mutex_unlock(lock_);
    assert(retval == 0);
  }

  void DoPrune(uint64_t now) {
    Entry *entry;
    while (entries_.Peek(&entry)) {
      if (entry->expiry >= now)
        break;
      entries_.PopFront();
      statistics_.num_remove++;
    }
    statistics_.num_prune++;
  }

  pthread_mutex_t *lock_;
  unsigned version_;
  Statistics statistics_;
  bool is_active_;
  BigQueue<Entry> entries_;

  int pipe_terminate_[2];
  int cleaning_interval_ms_;
  pthread_t thread_cleaner_;
};  // class DentryTracker

/**
 * Tracks the content hash associated to inodes of regular files whose content
 * may be in the page cache. It is used in cvmfs_open() and cvmfs_close().
 */
class PageCacheTracker {
 private:
  struct Entry {
    Entry() : nopen(0), idx_stat(-1) {}
    Entry(int32_t n, int32_t i, const shash::Any &h)
      : nopen(n), idx_stat(i), hash(h) {}
    /**
     * Reference counter for currently open files with a given inode. If the
     * sign bit is set, the entry is in the transition phase from one hash to
     * another. The sign will be cleared on Close() in this case.
     */
    int32_t nopen;
    /**
     * Points into the list of stat structs; >= 0 only for open files.
     */
    int32_t idx_stat;
    /**
     * The content hash of the data stored in the page cache. For chunked files,
     * hash contains an artificial hash over all the chunk hash values.
     */
    shash::Any hash;
  };

 public:
  /**
   * In the fuse file handle, use bit 62 to indicate that the file was opened
   * with a direct I/O setting and cvmfs_release() should not call Close().
   * Note that the sign bit (bit 63) indicates chunked files.
   */
  static const unsigned int kBitDirectIo = 62;

  /**
   * Instruct cvmfs_open() on how to handle the page cache.
   */
  struct OpenDirectives {
    /**
     * Flush the page cache; logically, the flush takes place some time between
     * cvmfs_open() and cvmfs_close().  That's important in case we have two
     * open() calls on stale page cache data.
     */
    bool keep_cache;
    /**
     * Don't use the page cache at all (neither write nor read). If this is set
     * on cvmfs_open(), don't call Close() on cvmfs_close().
     * Direct I/O prevents shared mmap on the file. Private mmap, however,
     * which includes loading binaries, still works.
     */
    bool direct_io;

    // Defaults to the old (pre v2.10) behavior: always flush the cache, never
    // use direct I/O.
    OpenDirectives() : keep_cache(false), direct_io(false) {}

    OpenDirectives(bool k, bool d) : keep_cache(k), direct_io(d) {}
  };

  /**
   * To avoid taking the PageCacheTracker mutex multiple times, the
   * fuse forget_multi callback evicts inodes through this RAII object.
   * Copy and assign operator should be deleted but that would require
   * all compilers to use RVO. TODO(jblomer): fix with C++11
   */
  class EvictRaii {
   public:
    explicit EvictRaii(PageCacheTracker *t);
    ~EvictRaii();
    void Evict(uint64_t inode);

   private:
    PageCacheTracker *tracker_;
  };

  // Cannot be moved to the statistics manager because it has to survive
  // reloads.  Added manually in the fuse module initialization and in talk.cc.
  struct Statistics {
    Statistics()
      : n_insert(0)
      , n_remove(0)
      , n_open_direct(0)
      , n_open_flush(0)
      , n_open_cached(0)
    {}
    uint64_t n_insert;
    uint64_t n_remove;
    uint64_t n_open_direct;
    uint64_t n_open_flush;
    uint64_t n_open_cached;
  };
  Statistics GetStatistics() { return statistics_; }

  PageCacheTracker();
  explicit PageCacheTracker(const PageCacheTracker &other);
  PageCacheTracker &operator= (const PageCacheTracker &other);
  ~PageCacheTracker();

  OpenDirectives Open(uint64_t inode, const shash::Any &hash,
                      const struct stat &info);
  /**
   * Forced direct I/O open. Used when the corresponding flag is set in the
   * file catalogs. In this case, we don't need to track the inode.
   */
  OpenDirectives OpenDirect();
  void Close(uint64_t inode);

  bool GetInfoIfOpen(uint64_t inode, shash::Any *hash, struct stat *info)
  {
    MutexLockGuard guard(lock_);
    Entry entry;
    bool retval = map_.Lookup(inode, &entry);
    if (retval && (entry.nopen != 0)) {
      assert(entry.idx_stat >= 0);
      *hash = entry.hash;
      if (info != NULL)
        *info = stat_store_.Get(entry.idx_stat);
      return true;
    }
    return false;
  }

  /**
   * Checks if the dirent's inode is registered in the page cache tracker and
   * if
   *  - it is currently open and has a different content than dirent
   *  - it has been previously found stale (no matter if now open or not)
   */
  bool IsStale(const catalog::DirectoryEntry &dirent) {
    Entry entry;
    const MutexLockGuard guard(lock_);

    const bool retval = map_.Lookup(dirent.inode(), &entry);
    if (!retval)
      return false;
    if (entry.hash.IsNull()) {
      // A previous call to IsStale() returned true (see below)
      return true;
    }
    if (entry.nopen == 0)
      return false;
    if (entry.hash == dirent.checksum())
      return false;

    bool is_stale = true;
    if (dirent.IsChunkedFile()) {
      // Shortcut for chunked files: go by last modified timestamp
      is_stale =
        stat_store_.Get(entry.idx_stat).st_mtime != dirent.mtime();
    }
    if (is_stale) {
      // We mark that inode as "stale" by setting its hash to NULL.
      // When we check next time IsStale(), it is returned stale even
      // if it is not open.
      // The call to GetInfoIfOpen() will from now on return the null hash.
      // That works, the caller will still assume that the version in the
      // page cache tracker is different from any inode in the catalogs.
      entry.hash = shash::Any();
      map_.Insert(dirent.inode(), entry);
    }
    return is_stale;
  }

  EvictRaii GetEvictRaii() { return EvictRaii(this); }

  // Used in RestoreState to prevent using the page cache tracker from a
  // previous version after hotpatch
  void Disable() { is_active_ = false; }

 private:
  static const unsigned kVersion = 0;

  void InitLock();
  void CopyFrom(const PageCacheTracker &other);

  pthread_mutex_t *lock_;
  unsigned version_;
  /**
   * The page cache tracker only works correctly if it is used from the start
   * of the mount. If the instance is hot-patched from a previous version, the
   * page cache tracker remains turned off.
   */
  bool is_active_;
  Statistics statistics_;
  SmallHashDynamic<uint64_t, Entry> map_;
  StatStore stat_store_;
};


}  // namespace glue

#endif  // CVMFS_GLUE_BUFFER_H_
