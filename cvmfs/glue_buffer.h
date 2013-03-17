/**
 * This file is part of the CernVM File System.
 *
 * The snapshot glue buffer is a data structure providing fast writes and
 * slow reads.  It is used to connect to consequtively loaded file system
 * snapshots: the previous snapshots' inodes are stored in the glue buffer
 * and a full path can be constructed from the data in the glue buffer.  This
 * full path can be looked up in the new file system snapshot.
 *
 * Note: the read-write lock has reversed meaning.  Many concurrent writers are
 * allowed but only serialized reading is permitted.
 */

#include <stdint.h>
#include <pthread.h>
#include <sched.h>

#include <cassert>
#include <string>
#include <map>
#include <vector>

#include "shortstring.h"
#include "atomic.h"
#include "dirent.h"
#include "catalog_mgr.h"

#ifndef CVMFS_GLUE_BUFFER_H_
#define CVMFS_GLUE_BUFFER_H_

class GlueBuffer {
 public:
  struct Statistics {
    Statistics() {
      atomic_init64(&num_ancient_hits);
      atomic_init64(&num_ancient_misses);
      atomic_init64(&num_busywait_cycles);
    }
    std::string Print() {
      return 
        "ancient(hits): " + StringifyInt(atomic_read64(&num_ancient_hits)) +
        "  ancient(misses): " + StringifyInt(atomic_read64(&num_ancient_misses)) +
        "  busy-wait-cycles: " + StringifyInt(atomic_read64(&num_busywait_cycles));
    }
    atomic_int64 num_ancient_hits;
    atomic_int64 num_ancient_misses;
    atomic_int64 num_busywait_cycles;
  };
  uint64_t GetNumInserts() { return atomic_read64(&buffer_pos_); }
  unsigned GetNumEntries() { return size_; }
  unsigned GetNumBytes() { return size_*sizeof(BufferEntry); }
  Statistics GetStatistics() { return statistics_; }
  
  GlueBuffer(const unsigned size);
  GlueBuffer(const GlueBuffer &other);
  GlueBuffer &operator= (const GlueBuffer &other);
  ~GlueBuffer();
  void Resize(const unsigned new_size);
  
  inline void Add(const uint64_t inode, const uint64_t parent_inode, 
                  const uint32_t generation, const NameString &name)
  {
    //assert((parent_inode == 0) || ((parent_inode & ((1<<26)-1)) != 0));
    ReadLock();
    
    uint32_t pos = atomic_xadd64(&buffer_pos_, 1) % size_;
    while (!atomic_cas32(&buffer_[pos].busy_flag, 0, 1)) {
      atomic_inc64(&statistics_.num_busywait_cycles);
      sched_yield();
    }
    buffer_[pos].generation = generation;
    buffer_[pos].inode = inode;
    buffer_[pos].parent_inode = parent_inode;
    buffer_[pos].name = name;
    atomic_dec32(&buffer_[pos].busy_flag);

    Unlock();
  }
  
  inline void AddDirent(const catalog::DirectoryEntry &dirent) {
    Add(dirent.inode(), dirent.parent_inode(), dirent.generation(), 
        dirent.name());
  }
  
  bool AncientInode2Path(const uint64_t inode, 
                         const uint32_t current_generation,
                         PathString *path);
  
 private:
  static const unsigned kVersion = 1;
  struct BufferEntry {
    BufferEntry() {
      atomic_init32(&busy_flag);
      generation = 0;
      inode = parent_inode = 0;
    }
    uint32_t generation;
    uint64_t inode;
    uint64_t parent_inode;
    NameString name;
    atomic_int32 busy_flag;
  };
  
  void InitLock();
  void CopyFrom(const GlueBuffer &other);
  inline void ReadLock() const {
    int retval = pthread_rwlock_rdlock(rwlock_);
    assert(retval == 0);
  }
  inline void WriteLock() const {
    int retval = pthread_rwlock_wrlock(rwlock_);
    assert(retval == 0);
  }
  inline void Unlock() const {
    int retval = pthread_rwlock_unlock(rwlock_);
    assert(retval == 0);
  }
  bool ConstructPath(const unsigned buffer_idx, PathString *path);
  
  pthread_rwlock_t *rwlock_;
  BufferEntry *buffer_;
  atomic_int64 buffer_pos_;
  unsigned size_;
  unsigned version_;
  Statistics statistics_;
};


/**
 * Saves the inodes of current working directories on this Fuse volume.
 * Required for catalog reloads and reloads of the Fuse module.
 */
class CwdBuffer : public catalog::RemountListener {
 public:
  struct Statistics {
    Statistics() {
      atomic_init64(&num_inserts);
      atomic_init64(&num_removes);
      atomic_init64(&num_ancient_hits);
      atomic_init64(&num_ancient_misses);
    }
    std::string Print() {
      return 
      "inserts: " + StringifyInt(atomic_read64(&num_inserts)) +
      "  removes: " + StringifyInt(atomic_read64(&num_removes)) +
      "  ancient(hits): " + StringifyInt(atomic_read64(&num_ancient_hits)) +
      "  ancient(misses): " + StringifyInt(atomic_read64(&num_ancient_misses));
    }
    atomic_int64 num_inserts;
    atomic_int64 num_removes;
    atomic_int64 num_ancient_hits;
    atomic_int64 num_ancient_misses;
  };
  Statistics GetStatistics() { return statistics_; }
  
  explicit CwdBuffer(const std::string mountpoint);
  explicit CwdBuffer(const CwdBuffer &other);
  CwdBuffer &operator= (const CwdBuffer &other);
  ~CwdBuffer();
  
  std::vector<PathString> GatherCwds();
  void Add(const uint64_t inode, const PathString &path);
  void Remove(const uint64_t);
  bool Find(const uint64_t inode, PathString *path);
  
  void BeforeRemount(catalog::AbstractCatalogManager *source);
 
 private: 
  static const unsigned kVersion = 1;
  void InitLock();
  void CopyFrom(const CwdBuffer &other);
  inline void Lock() const {
    int retval = pthread_mutex_lock(lock_);
    assert(retval == 0);
  }
  inline void Unlock() const {
    int retval = pthread_mutex_unlock(lock_);
    assert(retval == 0);
  }
  
  pthread_mutex_t *lock_;
  unsigned version_; 
  std::map<uint64_t, PathString> inode2cwd_;
  std::string mountpoint_;
  Statistics statistics_;
};

#endif  // CVMFS_GLUE_BUFFER_H_
