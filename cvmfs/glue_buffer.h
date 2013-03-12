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

#include "shortstring.h"
#include "atomic.h"

#ifndef CVMFS_GLUE_BUFFER_H_
#define CVMFS_GLUE_BUFFER_H_

class GlueBuffer {
 public:
  GlueBuffer(const unsigned size);
  GlueBuffer(const GlueBuffer &other);
  GlueBuffer &operator= (const GlueBuffer &other);
  ~GlueBuffer();
  void Resize(const unsigned new_size);
  
  inline void Add(const uint64_t inode, const uint64_t parent_inode, 
                  const uint64_t revision, const NameString &name)
  {
    ReadLock();
    
    uint32_t pos = atomic_xadd32(&buffer_pos_, 1) % size_;
    while (!atomic_cas32(&buffer_[pos].busy_flag, 0, 1)) {
      sched_yield();
    }
    buffer_[pos].revision = revision;
    buffer_[pos].inode = inode;
    buffer_[pos].parent_inode = parent_inode;
    buffer_[pos].name = name;
    atomic_dec32(&buffer_[pos].busy_flag);

    Unlock();
  }
  
  bool AncientInode2Path(const uint64_t inode, const uint64_t current_revision,
                         PathString *path);
  
 private:
  static const unsigned kVersion = 1;
  struct BufferEntry {
    BufferEntry() {
      atomic_init32(&busy_flag);
      revision = inode = parent_inode = 0;
    }
    uint64_t revision;
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
  atomic_int32 buffer_pos_;
  unsigned size_;
  unsigned version_;
};

#endif  // CVMFS_GLUE_BUFFER_H_
