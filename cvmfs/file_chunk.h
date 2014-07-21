/**
 * This file is part of the CernVM File System.
 *
 * This class implements a data wrapper for single dentries in CVMFS
 * Additionally to the normal file meta data it manages some
 * bookkeeping data like the associated catalog.
 */

#ifndef CVMFS_FILE_CHUNK_H_
#define CVMFS_FILE_CHUNK_H_

#include <sys/types.h>
#include <pthread.h>
#include <stdint.h>

#include <vector>
#include <string>

#include "hash.h"
#include "bigvector.h"
#include "smallhash.h"
#include "shortstring.h"
#include "atomic.h"

/**
 * Describes a FileChunk as generated from the FileProcessor in collaboration
 * with the ChunkGenerator.
 */
class FileChunk {
 public:
  FileChunk() : content_hash_(shash::Any(shash::kAny)), offset_(0), size_(0) { }
  FileChunk(const shash::Any &hash,
            const off_t       offset,
            const size_t      size) :
    content_hash_(hash),
    offset_(offset),
    size_(size) {}

  inline const shash::Any& content_hash() const { return content_hash_; }
  inline off_t             offset()       const { return offset_; }
  inline size_t            size()         const { return size_; }

 protected:
  shash::Any content_hash_; //!< content hash of the compressed file chunk
  off_t      offset_;       //!< byte offset in the uncompressed input file
  size_t     size_;         //!< uncompressed size of the data chunk
};

typedef BigVector<FileChunk> FileChunkList;

struct FileChunkReflist {
  FileChunkReflist() {
    list = NULL;
  }
  FileChunkReflist(FileChunkList *l, const PathString &p) :
    list(l), path(p) {}
  FileChunkList *list;
  PathString path;
};


/**
 * Stores the chunk index of a file descriptor.  Needed for the Fuse module
 */
struct ChunkFd {
  ChunkFd() {
    fd = -1;
    chunk_idx = 0;
  }
  int fd;  // -1 or pointing to chunk_idx
  unsigned chunk_idx;
};


/**
 * All chunk related data structures in the Fuse module.
 */
struct ChunkTables {
  ChunkTables();
  ~ChunkTables();
  ChunkTables(const ChunkTables &other);
  ChunkTables &operator= (const ChunkTables &other);
  void CopyFrom(const ChunkTables &other);
  void InitLocks();
  void InitHashmaps();

  pthread_mutex_t *Handle2Lock(const uint64_t handle) const;

  inline void Lock() {
    int retval = pthread_mutex_lock(lock);
    assert(retval == 0);
  }

  inline void Unlock() {
    int retval = pthread_mutex_unlock(lock);
    assert(retval == 0);
  }

  int version;
  static const unsigned kNumHandleLocks = 128;
  SmallHashDynamic<uint64_t, ChunkFd> handle2fd;
  // The file descriptors attached to handles need to be locked.
  // Using a hash map to survive with a small, fixed number of locks
  BigVector<pthread_mutex_t *> handle_locks;
  SmallHashDynamic<uint64_t, FileChunkReflist> inode2chunks;
  SmallHashDynamic<uint64_t, uint32_t> inode2references;
  uint64_t next_handle;
  pthread_mutex_t *lock;
};

#endif  // CVMFS_FILE_CHUNK_H_
