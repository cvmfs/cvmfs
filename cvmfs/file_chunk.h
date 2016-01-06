/**
 * This file is part of the CernVM File System.
 *
 * This class implements a data wrapper for single dentries in CVMFS
 * Additionally to the normal file meta data it manages some
 * bookkeeping data like the associated catalog.
 */

#ifndef CVMFS_FILE_CHUNK_H_
#define CVMFS_FILE_CHUNK_H_

#include <pthread.h>
#include <stdint.h>
#include <sys/types.h>

#include <string>
#include <vector>

#include "atomic.h"
#include "bigvector.h"
#include "compression.h"
#include "hash.h"
#include "shortstring.h"
#include "smallhash.h"
#include "util.h"

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
    size_(size) { }

  inline const shash::Any& content_hash() const { return content_hash_; }
  inline off_t             offset()       const { return offset_; }
  inline size_t            size()         const { return size_; }

 protected:
  shash::Any content_hash_;  //!< content hash of the compressed file chunk
  off_t      offset_;        //!< byte offset in the uncompressed input file
  size_t     size_;          //!< uncompressed size of the data chunk
};

typedef BigVector<FileChunk> FileChunkList;

struct FileChunkReflist {
  FileChunkReflist() : list(NULL) { }
  FileChunkReflist(
    FileChunkList *l,
    const PathString &p,
    zlib::Algorithms alg,
    bool external)
    : list(l)
    , path(p)
    , compression_alg(alg)
    , external_data(external) { }

  unsigned FindChunkIdx(const uint64_t offset);

  FileChunkList *list;
  PathString path;
  zlib::Algorithms compression_alg;
  bool external_data;
};


/**
 * Stores the chunk index of a file descriptor.  Needed for the Fuse module
 * and for libcvmfs.
 */
struct ChunkFd {
  ChunkFd() : fd(-1), chunk_idx(0) { }
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

  static const unsigned kVersion = 2;

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


/**
 * Connects virtual file descriptors to FileChunkLists.  Used by libcvmfs.
 * Tries to keep the file descriptors small because they need to fit within
 * 29bit.  This class takes the ownership of the FileChunkList objects pointed
 * to by the elements of fd_table_.
 */
class SimpleChunkTables : SingleCopy {
 public:
  /**
   * While a chunked file is open, a single file descriptor is moved around the
   * individual chunks.
   */
  struct OpenChunks {
    OpenChunks() : chunk_fd(NULL) { }
    ChunkFd *chunk_fd;
    FileChunkReflist chunk_reflist;
  };

  SimpleChunkTables();
  ~SimpleChunkTables();
  int Add(FileChunkReflist chunks);
  OpenChunks Get(int fd);
  void Release(int fd);

 private:
  inline void Lock() {
    int retval = pthread_mutex_lock(lock_);
    assert(retval == 0);
  }

  inline void Unlock() {
    int retval = pthread_mutex_unlock(lock_);
    assert(retval == 0);
  }

  std::vector<OpenChunks> fd_table_;
  pthread_mutex_t *lock_;
};

#endif  // CVMFS_FILE_CHUNK_H_
