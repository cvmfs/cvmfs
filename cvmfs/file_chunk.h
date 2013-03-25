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

#include <vector>
#include <string>

#include "hash.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * Describes a FileChunk as generated from the FileProcessor in collaboration
 * with the ChunkGenerator.
 */
class FileChunk {
 public:
  static const std::string kCasSuffix;

 public:
  FileChunk() : content_hash_(hash::Any(hash::kSha1)), offset_(0), size_(0) { }
  FileChunk(const hash::Any &hash,
            const off_t      offset,
            const size_t     size) :
    content_hash_(hash),
    offset_(offset),
    size_(size) { }

  inline const hash::Any& content_hash() const { return content_hash_; }
  inline off_t            offset()       const { return offset_; }
  inline size_t           size()         const { return size_; }

 protected:
  hash::Any content_hash_; //!< content hash of the compressed file chunk
  off_t     offset_;       //!< byte offset in the uncompressed input file
  size_t    size_;         //!< uncompressed size of the data chunk
};
typedef std::vector<FileChunk> FileChunks;

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#endif  // CVMFS_FILE_CHUNK_H_
