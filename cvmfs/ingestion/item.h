/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_ITEM_H_
#define CVMFS_INGESTION_ITEM_H_

#include <pthread.h>
#include <stdint.h>

#include <cassert>
#include <string>
#include <vector>

#include "compression.h"
#include "hash.h"
#include "ingestion/chunk_detector.h"
#include "util/pointer.h"
#include "util/single_copy.h"

class FileItem;


/**
 * Carries the information necessary to compress and checksum a file. During
 * processing, the bulk chunk and the chunks_ vector are filled.
 */
class FileItem : SingleCopy {
 public:
  explicit FileItem(
    const std::string &p,
    uint64_t min_chunk_size = 4 * 1024 * 1024,
    uint64_t avg_chunk_size = 8 * 1024 * 1024,
    uint64_t max_chunk_size = 16 * 1024 * 1024,
    zlib::Algorithms compression_algorithm = zlib::kZlibDefault,
    shash::Algorithms hash_algorithm = shash::kSha1,
    shash::Suffix hash_suffix = shash::kSuffixNone,
    bool may_have_chunks = true,
    bool has_legacy_bulk_chunk = false
   );
  ~FileItem();

  std::string path() { return path_; }
  Xor32Detector *chunk_detector() { return &chunk_detector_; }
  zlib::Algorithms compression_algorithm() { return compression_algorithm_; }
  shash::Algorithms hash_algorithm() { return hash_algorithm_; }
  shash::Suffix hash_suffix() { return hash_suffix_; }
  bool may_have_chunks() { return may_have_chunks_; }
  bool has_legacy_bulk_chunk() { return has_legacy_bulk_chunk_; }

 private:
  struct Piece {
    Piece() : offset(0) { }
    shash::Any hash;
    uint64_t offset;
  };

  const std::string path_;
  const zlib::Algorithms compression_algorithm_;
  const shash::Algorithms hash_algorithm_;
  const shash::Suffix hash_suffix_;
  const bool may_have_chunks_;
  const bool has_legacy_bulk_chunk_;

  Xor32Detector chunk_detector_;
  Piece bulk_chunk_;
  std::vector<Piece> chunks_;
  pthread_mutex_t *lock_;
};


/**
 * A chunk stores the state of compression and hashing contexts as the blocks
 * move through the pipeline.  A chunk can be a "bulk chunk" corresponding to
 * the processed data of an entire file, or it can be a partial chunk of a
 * (large) input file.
 */
class ChunkItem : SingleCopy {
 public:
  ChunkItem(FileItem *file_item, uint64_t offset);

  bool is_bulk_chunk() { return is_bulk_chunk_; }
  void MakeBulkChunk() { is_bulk_chunk_ = true; }

 private:
  FileItem *file_item_;
  uint64_t offset_;
  bool is_bulk_chunk_;
  UniquePtr<zlib::Compressor> compressor_;
  shash::ContextPtr hash_ctx_;
  UniquePtr<void> hash_ctx_buffer_;
};


/**
 * A block is an item of work in the pipeline.  A sequence of data blocks
 * followed by a stop block constitutes a Chunk.  A sequence of Chunks in turn
 * build constitute a file.
 * A block that carries data must have a non-zero-length payload.
 */
class BlockItem : SingleCopy {
 public:
  enum BlockType {
    kBlockHollow,
    kBlockData,
    kBlockStop,
  };

  BlockItem();
  explicit BlockItem(int64_t tag);

  void MakeStop();
  void MakeData(uint32_t capacity);
  void MakeData(unsigned char *data, uint32_t size);
  void MakeDataCopy(unsigned char *data, uint32_t size);
  void SetFileItem(FileItem *item);
  void SetChunkItem(ChunkItem *item);
  // Forget pointer to the data
  void Discharge();
  // Free data and reset to hollow block
  void Reset();

  uint32_t Write(void *buf, uint32_t count);

  unsigned char *data() { return data_.weak_ref(); }
  uint32_t capacity() { return capacity_; }
  uint32_t size() { return size_; }
  void set_size(uint32_t val) { assert(val <= capacity_); size_ = val; }

  BlockType type() { return type_; }
  int64_t tag() { return tag_; }
  FileItem *file_item() { return file_item_; }
  ChunkItem *chunk_item() { return chunk_item_; }

 private:
  BlockType type_;

  /**
   * Blocks with the same tag need to be processed sequentially.  That is, no
   * two threads of the same pipeline stage must operate on blocks of the same
   * tag.  The tags roughly correspond to chunks.
   * Tags can (and should) be set exactly once in the life time of a block.
   */
  int64_t tag_;

  /**
   * Can be set exactly once.
   */
  FileItem *file_item_;
  ChunkItem *chunk_item_;

  UniquePtr<unsigned char> data_;
  uint32_t capacity_;
  uint32_t size_;
};


#endif  // CVMFS_INGESTION_ITEM_H_
