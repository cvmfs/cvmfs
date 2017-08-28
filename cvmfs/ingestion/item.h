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
#include "util/single_copy.h"
#include "util_concurrency.h"

class FileItem;

template <class ItemT>
class Tube;

/**
 * A chunk stores the state of compression and hashing contexts as the blocks
 * move through the pipeline.  A chunk can be a "bulk chunk" corresponding to
 * the processed data of an entire file, or it can be a partial chunk of a
 * (large) input file.
 */
class ChunkItem : SingleCopy {
 public:
  ChunkItem(FileItem *file_item, uint64_t offset)
    : file_item_(file_item), offset_(offset) { }

  bool IsValid() { return file_item_ != NULL; }

 private:
  FileItem *file_item_;
  uint64_t offset_;
  zlib::Compressor *compressor_;
  shash::ContextPtr hash_ctx_;
};


class FileItem {
 public:
  explicit FileItem(const std::string &p) : path_(p), bulk_chunk_(this, 0) { }
  std::string path() { return path_; }

 private:
  struct Piece {
    Piece() : offset(0) { }
    shash::Any hash;
    uint64_t offset;
  };

  std::string path_;

  zlib::Algorithms compression_algorithm_;
  shash::Algorithms hash_algorithm_;
  bool may_have_chunks_;
  bool has_legacy_bulk_chunk_;
  ChunkItem bulk_chunk_;
  std::vector<Piece> chunks_;
};


/**
 * A block is an item of work in the pipeline.  A sequence of data blocks
 * followed by a stop block constitutes a Chunk.  A sequence of Chunks in turn
 * build constitute a file.
 */
class BlockItem : SingleCopy {
 public:
  enum BlockType {
    kBlockHollow,
    kBlockData,
    kBlockStop,
  };

  BlockItem();
  ~BlockItem();
  void MakeStop();
  void MakeData(uint32_t capacity, BlockItem *succ_item);
  void MakeData(unsigned char *data, uint32_t size, BlockItem *succ_item);
  // remove pointer to the data
  void Discharge();

  uint32_t Write(void *buf, uint32_t count);
  void Progress(Tube<BlockItem> *next_stage);

  unsigned char *data() { return data_; }
  uint32_t size() { return size_; }

  BlockType type() {
    MutexLockGuard guard(&lock_);
    return type_;
  }

 private:
  // Only called by predecessor
  void Progress(int32_t pred_nstage);
  void DoProgress();

  BlockType type_;

  /**
   * Blocks with the same tag need to be processed sequentially.  That is, no
   * two threads of the same pipeline stage must operate on blocks of the same
   * tag.  The tags roughly correspond to chunks.
   */
  uint64_t tag_;

  unsigned char *data_;
  uint32_t capacity_;
  uint32_t size_;

  BlockItem *succ_item_;
  int32_t pred_nstage_;
  Tube<BlockItem> *next_stage_;

  pthread_mutex_t lock_;
};


#endif  // CVMFS_INGESTION_ITEM_H_
