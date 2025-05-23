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

#include "compression/compression.h"
#include "crypto/hash.h"
#include "file_chunk.h"
#include "ingestion/chunk_detector.h"
#include "ingestion/ingestion_source.h"
#include "util/atomic.h"
#include "util/pointer.h"
#include "util/single_copy.h"

namespace upload {
struct UploadStreamHandle;
}

class ItemAllocator;

/**
 * Carries the information necessary to compress and checksum a file. During
 * processing, the bulk chunk and the chunks_ vector are filled.
 */
class FileItem : SingleCopy {
 public:
  explicit FileItem(IngestionSource *source,
                    uint64_t min_chunk_size = 4 * 1024 * 1024,
                    uint64_t avg_chunk_size = 8 * 1024 * 1024,
                    uint64_t max_chunk_size = 16 * 1024 * 1024,
                    zlib::Algorithms compression_algorithm = zlib::kZlibDefault,
                    shash::Algorithms hash_algorithm = shash::kSha1,
                    shash::Suffix hash_suffix = shash::kSuffixNone,
                    bool may_have_chunks = true,
                    bool has_legacy_bulk_chunk = false);
  ~FileItem();

  static FileItem *CreateQuitBeacon() {
    std::string quit_marker = std::string(1, kQuitBeaconMarker);
    UniquePtr<FileIngestionSource> source(new FileIngestionSource(quit_marker));
    return new FileItem(source.Release());
  }
  bool IsQuitBeacon() {
    return (path().length() == 1) && (path()[0] == kQuitBeaconMarker);
  }

  std::string path() { return source_->GetPath(); }
  uint64_t size() { return size_; }
  Xor32Detector *chunk_detector() { return &chunk_detector_; }
  shash::Any bulk_hash() { return bulk_hash_; }
  zlib::Algorithms compression_algorithm() { return compression_algorithm_; }
  shash::Algorithms hash_algorithm() { return hash_algorithm_; }
  shash::Suffix hash_suffix() { return hash_suffix_; }
  bool may_have_chunks() { return may_have_chunks_; }
  bool has_legacy_bulk_chunk() { return has_legacy_bulk_chunk_; }

  void set_size(uint64_t val) { size_ = val; }
  void set_may_have_chunks(bool val) { may_have_chunks_ = val; }
  void set_is_fully_chunked() { atomic_inc32(&is_fully_chunked_); }
  bool is_fully_chunked() { return atomic_read32(&is_fully_chunked_) != 0; }
  uint64_t nchunks_in_fly() { return atomic_read64(&nchunks_in_fly_); }

  uint64_t GetNumChunks() { return chunks_.size(); }
  FileChunkList *GetChunksPtr() { return &chunks_; }

  bool Open() { return source_->Open(); }
  ssize_t Read(void *buffer, size_t nbyte) {
    return source_->Read(buffer, nbyte);
  }
  bool Close() { return source_->Close(); }
  bool GetSize(uint64_t *size) { return source_->GetSize(size); }

  // Called by ChunkItem constructor, decremented when a chunk is registered
  void IncNchunksInFly() { atomic_inc64(&nchunks_in_fly_); }
  void RegisterChunk(const FileChunk &file_chunk);
  bool IsProcessed() {
    return is_fully_chunked() && (atomic_read64(&nchunks_in_fly_) == 0);
  }

 private:
  static const uint64_t kSizeUnknown = uint64_t(-1);
  static const char kQuitBeaconMarker = '\0';

  UniquePtr<IngestionSource> source_;
  const zlib::Algorithms compression_algorithm_;
  const shash::Algorithms hash_algorithm_;
  const shash::Suffix hash_suffix_;
  const bool has_legacy_bulk_chunk_;

  uint64_t size_;
  bool may_have_chunks_;

  Xor32Detector chunk_detector_;
  shash::Any bulk_hash_;
  FileChunkList chunks_;
  /**
   * Number of chunks created but not yet uploaded and registered
   */
  atomic_int64 nchunks_in_fly_;
  /**
   * Switches to true once all of the file has been through the chunking
   * stage
   */
  atomic_int32 is_fully_chunked_;
  pthread_mutex_t lock_;
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

  void MakeBulkChunk();
  bool IsSolePiece() {
    return !is_bulk_chunk_ && (offset_ == 0) && (size_ == file_item_->size());
  }

  bool is_bulk_chunk() { return is_bulk_chunk_; }
  FileItem *file_item() { return file_item_; }
  uint64_t offset() { return offset_; }
  uint64_t size() { return size_; }
  upload::UploadStreamHandle *upload_handle() { return upload_handle_; }
  // An active zlib compression stream requires 256kB of memory.  Therefore,
  // we create it only for the absolutely necessary duration and free the space
  // afterwards.
  zlib::Compressor *GetCompressor();
  void ReleaseCompressor();

  shash::ContextPtr hash_ctx() { return hash_ctx_; }
  shash::Any *hash_ptr() { return &hash_value_; }

  void set_size(uint64_t val) {
    assert(size_ == 0);
    size_ = val;
  }
  void set_upload_handle(upload::UploadStreamHandle *val) {
    assert((upload_handle_ == NULL) && (val != NULL));
    upload_handle_ = val;
  }

 private:
  FileItem *file_item_;
  uint64_t offset_;
  /**
   * The size of a chunk is not defined before the corresponding stop block
   * has been dispatched.
   */
  uint64_t size_;
  bool is_bulk_chunk_;
  /**
   * Deleted by the uploader.
   */
  upload::UploadStreamHandle *upload_handle_;
  UniquePtr<zlib::Compressor> compressor_;
  shash::ContextPtr hash_ctx_;
  shash::Any hash_value_;
  unsigned char hash_ctx_buffer_[shash::kMaxContextSize];
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

  explicit BlockItem(ItemAllocator *allocator);
  BlockItem(int64_t tag, ItemAllocator *allocator);
  ~BlockItem();

  static BlockItem *CreateQuitBeacon() { return new BlockItem(NULL); }
  bool IsQuitBeacon() { return type_ == kBlockHollow; }

  void MakeStop();
  void MakeData(uint32_t capacity);
  void MakeDataMove(BlockItem *other);
  void MakeDataCopy(const unsigned char *data, uint32_t size);
  void SetFileItem(FileItem *item);
  void SetChunkItem(ChunkItem *item);
  // Free data and reset to hollow block
  void Reset();

  uint32_t Write(void *buf, uint32_t count);

  bool IsEmpty() { return size_ == 0; }
  bool IsFull() { return size_ == capacity_; }

  unsigned char *data() { return data_; }
  uint32_t capacity() { return capacity_; }
  uint32_t size() { return size_; }
  void set_size(uint32_t val) {
    assert(val <= capacity_);
    size_ = val;
  }

  BlockType type() { return type_; }
  int64_t tag() { return tag_; }
  FileItem *file_item() { return file_item_; }
  ChunkItem *chunk_item() { return chunk_item_; }
  static uint64_t managed_bytes() { return atomic_read64(&managed_bytes_); }

 private:
  /**
   * Total capacity of all BlockItem()
   */
  static atomic_int64 managed_bytes_;

  // Forget pointer to the data
  void Discharge();

  ItemAllocator *allocator_;
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

  /**
   * Managed by ItemAllocator
   */
  unsigned char *data_;
  uint32_t capacity_;
  uint32_t size_;
};

#endif  // CVMFS_INGESTION_ITEM_H_
