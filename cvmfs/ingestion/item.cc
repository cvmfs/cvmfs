/**
 * This file is part of the CernVM File System.
 */

#include "item.h"

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>

#include "ingestion/ingestion_source.h"
#include "item_mem.h"
#include "util/concurrency.h"
#include "util/smalloc.h"

FileItem::FileItem(IngestionSource *source,
                   uint64_t min_chunk_size,
                   uint64_t avg_chunk_size,
                   uint64_t max_chunk_size,
                   zlib::Algorithms compression_algorithm,
                   shash::Algorithms hash_algorithm,
                   shash::Suffix hash_suffix,
                   bool may_have_chunks,
                   bool has_legacy_bulk_chunk)
    : source_(source)
    , compression_algorithm_(compression_algorithm)
    , hash_algorithm_(hash_algorithm)
    , hash_suffix_(hash_suffix)
    , has_legacy_bulk_chunk_(has_legacy_bulk_chunk)
    , size_(kSizeUnknown)
    , may_have_chunks_(may_have_chunks)
    , chunk_detector_(min_chunk_size, avg_chunk_size, max_chunk_size)
    , bulk_hash_(hash_algorithm)
    , chunks_(1) {
  const int retval = pthread_mutex_init(&lock_, NULL);
  assert(retval == 0);
  atomic_init64(&nchunks_in_fly_);
  atomic_init32(&is_fully_chunked_);
}

FileItem::~FileItem() { pthread_mutex_destroy(&lock_); }

void FileItem::RegisterChunk(const FileChunk &file_chunk) {
  const MutexLockGuard lock_guard(lock_);

  switch (file_chunk.content_hash().suffix) {
    case shash::kSuffixPartial:
      chunks_.PushBack(file_chunk);
      break;

    default:
      assert(file_chunk.offset() == 0);
      assert(file_chunk.size() == size_);
      bulk_hash_ = file_chunk.content_hash();
      break;
  }
  atomic_dec64(&nchunks_in_fly_);
}


//------------------------------------------------------------------------------


ChunkItem::ChunkItem(FileItem *file_item, uint64_t offset)
    : file_item_(file_item)
    , offset_(offset)
    , size_(0)
    , is_bulk_chunk_(false)
    , upload_handle_(NULL)
    , compressor_(NULL) {
  hash_ctx_.algorithm = file_item->hash_algorithm();
  hash_ctx_.size = shash::GetContextSize(hash_ctx_.algorithm);
  hash_ctx_.buffer = hash_ctx_buffer_;
  shash::Init(hash_ctx_);
  hash_value_.algorithm = hash_ctx_.algorithm;
  hash_value_.suffix = shash::kSuffixPartial;
  file_item_->IncNchunksInFly();
}


void ChunkItem::MakeBulkChunk() {
  is_bulk_chunk_ = true;
  hash_value_.suffix = file_item_->hash_suffix();
}


zlib::Compressor *ChunkItem::GetCompressor() {
  if (!compressor_.IsValid()) {
    compressor_ = zlib::Compressor::Construct(
        file_item_->compression_algorithm());
  }
  return compressor_.weak_ref();
}


void ChunkItem::ReleaseCompressor() { compressor_.Destroy(); }


//------------------------------------------------------------------------------

atomic_int64 BlockItem::managed_bytes_ = 0;


BlockItem::BlockItem(ItemAllocator *allocator)
    : allocator_(allocator)
    , type_(kBlockHollow)
    , tag_(-1)
    , file_item_(NULL)
    , chunk_item_(NULL)
    , data_(NULL)
    , capacity_(0)
    , size_(0) { }


BlockItem::BlockItem(int64_t tag, ItemAllocator *allocator)
    : allocator_(allocator)
    , type_(kBlockHollow)
    , tag_(tag)
    , file_item_(NULL)
    , chunk_item_(NULL)
    , data_(NULL)
    , capacity_(0)
    , size_(0) {
  assert(tag_ >= 0);
}


BlockItem::~BlockItem() {
  if (data_)
    allocator_->Free(data_);
  atomic_xadd64(&managed_bytes_, -static_cast<int64_t>(capacity_));
}


void BlockItem::Discharge() {
  data_ = NULL;
  size_ = capacity_ = 0;
}


void BlockItem::MakeStop() {
  assert(type_ == kBlockHollow);
  type_ = kBlockStop;
}


void BlockItem::MakeData(uint32_t capacity) {
  assert(type_ == kBlockHollow);
  assert(allocator_ != NULL);
  assert(capacity > 0);

  type_ = kBlockData;
  capacity_ = capacity;
  data_ = reinterpret_cast<unsigned char *>(allocator_->Malloc(capacity_));
  atomic_xadd64(&managed_bytes_, static_cast<int64_t>(capacity_));
}


/**
 * Move data from one block to another.
 */
void BlockItem::MakeDataMove(BlockItem *other) {
  assert(type_ == kBlockHollow);
  assert(other->type_ == kBlockData);
  assert(other->size_ > 0);

  type_ = kBlockData;
  capacity_ = size_ = other->size_;
  data_ = other->data_;
  allocator_ = other->allocator_;

  other->Discharge();
}


/**
 * Copy a piece of one block's data into a new block.
 */
void BlockItem::MakeDataCopy(const unsigned char *data, uint32_t size) {
  assert(type_ == kBlockHollow);
  assert(allocator_ != NULL);
  assert(size > 0);

  type_ = kBlockData;
  capacity_ = size_ = size;
  data_ = reinterpret_cast<unsigned char *>(allocator_->Malloc(capacity_));
  memcpy(data_, data, size);
  atomic_xadd64(&managed_bytes_, static_cast<int64_t>(capacity_));
}


void BlockItem::Reset() {
  assert(type_ == kBlockData);

  atomic_xadd64(&managed_bytes_, -static_cast<int64_t>(capacity_));
  allocator_->Free(data_);
  data_ = NULL;
  size_ = capacity_ = 0;
  type_ = kBlockHollow;
}


void BlockItem::SetChunkItem(ChunkItem *value) {
  assert(value != NULL);
  assert(chunk_item_ == NULL);
  chunk_item_ = value;
}


void BlockItem::SetFileItem(FileItem *value) {
  assert(value != NULL);
  assert(file_item_ == NULL);
  file_item_ = value;
}


uint32_t BlockItem::Write(void *buf, uint32_t count) {
  assert(type_ == kBlockData);

  const uint32_t remaining = capacity_ - size_;
  const uint32_t nbytes = std::min(remaining, count);
  memcpy(data_ + size_, buf, nbytes);
  size_ += nbytes;
  return nbytes;
}
