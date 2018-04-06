/**
 * This file is part of the CernVM File System.
 */

#include "item.h"

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>

#include "ingestion/ingestion_source.h"
#include "smalloc.h"
#include "sync_item.h"
#include "util/shared_ptr.h"
#include "util_concurrency.h"

FileItem::FileItem(
  IngestionSource* source,
  uint64_t min_chunk_size,
  uint64_t avg_chunk_size,
  uint64_t max_chunk_size,
  zlib::Algorithms compression_algorithm,
  shash::Algorithms hash_algorithm,
  shash::Suffix hash_suffix,
  bool may_have_chunks,
  bool has_legacy_bulk_chunk)
  : path_(source->GetPath())
  , source_(source)
  , compression_algorithm_(compression_algorithm)
  , hash_algorithm_(hash_algorithm)
  , hash_suffix_(hash_suffix)
  , has_legacy_bulk_chunk_(has_legacy_bulk_chunk)
  , size_(kSizeUnknown)
  , may_have_chunks_(may_have_chunks)
  , chunk_detector_(min_chunk_size, avg_chunk_size, max_chunk_size)
  , bulk_hash_(hash_algorithm)
  , chunks_(1)
{
  int retval = pthread_mutex_init(&lock_, NULL);
  assert(retval == 0);
  atomic_init64(&nchunks_in_fly_);
  atomic_init32(&is_fully_chunked_);
}

FileItem::~FileItem() {
  pthread_mutex_destroy(&lock_);
  delete source_;
}

void FileItem::RegisterChunk(const FileChunk &file_chunk) {
  MutexLockGuard lock_guard(lock_);

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
  , compressor_(zlib::Compressor::Construct(file_item->compression_algorithm()))
{
  hash_ctx_.algorithm = file_item->hash_algorithm();
  hash_ctx_.size = shash::GetContextSize(hash_ctx_.algorithm);
  hash_ctx_buffer_ = smalloc(hash_ctx_.size);
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


//------------------------------------------------------------------------------

atomic_int64 BlockItem::managed_bytes_ = 0;


BlockItem::BlockItem()
  : type_(kBlockHollow)
  , tag_(-1)
  , file_item_(NULL)
  , chunk_item_(NULL)
  , data_(NULL)
  , capacity_(0)
  , size_(0)
{ }


BlockItem::BlockItem(int64_t tag)
  : type_(kBlockHollow)
  , tag_(tag)
  , file_item_(NULL)
  , chunk_item_(NULL)
  , data_(NULL)
  , capacity_(0)
  , size_(0)
{
  assert(tag_ >= 0);
}


void BlockItem::Discharge() {
  atomic_xadd64(&managed_bytes_, -static_cast<int64_t>(capacity_));
  data_.Release();
  size_ = capacity_ = 0;
}


void BlockItem::MakeStop() {
  assert(type_ == kBlockHollow);
  type_ = kBlockStop;
}


void BlockItem::MakeData(uint32_t capacity) {
  assert(type_ == kBlockHollow);
  assert(capacity > 0);

  type_ = kBlockData;
  capacity_ = capacity;
  data_ = reinterpret_cast<unsigned char *>(smalloc(capacity_));
  atomic_xadd64(&managed_bytes_, static_cast<int64_t>(capacity_));
}


/**
 * Move data from one block to another.
 */
void BlockItem::MakeData(
  unsigned char *data,
  uint32_t size)
{
  assert(type_ == kBlockHollow);
  assert(size > 0);

  type_ = kBlockData;
  capacity_ = size_ = size;
  data_ = data;
  atomic_xadd64(&managed_bytes_, static_cast<int64_t>(capacity_));
}


/**
 * Copy a piece of one block's data into a new block.
 */
void BlockItem::MakeDataCopy(
  unsigned char *data,
  uint32_t size)
{
  assert(type_ == kBlockHollow);
  assert(size > 0);

  type_ = kBlockData;
  capacity_ = size_ = size;
  data_ = reinterpret_cast<unsigned char *>(smalloc(capacity_));
  memcpy(data_, data, size);
  atomic_xadd64(&managed_bytes_, static_cast<int64_t>(capacity_));
}


void BlockItem::Reset() {
  assert(type_ == kBlockData);

  atomic_xadd64(&managed_bytes_, -static_cast<int64_t>(capacity_));
  data_.Destroy();
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

  uint32_t remaining = capacity_ - size_;
  uint32_t nbytes = std::min(remaining, count);
  memcpy(data_.weak_ref() + size_, buf, nbytes);
  size_ += nbytes;
  return nbytes;
}
