/**
 * This file is part of the CernVM File System.
 */

#include "item.h"

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>

#include "smalloc.h"
#include "util_concurrency.h"


FileItem::FileItem(
  const std::string &p,
  uint64_t min_chunk_size,
  uint64_t avg_chunk_size,
  uint64_t max_chunk_size,
  zlib::Algorithms compression_algorithm,
  shash::Algorithms hash_algorithm,
  shash::Suffix hash_suffix,
  bool may_have_chunks,
  bool has_legacy_bulk_chunk)
  : path_(p)
  , compression_algorithm_(compression_algorithm)
  , hash_algorithm_(hash_algorithm)
  , hash_suffix_(hash_suffix)
  , may_have_chunks_(may_have_chunks)
  , has_legacy_bulk_chunk_(has_legacy_bulk_chunk)
  , chunk_detector_(min_chunk_size, avg_chunk_size, max_chunk_size)
{
  lock_ = reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_, NULL);
  assert(retval == 0);
}


FileItem::~FileItem() {
  pthread_mutex_destroy(lock_);
  free(lock_);
}


//------------------------------------------------------------------------------


ChunkItem::ChunkItem(FileItem *file_item, uint64_t offset)
  : file_item_(file_item)
  , offset_(offset)
  , compressor_(zlib::Compressor::Construct(file_item->compression_algorithm()))
{
  hash_ctx_.algorithm = file_item->hash_algorithm();
  hash_ctx_.size = shash::GetContextSize(hash_ctx_.algorithm);
  hash_ctx_buffer_ = smalloc(hash_ctx_.size);
  hash_ctx_.buffer = hash_ctx_buffer_;
}


//------------------------------------------------------------------------------


BlockItem::BlockItem()
  : type_(kBlockHollow)
  , tag_(-1)
  , file_item_(NULL)
  , chunk_item_(NULL)
  , data_(NULL)
  , capacity_(0)
  , size_(0)
{ }


BlockItem::BlockItem(uint64_t tag)
  : type_(kBlockHollow)
  , tag_(tag)
  , file_item_(NULL)
  , chunk_item_(NULL)
  , data_(NULL)
  , capacity_(0)
  , size_(0)
{ }


void BlockItem::Discharge() {
  data_.Release();
  size_ = capacity_ = 0;
}


void BlockItem::MakeStop() {
  assert(type_ == kBlockHollow);
  type_ = kBlockStop;
}


void BlockItem::MakeData(uint32_t capacity) {
  assert(type_ == kBlockHollow);

  type_ = kBlockData;
  capacity_ = capacity;
  data_ = reinterpret_cast<unsigned char *>(smalloc(capacity_));
}


/**
 * Move data from one block to another.
 */
void BlockItem::MakeData(
  unsigned char *data,
  uint32_t size)
{
  assert(type_ == kBlockHollow);

  type_ = kBlockData;
  capacity_ = size_ = size;
  data_ = data;
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
