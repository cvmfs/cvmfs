#include "file.h"

#include "chunk.h"


File::~File() {
  if (bulk_chunk_ != NULL) {
    delete bulk_chunk_;
  }

  ChunkVector::const_iterator i    = chunks_.begin();
  ChunkVector::const_iterator iend = chunks_.end();
  for (; i != iend; ++i) {
    delete *i;
  }
  chunks_.clear();
}


Chunk* File::CreateNextChunk(const off_t offset) {
  assert (offset < size_);
  assert (current_chunk_ == NULL || current_chunk_->size() == 0);

  // check if we are currently initializing the first current_chunk_
  const bool creates_initial_chunk = (current_chunk_ == NULL);
  assert (creates_initial_chunk || might_become_chunked_);
  assert (offset != 0 || creates_initial_chunk);

  if (! creates_initial_chunk) {
    // copy the first current_chunk_ as the bulk_chunk_ as soon as we create
    // a second chunk, thus defining the file to be chunked in general
    if (! HasBulkChunk()) {
      assert (might_become_chunked_);
      CreateBulkChunk();
    }

    // fix the size of the current_chunk_ since we now create it's successor at
    // the given offset
    current_chunk_->set_size(offset - current_chunk_->offset());
  }

  // create and register a new chunk
  Chunk *predecessor = current_chunk_;
  current_chunk_ = new Chunk(offset);
  if (creates_initial_chunk && might_become_chunked_) {
    assert (offset == 0);
    current_chunk_->EnableDeferredWrite();
  }
  chunks_.push_back(current_chunk_);

  return predecessor;
}


Chunk* File::FinalizeLastChunk() {
  assert (current_chunk_         != NULL);
  assert (current_chunk_->size() == 0);
  current_chunk_->set_size(size_ - current_chunk_->offset());
  return current_chunk_;
}


void File::Finalize() {
  // doing some sanity checks (might be removed later on)
  assert (bulk_chunk_ == NULL || chunks_.size() > 1);

  size_t aggregated_size    = 0;
  off_t  previous_chunk_end = 0;
  ChunkVector::const_iterator i    = chunks_.begin();
  ChunkVector::const_iterator iend = chunks_.end();
  for (; i != iend; ++i) {
    Chunk *current_chunk = *i;
    assert (current_chunk->size() > 0);
    assert (previous_chunk_end == current_chunk->offset());
    assert (current_chunk->IsFullyProcessed());
    previous_chunk_end = current_chunk->offset() + current_chunk->size();
    aggregated_size += current_chunk->size();
  }
  assert (aggregated_size == size_);

  // a file with only one generated chunk is _not_ a chunked file and it's
  // bulk_chunk_ is equal to the single generated chunk
  if (chunks_.size() == 1) {
    assert (bulk_chunk_ == NULL);
    bulk_chunk_ = chunks_.front();
    chunks_.clear();
  }

  // more sanity checks
  assert (bulk_chunk_           != NULL);
  assert (bulk_chunk_->offset() == 0);
  assert (bulk_chunk_->size()   == size_);
  assert (bulk_chunk_->IsFullyProcessed());
}


void File::CreateBulkChunk() {
  assert (current_chunk_           != NULL);
  assert (bulk_chunk_              == NULL);
  assert (current_chunk_->offset() == 0 && current_chunk_->size() == 0);
  bulk_chunk_ = current_chunk_->CopyAsBulkChunk(size_);
}
