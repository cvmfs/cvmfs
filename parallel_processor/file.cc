#include "file.h"

#include "chunk.h"
#include "io_dispatcher.h"

File::~File() {
  if (HasBulkChunk()) {
    delete bulk_chunk_;
  }

  ChunkVector::const_iterator i    = chunks_.begin();
  ChunkVector::const_iterator iend = chunks_.end();
  for (; i != iend; ++i) {
    delete *i;
  }
  chunks_.clear();
}


void File::AddChunk(Chunk *chunk) {
  io_dispatcher_->RegisterChunk(chunk);
  if (chunk->IsBulkChunk()) {
    bulk_chunk_ = chunk;
  } else {
    chunks_.push_back(chunk);
  }
}


void File::CreateInitialChunk() {
  assert (bulk_chunk_    == NULL);
  assert (chunks_.size() == 0);

  // if we are dealing with a file that will definitely _not_ be chunked, we
  // directly mark the initial chunk as being a bulk chunk
  const off_t offset        = 0;
  Chunk *new_chunk = new Chunk(this, offset);

  // for a potentially chunked file, the initial chunk needs to defer the write
  // back of data until a final decision has been made
  if (might_become_chunked_) {
    new_chunk->EnableDeferredWrite();
  } else {
    new_chunk->SetAsBulkChunk();
    new_chunk->set_size(size_);
  }

  // register the new initial chunk
  AddChunk(new_chunk);
}


Chunk* File::CreateNextChunk(const off_t offset) {
  assert (offset > 0 && offset < size_);
  assert (chunks_.size() > 0);
  assert (might_become_chunked_);

  Chunk *latest_chunk = current_chunk();
  assert (! latest_chunk->IsFullyDefined());

  // copy the initially created Chunk as the bulk_chunk_ as soon as we create
  // a second Chunk, thus defining the file to be chunked in general
  if (! HasBulkChunk()) {
    ForkOffBulkChunk();
  }

  // define the size of the current Chunk as we are creating a new Chunk that
  // will start at 'offset'
  latest_chunk->set_size(offset - latest_chunk->offset());
  Chunk *predecessor = latest_chunk;
  AddChunk(new Chunk(this, offset));

  return predecessor;
}


void File::FinalizeLastChunk() {
  assert (might_become_chunked_);
  assert (chunks_.size() > 0);

  Chunk *latest_chunk = current_chunk();
  assert (latest_chunk != NULL);
  assert (! latest_chunk->IsFullyDefined());

  latest_chunk->set_size(size_ - latest_chunk->offset());
  assert (latest_chunk->offset() + latest_chunk->size() == size_);
}


void File::Finalize() {
  if (might_become_chunked_) {
    // do some sanity checks for chunked files (might be removed later on)
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
  } else {
    assert (chunks_.size() == 0);
  }

  // more sanity checks
  assert (bulk_chunk_           != NULL);
  assert (bulk_chunk_->offset() == 0);
  assert (bulk_chunk_->size()   == size_);
  assert (bulk_chunk_->IsFullyProcessed());

  // notify about the finished file processing
  io_dispatcher_->CommitFile(this);
}


void File::ForkOffBulkChunk() {
  assert (! HasBulkChunk());
  Chunk *latest_chunk = current_chunk();
  assert (latest_chunk != NULL);
  assert (latest_chunk->offset() == 0 && ! latest_chunk->IsFullyDefined());

  Chunk* bulk_chunk = latest_chunk->CopyAsBulkChunk(size_);
  AddChunk(bulk_chunk);
}


void File::ChunkCommitted(Chunk *chunk) {
  if (++committed_chunks_ == chunks_.size() + 1) {
    Finalize();
  }
}
