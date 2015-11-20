/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "file.h"

#include "chunk.h"
#include "io_dispatcher.h"

#include "../util.h"

namespace upload {

File::File(const std::string    &path,
           IoDispatcher         *io_dispatcher,
           ChunkDetector        *chunk_detector,
           shash::Algorithms     hash_algorithm,
           zlib::Algorithms      compression_alg,
           const shash::Suffix   hash_suffix) :
  AbstractFile(path, GetFileSize(path)),
  might_become_chunked_(chunk_detector != NULL &&
                        chunk_detector->MightFindChunks(size())),
  hash_algorithm_(hash_algorithm),
  hash_suffix_(hash_suffix),
  compression_alg_(compression_alg),
  bulk_chunk_(NULL),
  io_dispatcher_(io_dispatcher),
  chunk_detector_(chunk_detector)
{
  chunks_to_commit_ = 0;  // tbb::atomic has no init constructor
  CreateInitialChunk();
}


File::~File() {
  if (HasBulkChunk()) {
    delete bulk_chunk_;
    bulk_chunk_ = NULL;
  }

  if (HasChunkDetector()) {
    delete chunk_detector_;
    chunk_detector_ = NULL;
  }

  ChunkVector::const_iterator i    = chunks_.begin();
  ChunkVector::const_iterator iend = chunks_.end();
  for (; i != iend; ++i) {
    delete *i;
  }
  chunks_.clear();
}


void File::AddChunk(Chunk *chunk, const bool register_chunk) {
  if (register_chunk) {
    io_dispatcher_->RegisterChunk(chunk);
    ++chunks_to_commit_;
  }
  if (chunk->IsBulkChunk()) {
    bulk_chunk_ = chunk;
  } else {
    chunks_.push_back(chunk);
  }
}


void File::CreateInitialChunk() {
  assert(bulk_chunk_ == NULL);
  assert(chunks_.size() == 0);

  const off_t offset = 0;
  Chunk *new_chunk = new Chunk(this, offset, hash_algorithm_, compression_alg_);

  if (might_become_chunked_) {
    // for a potentially chunked file, the initial chunk needs to defer the
    // write back of data until a final decision has been made
    // as soon as a second chunk has been generated, this chunk will be
    // duplicated and serve as the beginning of the bulk chunk as well
    new_chunk->EnableDeferredWrite();
  } else {
    // if we are dealing with a file that will definitely _not_ be chunked, we
    // directly mark the initial chunk as being a bulk chunk
    new_chunk->SetAsBulkChunk();
    new_chunk->set_size(size());
  }

  // register the new initial chunk
  AddChunk(new_chunk);
}


Chunk* File::CreateNextChunk(const off_t offset) {
  assert(offset > 0 && offset < static_cast<off_t>(size()));
  assert(chunks_.size() > 0);
  assert(might_become_chunked_);

  Chunk *latest_chunk = current_chunk();
  assert(latest_chunk != NULL);
  assert(!latest_chunk->IsFullyDefined());

  // copy the initially created Chunk as the bulk_chunk_ as soon as we create
  // a second Chunk, thus defining the file to be chunked in general
  // (see CreateInitialChunk())
  if (!HasBulkChunk()) {
    ForkOffBulkChunk();
  }

  // define the size of the current Chunk as we are creating a new Chunk that
  // will start at 'offset'
  latest_chunk->set_size(offset - latest_chunk->offset());
  Chunk *predecessor = latest_chunk;
  AddChunk(new Chunk(this, offset, hash_algorithm_, compression_alg_));

  return predecessor;
}


void File::ForkOffBulkChunk() {
  // (see CreateInitialChunk())
  // this takes care of copying the first generated Chunk to use it as bulk
  // Chunk as well as first Chunk in the list
  assert(!HasBulkChunk());
  Chunk *latest_chunk = current_chunk();
  assert(latest_chunk != NULL);
  assert(latest_chunk->offset() == 0 && !latest_chunk->IsFullyDefined());

  Chunk* bulk_chunk = latest_chunk->CopyAsBulkChunk(size());
  AddChunk(bulk_chunk);
}


void File::PromoteSingleChunkAsBulkChunk() {
  // if the file was initally classified as possible chunked file but turns out
  // to have no cuts, we need to use it's only Chunk as bulk Chunk.
  assert(might_become_chunked_);
  assert(chunks_.size() == 1);
  Chunk *only_chunk = current_chunk();
  assert(only_chunk != NULL);
  assert(only_chunk->offset() == 0);
  assert(only_chunk->size() == size());
  assert(!only_chunk->IsBulkChunk());
  assert(only_chunk->IsFullyDefined());

  // Use the single generated Chunk as bulk chunk and clear the chunked file
  // Chunk list
  only_chunk->SetAsBulkChunk();
  chunks_.clear();
  AddChunk(only_chunk, false);  // do not register in the IoDispatcher again
                                // this chunk is only promoted, not created
}


void File::FullyDefineLastChunk() {
  assert(might_become_chunked_);
  assert(chunks_.size() > 0);

  Chunk *latest_chunk = current_chunk();
  assert(latest_chunk != NULL);
  assert(!latest_chunk->IsFullyDefined());

  latest_chunk->set_size(size() - latest_chunk->offset());
  assert(latest_chunk->offset() + latest_chunk->size() == size());

  // only one Chunk was generated during the processing of this file, though it
  // was classified as possible chunked file --> re-define the file as not being
  // chunked and use the single generated Chunk as bulk Chunk
  if (might_become_chunked_ && !HasBulkChunk()) {
    PromoteSingleChunkAsBulkChunk();
  }
}


void File::Finalize() {
#ifndef NDEBUG
  // check sanity of generated chunk list (this code does not manipulate any-
  // thing and can safely be disabled in release mode)
  if (might_become_chunked_ && chunks_.size() > 0) {
    size_t aggregated_size    = 0;
    off_t  previous_chunk_end = 0;
    ChunkVector::const_iterator i    = chunks_.begin();
    ChunkVector::const_iterator iend = chunks_.end();
    for (; i != iend; ++i) {
      Chunk *current_chunk = *i;
      assert(current_chunk->size() > 0);
      assert(previous_chunk_end == current_chunk->offset());
      assert(current_chunk->IsFullyProcessed());
      previous_chunk_end = current_chunk->offset() + current_chunk->size();
      aggregated_size += current_chunk->size();
    }
    assert(aggregated_size == size());
  } else {
    assert(chunks_.size() == 0);
  }
#endif

  // more sanity checks
  assert(HasBulkChunk());
  assert(bulk_chunk_->offset() == 0);
  assert(bulk_chunk_->size()   == size());
  assert(bulk_chunk_->IsFullyProcessed());

  // notify about the finished file processing
  io_dispatcher_->CommitFile(this);
}


void File::ChunkCommitted(Chunk *chunk) {
  if (--chunks_to_commit_ == 0) {
    Finalize();
  }
}

}  // namespace upload
