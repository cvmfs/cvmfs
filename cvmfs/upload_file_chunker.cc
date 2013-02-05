#include "upload_file_chunker.h"

#include <algorithm>

#include "upload_file_chunker_xor32.h"

using namespace upload;

const size_t  // TODO: make this configurable
ChunkGenerator::average_chunk_size_ =  8 * 1024 * 1024;
const size_t
ChunkGenerator::minimal_chunk_size_ =  4 * 1024 * 1024;
const size_t
ChunkGenerator::maximal_chunk_size_ = 16 * 1024 * 1024;

ChunkGenerator::RegisteredChunkGenerators
ChunkGenerator::registered_chunk_generators_;

ChunkGenerator* ChunkGenerator::Construct(const MemoryMappedFile &mmf) {
  if (registered_chunk_generators_.empty()) {
    RegisterChunkGenerators();
  }

  RegisteredChunkGenerators::const_iterator i    = registered_chunk_generators_.begin();
  RegisteredChunkGenerators::const_iterator iend = registered_chunk_generators_.end();
  for (; i != iend; ++i) {
    if ((*i)->WillHandleFile(mmf)) {
      return (*i)->Construct(mmf);
    }
  }

  return NULL;
}


void ChunkGenerator::RegisterChunkGenerators() {
  assert (registered_chunk_generators_.empty());

  RegisterChunkGenerator<Xor32ChunkGenerator>();
  RegisterChunkGenerator<NaiveChunkGenerator>();
}


ChunkGenerator::ChunkGenerator(const MemoryMappedFile &mmf) :
  mmf_(mmf),
  offset_(0)
{
  assert (mmf.IsMapped());
}


Chunk ChunkGenerator::Next() {
  assert (HasMoreData());

  // check if there is enough data left to chunk it at least one more time
  const off_t  next_cut_mark = (mmf_.size() - offset_ <= average_chunk_size_) ?
                                  mmf_.size()                                 :
                                  FindNextCutMark();
  const off_t  chunk_offset  = offset_;
  const size_t chunk_size    = next_cut_mark - chunk_offset;

  offset_ = next_cut_mark;
  return Chunk(chunk_offset, chunk_size);
}


bool ChunkGenerator::HasMoreData() const {
  return (size_t)offset_ < mmf_.size();
}


bool NaiveChunkGenerator::WillHandleFile(const MemoryMappedFile &mmf) {
  // this will always kick in as a last resort!
  return true;
}


off_t NaiveChunkGenerator::FindNextCutMark() const {
  assert (HasMoreData());
  return offset() + std::min(average_chunk_size_, mmf().size() - offset());
}
