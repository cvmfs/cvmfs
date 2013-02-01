#include "upload_file_chunker.h"

#include <algorithm>

using namespace upload;

size_t  // TODO: make this configurable
ChunkGenerator::average_chunk_size_ = 8 * 1024 * 1024;

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
  const off_t  next_cut_mark = FindNextCutMark();
  const off_t  chunk_offset = offset_;
  const size_t chunk_size   = next_cut_mark - chunk_offset;

  offset_ = next_cut_mark;
  return Chunk(chunk_offset, chunk_size);
}


bool ChunkGenerator::HasMoreData() const {
  return offset_ < mmf_.size();
}


bool NaiveChunkGenerator::WillHandleFile(const MemoryMappedFile &mmf) {
  // this will always kick in as a last resort!
  return true;
}


off_t NaiveChunkGenerator::FindNextCutMark() const {
  assert (HasMoreData());
  return offset() + std::min(average_chunk_size_, mmf().size() - offset());
}
