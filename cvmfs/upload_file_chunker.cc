#include "upload_file_chunker.h"

#include <algorithm>

#include "upload_file_chunker_xor32.h"

using namespace upload;

size_t
ChunkGenerator::minimal_chunk_size_ =  4 * 1024 * 1024;
size_t
ChunkGenerator::average_chunk_size_ =  8 * 1024 * 1024;
size_t
ChunkGenerator::maximal_chunk_size_ = 16 * 1024 * 1024;

void ChunkGenerator::SetFileChunkRestrictions(const size_t minimal_chunk_size,
                                              const size_t average_chunk_size,
                                              const size_t maximal_chunk_size) {
  assert (minimal_chunk_size < average_chunk_size &&
          average_chunk_size < maximal_chunk_size);

  ChunkGenerator::minimal_chunk_size_ = minimal_chunk_size;
  ChunkGenerator::average_chunk_size_ = average_chunk_size;
  ChunkGenerator::maximal_chunk_size_ = maximal_chunk_size;
}


void ChunkGenerator::RegisterPlugins() {
  RegisterPlugin<Xor32ChunkGenerator>();
  RegisterPlugin<NaiveChunkGenerator>();
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

//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


bool NaiveChunkGenerator::WillHandle(const MemoryMappedFile &mmf) {
  // this will always kick in as a last resort!
  return true;
}


off_t NaiveChunkGenerator::FindNextCutMark() const {
  assert (HasMoreData());
  return offset() + std::min(average_chunk_size_, mmf().size() - offset());
}
