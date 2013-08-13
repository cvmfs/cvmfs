#include "chunk_detector.h"

#include <algorithm>
#include <limits>

off_t StaticOffsetDetector::FindNextCutMark(CharBuffer *buffer) {
  const off_t next_cut = last_cut() + offset_;
  if (next_cut >= buffer->base_offset() + buffer->size()) {
    return NoCut(next_cut);
  }

  return DoCut(next_cut);
}


//
// # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
//



// this defines the center of the interval where the xor32 rolling checksum is
// queried. You should never change this number, since it affects the definition
// of cut marks.
const int32_t Xor32Detector::magic_number_   = // TODO: C++11 (constexpr (?))
  std::numeric_limits<uint32_t>::max() / 2;
const int32_t Xor32Detector::threshold_ =
  std::numeric_limits<uint32_t>::max() / ChunkDetector::kAvgChunkSize;


off_t Xor32Detector::FindNextCutMark(CharBuffer *buffer) {
  assert (ChunkDetector::kMinChunkSize >= xor32_influence);
  const unsigned char *data = buffer->ptr();

  // get the offset where the next xor32 computation needs to be continued
  // Note: this could be after collecting at least kMinChunkSize bytes in the
  //       current chunk, or directly at the beginning of the buffer, when a
  //       cut mark is currently searched
  const off_t global_offset =
    std::max(
           last_cut() +
           static_cast<off_t>(ChunkDetector::kMinChunkSize - xor32_influence),
           xor32_ptr_);

  // check if the next xor32 computation is taking place in the current buffer
  if (global_offset >= buffer->base_offset() + buffer->used_bytes()) {
    return NoCut(global_offset);
  }

  // get the byte offset in the current buffer
  off_t internal_offset = global_offset - buffer->base_offset();
  assert (internal_offset >= 0);
  assert (internal_offset < buffer->used_bytes());

  // precompute the xor32 rolling checksum for finding the next cut mark
  // Note: this might be skipped, if the precomputation was already performed
  //       for the current rolling checksum
  //       (internal_precompute_end will be negative --> loop is not entered)
  const off_t precompute_end = last_cut() + ChunkDetector::kMinChunkSize;
  const off_t internal_precompute_end =
    std::min(precompute_end - buffer->base_offset(),
             static_cast<off_t>(buffer->used_bytes()));
  assert (internal_precompute_end - internal_offset <=
          static_cast<off_t>(xor32_influence));
  for (; internal_offset < internal_precompute_end; ++internal_offset) {
    xor32(data[internal_offset]);
  }

  // do the actual computation and try to find a xor32 based cut mark
  // Note: this loop is bound either by kMaxChunkSize or by the size of the
  //       current buffer, thus the computation would continue later
  const off_t internal_max_chunk_size_end =
    last_cut() + ChunkDetector::kMaxChunkSize - buffer->base_offset();
  const off_t internal_compute_end =
    std::min(internal_max_chunk_size_end,
             static_cast<off_t>(buffer->used_bytes()));
  for (; internal_offset < internal_compute_end; ++internal_offset) {
    xor32(data[internal_offset]);

    // check if we found a cut mark
    if (CheckThreshold()) {
      return DoCut(internal_offset + buffer->base_offset());
    }
  }

  // check if the loop was exited because we reached kMaxChunkSize and do a
  // hard cut in this case
  // if not, it exited because we ran out of data in this buffer --> continue
  // computation with the next buffer
  if (internal_offset == internal_max_chunk_size_end) {
    return DoCut(internal_offset + buffer->base_offset());
  } else {
    return NoCut(internal_offset + buffer->base_offset());
  }
}

