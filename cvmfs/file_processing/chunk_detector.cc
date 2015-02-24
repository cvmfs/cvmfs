/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "chunk_detector.h"

#include <algorithm>
#include <limits>

namespace upload {

off_t StaticOffsetDetector::FindNextCutMark(CharBuffer *buffer) {
  assert(buffer->IsInitialized());

  const off_t beginning = buffer->base_offset();
  const off_t end =
    buffer->base_offset() + static_cast<off_t>(buffer->used_bytes());

  const off_t next_cut = last_cut() + chunk_size_;
  if (next_cut >= beginning && next_cut < end) {
    return DoCut(next_cut);
  }

  return NoCut(next_cut);
}


//------------------------------------------------------------------------------



// this defines the center of the interval where the xor32 rolling checksum is
// queried. You should never change this number, since it affects the definition
// of cut marks.
// TODO(rmeusel): C++11 (constexpr (?))
const int32_t Xor32Detector::magic_number_   =
  std::numeric_limits<uint32_t>::max() / 2;


Xor32Detector::Xor32Detector(const size_t minimal_chunk_size,
                             const size_t average_chunk_size,
                             const size_t maximal_chunk_size) :
  minimal_chunk_size_(minimal_chunk_size),
  average_chunk_size_(average_chunk_size),
  maximal_chunk_size_(maximal_chunk_size),
  xor32_ptr_(0), xor32_(0),
  threshold_(std::numeric_limits<uint32_t>::max() / average_chunk_size)
{
  assert(minimal_chunk_size_ > 0);
  assert(minimal_chunk_size_ < average_chunk_size_);
  assert(average_chunk_size_ < maximal_chunk_size_);
}


off_t Xor32Detector::FindNextCutMark(CharBuffer *buffer) {
  assert(minimal_chunk_size_ >= xor32_influence);
  const unsigned char *data = buffer->ptr();

  // get the offset where the next xor32 computation needs to be continued
  // Note: this could be after collecting at least kMinChunkSize bytes in the
  //       current chunk, or directly at the beginning of the buffer, when a
  //       cut mark is currently searched
  const off_t global_offset =
    std::max(
           last_cut() +
           static_cast<off_t>(minimal_chunk_size_ - xor32_influence),
           xor32_ptr_);

  // check if the next xor32 computation is taking place in the current buffer
  if (global_offset >= buffer->base_offset()
                       + static_cast<off_t>(buffer->used_bytes())) {
    return NoCut(global_offset);
  }

  // get the byte offset in the current buffer
  off_t internal_offset = global_offset - buffer->base_offset();
  assert(internal_offset >= 0);
  assert(internal_offset < static_cast<off_t>(buffer->used_bytes()));

  // precompute the xor32 rolling checksum for finding the next cut mark
  // Note: this might be skipped, if the precomputation was already performed
  //       for the current rolling checksum
  //       (internal_precompute_end will be negative --> loop is not entered)
  const off_t precompute_end = last_cut() + minimal_chunk_size_;
  const off_t internal_precompute_end =
    std::min(precompute_end - buffer->base_offset(),
             static_cast<off_t>(buffer->used_bytes()));
  assert(internal_precompute_end - internal_offset <=
         static_cast<off_t>(xor32_influence));
  for (; internal_offset < internal_precompute_end; ++internal_offset) {
    xor32(data[internal_offset]);
  }

  // do the actual computation and try to find a xor32 based cut mark
  // Note: this loop is bound either by kMaxChunkSize or by the size of the
  //       current buffer, thus the computation would continue later
  const off_t internal_max_chunk_size_end =
    last_cut() + maximal_chunk_size_ - buffer->base_offset();
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

}  // namespace upload
