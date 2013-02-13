#include "upload_file_chunker_xor32.h"

#include <limits>

using namespace upload;

// this defines the center of the interval where the xor32 rolling checksum is
// queried. You should never change this number, since it affects the definition
// of cut marks.
// Note: We consider checksums respectively their underlying byte-patterns
//       'stronger', if their difference to the magic_number is smaller.
//       See Xor32ChunkGenerator::InitThresholds()
const uint32_t Xor32ChunkGenerator::magic_number_   =
  std::numeric_limits<uint32_t>::max() / 2;
const uint32_t Xor32ChunkGenerator::base_threshold_ =
  std::numeric_limits<uint32_t>::max() / average_chunk_size_;


Xor32ChunkGenerator::Xor32ChunkGenerator(const MemoryMappedFile &mmf) :
  ChunkGenerator(mmf)
{
  InitThresholds();
}

void Xor32ChunkGenerator::InitThresholds() {
  // threshold interval is centered around magic_number_. We do the division
  // by two only once to save some time:
  static const uint32_t base_threshold = base_threshold_ / 2;

  // this is a list of cut mark thresholds
  // Note: The threshold gets bigger when the chunk starts to grow towards it's
  //       maximal size. It is always better to trigger on a 'weak' pattern than
  //       on a hard predefined file size boundary!
  thresholds_.push_back(std::make_pair(
                              (average_chunk_size_ - minimal_chunk_size_) / 2,
                              (base_threshold / 4) + 1));
  thresholds_.push_back(std::make_pair(
                              average_chunk_size_,
                              ((base_threshold / 4) * 3) + 1));
  thresholds_.push_back(std::make_pair(
                              (maximal_chunk_size_ - average_chunk_size_) / 2,
                              ((base_threshold / 2) * 3) + 1));
  thresholds_.push_back(std::make_pair(
                              maximal_chunk_size_,
                              base_threshold * 4));
}


bool Xor32ChunkGenerator::WillHandle(const MemoryMappedFile &mmf) {
  // TODO: only handle binary files with this chunk generator
  //       even better: files with high informational entropy
  return true;
}

off_t Xor32ChunkGenerator::FindNextCutMark() const {
  assert (HasMoreData());

  const unsigned char  *data           = mmf().buffer();
  const off_t           start_offset   = offset();
  off_t                 off            = offset();
  const size_t          max_data_size  = std::min(
                                                offset() + maximal_chunk_size_,
                                                mmf().size());
  uint32_t              xor_output     = 0;

  assert (max_data_size > minimal_chunk_size_);

  // xor32 only depends on a window of the last 32 bytes in the data stream
  // we therefore do not compute the xor32 checksum for bytes below the minimal
  // chunk size. Although, we need to initialize the xor32 checksum with the
  // last 32 bytes of the data contained in the minimal chunk size
  off += minimal_chunk_size_ - 32;
  for (int i = 0; i < 32; ++i, ++off) {
    xor_output = xor32(xor_output, data[off]);
  }

  // the rest of the data needs to be processed byte by byte to find locations
  // at which the file can be cut. This is the part of the method, that really
  // consumes some time

  Thresholds::const_iterator i    = thresholds_.begin();
  Thresholds::const_iterator iend = thresholds_.end();
  for (; i != iend; ++i) {
    const off_t    limit     = (off_t)std::min(
                                        start_offset + i->first,
                                        max_data_size);
    const uint32_t threshold = i->second;

    for (; off < limit; ++off) {
      // calculate the xor32 rolling checksum
      xor_output = xor32(xor_output, data[off]);

      // check if we found a cut mark
      if (abs(xor_output - magic_number_) < (int32_t)threshold) {
        return off;
      }
    }
  }

  // we didn't find a possible cut mark. This will return the maximal chunk
  // size!
  return off;
}
