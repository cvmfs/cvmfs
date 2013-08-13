/**
 * This file is part of the CernVM File System.
 */

#ifndef ULOAD_FILE_PROCESSING_CHUNK_DETECTOR_H
#define ULOAD_FILE_PROCESSING_CHUNK_DETECTOR_H

#include <algorithm>
#include <vector>
#include <sys/types.h>

#include "buffer.h"

namespace upload {

class ChunkDetector {
 public:
  static const size_t kMinChunkSize = 2 * 1024 * 1024;
  static const size_t kAvgChunkSize = 4 * 1024 * 1024;
  static const size_t kMaxChunkSize = 8 * 1024 * 1024;

 public:
  ChunkDetector() : last_cut_(0) {}
  virtual ~ChunkDetector() {};
  virtual off_t FindNextCutMark(CharBuffer *buffer) = 0;

  static bool MightBecomeChunked(const size_t size) {
    return size > kMinChunkSize;
  }

 protected:
  virtual off_t DoCut(const off_t offset) {
    last_cut_ = offset;
    return offset;
  }

  virtual off_t NoCut(const off_t offset) { return 0; }

  off_t last_cut() const { return last_cut_; }

 private:
  off_t last_cut_;
};


class StaticOffsetDetector : public ChunkDetector {
 public:
  StaticOffsetDetector(const off_t static_offset = ChunkDetector::kAvgChunkSize) :
    offset_(static_offset)
  {
    assert (static_offset >= 0);
    assert (static_cast<size_t>(static_offset) >= kMinChunkSize);
    assert (static_cast<size_t>(static_offset) <  kMaxChunkSize);
  }

  off_t FindNextCutMark(CharBuffer *buffer);

 private:
  const off_t offset_;
};


/**
 * The xor32 rolling checksum was proposed by Kendy Kutzner who used it for
 * file chunking in Igor-FS [1].
 *
 * It takes a byte stream and constantly computes a 32-bit checksum in a way
 * that the result is only dependent on the last read 32 bytes of the stream.
 * Given random input data, the checksum eventually produces each number in the
 * 32-bit value range with roughly the same probability.
 * We exploit that by constantly checking the rolling checksum result for a
 * specific interval. Thus, we detect characteristic 32-byte long patches in a
 * file that do not depend on their actual position inside the data stream.
 * Thereby local modifications of a file might not affect global chunk creation.
 *
 * [1]     "The Decentralized File System Igor-FS
 *          as an Application for Overlay-Networks"
 *     Dissertation of Dipl.-Ing. Kendy Kutzner - 14. Februar 2008
 */
class Xor32Detector : public ChunkDetector {
 protected:
  typedef std::pair<size_t, uint32_t> Threshold;
  typedef std::vector<Threshold> Thresholds;

  // xor32 only depends on a window of the last 32 bytes in the data stream
  static const size_t xor32_influence = 32;

 public:
  Xor32Detector() : xor32_ptr_(0), xor32_(0) {}

  off_t FindNextCutMark(CharBuffer *buffer);

 protected:
  virtual off_t DoCut(const off_t offset) {
    xor32_     = 0;
    xor32_ptr_ = offset;
    return ChunkDetector::DoCut(offset);
  }

  virtual off_t NoCut(const off_t offset) {
    xor32_ptr_ = offset;
    return ChunkDetector::NoCut(offset);
  }

  inline void xor32(const char byte) {
    xor32_ = (xor32_ << 1) ^ byte;
  }

  inline bool CheckThreshold() {
    return std::abs(static_cast<int32_t>(xor32_) - magic_number_) < threshold_;
  }

 private:
  off_t    xor32_ptr_;
  uint32_t xor32_;

  static const int32_t magic_number_;
  static const int32_t threshold_;
};

} // namespace upload

#endif /* ULOAD_FILE_PROCESSING_CHUNK_DETECTOR_H */
