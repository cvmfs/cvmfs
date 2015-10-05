/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_FILE_PROCESSING_CHUNK_DETECTOR_H_
#define CVMFS_FILE_PROCESSING_CHUNK_DETECTOR_H_

#include <gtest/gtest_prod.h>
#include <sys/types.h>

#include <algorithm>
#include <utility>
#include <vector>

#include "char_buffer.h"

namespace upload {

/**
 * Abstract base class for a cutmark detector.
 * This decides on which file positions a File should be chunked.
 */
class ChunkDetector {
 public:
  ChunkDetector() : last_cut_(0) {}
  virtual ~ChunkDetector() { }
  virtual off_t FindNextCutMark(CharBuffer *buffer) = 0;

  virtual bool MightFindChunks(const size_t size) const = 0;

 protected:
  /**
   * When returning from an implemented FindNextCutMark call you must call this
   * function when a cut mark has been found.
   * Like: return DoCut(found_offset)
   */
  virtual off_t DoCut(const off_t offset) {
    last_cut_ = offset;
    return offset;
  }

  /**
   * Same as DoCut() but if no cut mark has been found in the given Buffer in
   * FindNextCutMark()
   */
  virtual off_t NoCut(const off_t offset) { return 0; }

  off_t last_cut() const { return last_cut_; }

 private:
  off_t last_cut_;
};

/**
 * The StaticOffsetDetector cuts files on a hard threshold and generates
 * uniform size Chunks.
 */
class StaticOffsetDetector : public ChunkDetector {
 public:
  explicit StaticOffsetDetector(const size_t static_chunk_size) :
    chunk_size_(static_chunk_size) {}

  bool MightFindChunks(const size_t size) const { return size > chunk_size_; }

  off_t FindNextCutMark(CharBuffer *buffer);

 private:
  const size_t chunk_size_;
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
  FRIEND_TEST(T_ChunkDetectors, Xor32);

 protected:
  typedef std::pair<size_t, uint32_t> Threshold;
  typedef std::vector<Threshold> Thresholds;

  // xor32 only depends on a window of the last 32 bytes in the data stream
  static const size_t xor32_influence = 32;

 public:
  Xor32Detector(const size_t minimal_chunk_size,
                const size_t average_chunk_size,
                const size_t maximal_chunk_size);

  bool MightFindChunks(const size_t size) const {
    return size > minimal_chunk_size_;
  }

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

  inline void xor32(const unsigned char byte) {
    xor32_ = (xor32_ << 1) ^ byte;
  }

  inline bool CheckThreshold() {
    return std::abs(static_cast<int32_t>(xor32_) - magic_number_) < threshold_;
  }

 private:
  const size_t minimal_chunk_size_;
  const size_t average_chunk_size_;
  const size_t maximal_chunk_size_;

  off_t    xor32_ptr_;
  uint32_t xor32_;

  static const int32_t magic_number_;
  const int32_t threshold_;
};

}  // namespace upload

#endif  // CVMFS_FILE_PROCESSING_CHUNK_DETECTOR_H_
