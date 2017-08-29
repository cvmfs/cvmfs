/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_CHUNK_DETECTOR_H_
#define CVMFS_INGESTION_CHUNK_DETECTOR_H_

#include <gtest/gtest_prod.h>
#include <stdint.h>

#include <algorithm>

class BlockItem;

/**
 * Abstract base class for a cutmark detector. This decides on which file
 * positions a File should be chunked.
 */
class ChunkDetector {
 public:
  ChunkDetector() : last_cut_(0), offset_(0) {}
  virtual ~ChunkDetector() { }
  uint64_t FindNextCutMark(BlockItem *block);

  virtual bool MightFindChunks(uint64_t size) const = 0;

 protected:
  virtual uint64_t DoFindNextCutMark(BlockItem *block) = 0;

  /**
   * When returning from an implemented FindNextCutMark call you must call this
   * function when a cut mark has been found.
   * Like: return DoCut(found_offset)
   */
  virtual uint64_t DoCut(uint64_t offset) {
    last_cut_ = offset;
    return offset;
  }

  /**
   * Same as DoCut() but if no cut mark has been found in the given buffer in
   * FindNextCutMark()
   */
  virtual uint64_t NoCut(uint64_t offset) { return 0; }

  uint64_t last_cut() const { return last_cut_; }
  uint64_t offset() const { return offset_; }

 private:
  uint64_t last_cut_;
  uint64_t offset_;
};


/**
 * The StaticOffsetDetector cuts files on a hard threshold and generates
 * uniform sized Chunks.
 */
class StaticOffsetDetector : public ChunkDetector {
 public:
  explicit StaticOffsetDetector(uint64_t s) : chunk_size_(s) { }
  bool MightFindChunks(uint64_t size) const { return size > chunk_size_; }

 protected:
  virtual uint64_t DoFindNextCutMark(BlockItem *buffer);

 private:
  const uint64_t chunk_size_;
};


/**
 * The xor32 rolling used in Igor-FS [1].
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
 *     Dissertation of Dipl.-Ing. Kendy Kutzner (2008)
 */
class Xor32Detector : public ChunkDetector {
  FRIEND_TEST(T_ChunkDetectors, Xor32);

 public:
  Xor32Detector(const uint64_t minimal_chunk_size,
                const uint64_t average_chunk_size,
                const uint64_t maximal_chunk_size);

  bool MightFindChunks(const uint64_t size) const {
    return size > minimal_chunk_size_;
  }

 protected:
  virtual uint64_t DoFindNextCutMark(BlockItem *buffer);

  virtual uint64_t DoCut(const uint64_t offset) {
    xor32_     = 0;
    xor32_ptr_ = offset;
    return ChunkDetector::DoCut(offset);
  }

  virtual uint64_t NoCut(const uint64_t offset) {
    xor32_ptr_ = offset;
    return ChunkDetector::NoCut(offset);
  }

  inline void xor32(const unsigned char byte) {
    xor32_ = (xor32_ << 1) ^ byte;
  }

  inline bool CheckThreshold() {
    return std::abs(static_cast<int32_t>(xor32_) - kMagicNumber) < threshold_;
  }

 private:
  // xor32 only depends on a window of the last 32 bytes in the data stream
  static const unsigned kXor32Window = 32;
  static const int32_t kMagicNumber;

  const uint64_t minimal_chunk_size_;
  const uint64_t average_chunk_size_;
  const uint64_t maximal_chunk_size_;
  const int32_t threshold_;

  uint64_t xor32_ptr_;
  uint32_t xor32_;
};

#endif  // CVMFS_INGESTION_CHUNK_DETECTOR_H_
