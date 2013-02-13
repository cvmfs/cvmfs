/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_FILE_CHUNKER_XOR32_H_
#define CVMFS_UPLOAD_FILE_CHUNKER_XOR32_H_

#include "upload_file_chunker.h"

namespace upload {

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
 */
class Xor32ChunkGenerator : public ChunkGenerator {
 public:
  Xor32ChunkGenerator(const MemoryMappedFile &mmf);
  static bool  WillHandle(const MemoryMappedFile &mmf);

 protected:
  off_t        FindNextCutMark() const;

  typedef std::pair<size_t, uint32_t> Threshold;
  typedef std::vector<Threshold> Thresholds;

 private:
  void InitThresholds();

  inline uint32_t xor32(const uint32_t checksum, const char byte) const {
    return (checksum << 1) ^ byte;
  }

 private:
  Thresholds            thresholds_;
  static const uint32_t magic_number_;
  static const uint32_t base_threshold_;
};

}

/*
 * [1] "The Decentralized File System Igor-FS as an Application for Overlay-Networks"
 *     Dissertation of Dipl.-Ing. Kendy Kutzner - 14. Februar 2008
 */

#endif /* CVMFS_UPLOAD_FILE_CHUNKER_XOR32_H_ */
