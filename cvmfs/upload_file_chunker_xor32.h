/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_FILE_CHUNKER_XOR32_H_
#define CVMFS_UPLOAD_FILE_CHUNKER_XOR32_H_

#include "upload_file_chunker.h"

namespace upload {

class Xor32ChunkGenerator : public ChunkGenerator {
 public:
  Xor32ChunkGenerator(const MemoryMappedFile &mmf);
  static bool  WillHandleFile(const MemoryMappedFile &mmf);

 protected:
  off_t        FindNextCutMark() const;

  typedef std::pair<size_t, uint32_t> Threshold;
  typedef std::vector<Threshold> Thresholds;

 private:
  void InitThresholds();

 private:
  Thresholds            thresholds_;
  static const uint32_t magic_number_;
  static const uint32_t base_threshold_;
};

}

#endif /* CVMFS_UPLOAD_FILE_CHUNKER_XOR32_H_ */
