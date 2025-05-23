/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_MMAP_FILE_H_
#define CVMFS_UTIL_MMAP_FILE_H_

#include <cstddef>
#include <string>

#include "util/export.h"
#include "util/single_copy.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif


/**
 * Wraps the functionality of mmap() to create a read-only memory mapped file.
 *
 * Note: You need to call Map() to actually map the provided file path to memory
 */
class CVMFS_EXPORT MemoryMappedFile : SingleCopy {
 public:
  explicit MemoryMappedFile(const std::string &file_path);
  ~MemoryMappedFile();

  bool Map();
  void Unmap();

  inline unsigned char *buffer() const { return mapped_file_; }
  inline size_t size() const { return mapped_size_; }
  inline const std::string &file_path() const { return file_path_; }

  inline bool IsMapped() const { return mapped_; }

 private:
  const std::string file_path_;
  int file_descriptor_;
  unsigned char *mapped_file_;
  size_t mapped_size_;
  bool mapped_;
};


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_MMAP_FILE_H_
