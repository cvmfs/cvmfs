/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_SPOOLER_RESULT_H_
#define CVMFS_UPLOAD_SPOOLER_RESULT_H_

#include <string>

#include "compression.h"
#include "file_chunk.h"

namespace upload {

/**
 * This data structure will be passed to every callback spoolers will invoke.
 * It encapsulates the results of a spooler command along with the given
 * local_path to identify the spooler action performed.
 *
 * Note: When the return_code is different from 0 the content_hash is most
 *       likely undefined, Null or rubbish.
 */
struct SpoolerResult {
  SpoolerResult(const int             return_code = -1,
                const std::string     &local_path  = "",
                const shash::Any      &digest      = shash::Any(),
                const FileChunkList   &file_chunks = FileChunkList(),
                const zlib::Algorithms  compression_alg = zlib::kZlibDefault) :
    return_code(return_code),
    local_path(local_path),
    content_hash(digest),
    file_chunks(file_chunks),
    compression_alg(compression_alg) {}

  inline bool IsChunked() const { return !file_chunks.IsEmpty(); }

  int           return_code;   //!< the return value of the spooler operation
  std::string   local_path;    //!< the local_path previously given as input
  /**
   * The content_hash of the bulk file derived during processing
   */
  shash::Any    content_hash;
  FileChunkList file_chunks;   //!< the file chunks generated during processing
  zlib::Algorithms  compression_alg;
};

}  // namespace upload

#endif  // CVMFS_UPLOAD_SPOOLER_RESULT_H_
