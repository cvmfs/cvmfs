/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UPLOAD_SPOOLER_RESULT_H_
#define CVMFS_UPLOAD_SPOOLER_RESULT_H_

#include "compression.h"
#include "file_chunk.h"

#include <string>

namespace upload
{
  /**
   * This data structure will be passed to every callback spoolers will invoke.
   * It encapsulates the results of a spooler command along with the given
   * local_path to identify the spooler action performed.
   *
   * Note: When the return_code is different from 0 the content_hash is most
   *       likely undefined, Null or rubbish.
   */
  struct SpoolerResult {
    SpoolerResult(const int           return_code = -1,
                  const std::string  &local_path  = "",
                  const hash::Any    &digest      = hash::Any(),
                  const FileChunks   &file_chunks = FileChunks()) :
      return_code(return_code),
      local_path(local_path),
      content_hash(digest),
      file_chunks(file_chunks) {}

    inline bool IsChunked() const { return !file_chunks.empty(); }

    int         return_code;  //!< the return value of the spooler operation
    std::string local_path;   //!< the local_path previously given as input
    hash::Any   content_hash; //!< the content_hash of the bulk file derived
                              //!< during processing
    FileChunks  file_chunks;  //!< the file chunks generated during processing
  };
}

#endif /* CVMFS_UPLOAD_SPOOLER_RESULT_H_ */
