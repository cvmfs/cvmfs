/**
 * This file is part of the CernVM File System.
 */

#ifndef UPLOAD_FILE_PROCESSING_FILE_PROCESSOR_H_
#define UPLOAD_FILE_PROCESSING_FILE_PROCESSOR_H_

#include "../util.h"

namespace upload {

class IoDispatcher;

class FileProcessor {
 public:
  FileProcessor(const bool   enable_file_chunking,
                const size_t minimal_chunk_size = 2 * 1024 * 1024,
                const size_t average_chunk_size = 4 * 1024 * 1024,
                const size_t maximal_chunk_size = 8 * 1024 * 1024);
  virtual ~FileProcessor();

  void Process(const std::string  &local_path,
               const bool          allow_chunking,
               const std::string  &hash_suffix = "");

  void WaitForProcessing();

 private:
  IoDispatcher  *io_dispatcher_;

  const bool     chunking_enabled_;
  const size_t   minimal_chunk_size_;
  const size_t   average_chunk_size_;
  const size_t   maximal_chunk_size_;
};

}

#endif /* UPLOAD_FILE_PROCESSING_FILE_PROCESSOR_H_ */
