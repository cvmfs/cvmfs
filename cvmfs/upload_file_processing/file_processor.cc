/**
 * This file is part of the CernVM File System.
 */

#include "file_processor.h"

#include <cassert>

#include "io_dispatcher.h"
#include "file.h"
#include "chunk_detector.h"
#include "../logging.h"

using namespace upload;

FileProcessor::FileProcessor(const bool   enable_file_chunking,
                             const size_t minimal_chunk_size,
                             const size_t average_chunk_size,
                             const size_t maximal_chunk_size) :
  io_dispatcher_(new IoDispatcher),
  chunking_enabled_(enable_file_chunking),
  minimal_chunk_size_(minimal_chunk_size),
  average_chunk_size_(average_chunk_size),
  maximal_chunk_size_(maximal_chunk_size)
{
  assert (io_dispatcher_ != NULL);
}


FileProcessor::~FileProcessor() {
  delete io_dispatcher_;
  io_dispatcher_ = NULL;
}


void FileProcessor::Process(const std::string  &local_path,
                            const bool          allow_chunking,
                            const std::string  &hash_suffix) {
  ChunkDetector *chunk_detector = new Xor32Detector(minimal_chunk_size_,
                                                    average_chunk_size_,
                                                    maximal_chunk_size_);
  File *file = new File(local_path,
                        io_dispatcher_,
                        chunk_detector,
                        chunking_enabled_ && allow_chunking,
                        hash_suffix);

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Scheduling '%s' for processing ("
                                        "chunking: %s, hash_suffix: %s)",
           local_path.c_str(),
           ((allow_chunking) ? "true" : "false"),
           hash_suffix.c_str());
  io_dispatcher_->ScheduleRead(file);
}


void FileProcessor::WaitForProcessing() {
  io_dispatcher_->Wait();
}
