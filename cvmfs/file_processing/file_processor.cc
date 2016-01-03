/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "file_processor.h"

#include <cassert>
#include <string>

#include "../logging.h"
#include "chunk.h"
#include "chunk_detector.h"
#include "file.h"
#include "io_dispatcher.h"

namespace upload {

FileProcessor::FileProcessor(AbstractUploader         *uploader,
                             const SpoolerDefinition  &spooler_definition) :
  io_dispatcher_(new IoDispatcher(uploader,
                                  this,
                                  spooler_definition.number_of_threads)),
  compression_alg_(spooler_definition.compression_alg),
  hash_algorithm_(spooler_definition.hash_algorithm),
  chunking_enabled_(spooler_definition.use_file_chunking),
  minimal_chunk_size_(spooler_definition.min_file_chunk_size),
  average_chunk_size_(spooler_definition.avg_file_chunk_size),
  maximal_chunk_size_(spooler_definition.max_file_chunk_size)
{
  assert(io_dispatcher_ != NULL);
  assert(!chunking_enabled_ || minimal_chunk_size_ > 0);
  assert(!chunking_enabled_ || average_chunk_size_ > 0);
  assert(!chunking_enabled_ || maximal_chunk_size_ > 0);
  assert(!chunking_enabled_ || minimal_chunk_size_ <= average_chunk_size_);
  assert(!chunking_enabled_ || average_chunk_size_ <= maximal_chunk_size_);
}


FileProcessor::~FileProcessor() {
  delete io_dispatcher_;
  io_dispatcher_ = NULL;
}


void FileProcessor::Process(const std::string   &local_path,
                            const bool           allow_chunking,
                            const shash::Suffix  hash_suffix) {
  ChunkDetector *chunk_detector = (chunking_enabled_ && allow_chunking)
                                        ? new Xor32Detector(minimal_chunk_size_,
                                                            average_chunk_size_,
                                                            maximal_chunk_size_)
                                        : NULL;
  File *file = new File(local_path,
                        io_dispatcher_,
                        chunk_detector,
                        hash_algorithm_,
                        compression_alg_,
                        hash_suffix);

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Scheduling '%s' for processing ("
                                        "chunking: %s, hash_suffix: %c)",
           local_path.c_str(),
           ((allow_chunking) ? "true" : "false"),
           hash_suffix);
  io_dispatcher_->ScheduleRead(file);
}


void FileProcessor::FileDone(File *file) {
  assert(file != NULL);
  assert(!file->path().empty());
  assert(file->bulk_chunk() != NULL);
  assert(!file->bulk_chunk()->content_hash().IsNull());

  // extract crucial information from the Chunk structures and wrap them into
  // the global FileChunk data structure
  FileChunkList resulting_chunks;
  const ChunkVector &generated_chunks = file->chunks();
  ChunkVector::const_iterator i    = generated_chunks.begin();
  ChunkVector::const_iterator iend = generated_chunks.end();
  for (; i != iend; ++i) {
    Chunk *current_chunk = *i;
    shash::Any chunk_hash = current_chunk->content_hash();
    chunk_hash.set_suffix(shash::kSuffixPartial);
    resulting_chunks.PushBack(FileChunk(chunk_hash,
                                        current_chunk->offset(),
                                        current_chunk->size()));
  }

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "File '%s' processed completely "
                                        "(bulk hash: %s suffix: %c)",
           file->path().c_str(),
           file->bulk_chunk()->content_hash().ToString().c_str(),
           file->hash_suffix());
  assert(file->hash_suffix() == file->bulk_chunk()->content_hash().suffix);
  NotifyListeners(SpoolerResult(0,
                                file->path(),
                                file->bulk_chunk()->content_hash(),
                                resulting_chunks,
                                compression_alg_));
}


void FileProcessor::WaitForProcessing() {
  io_dispatcher_->Wait();
}

}  // namespace upload
