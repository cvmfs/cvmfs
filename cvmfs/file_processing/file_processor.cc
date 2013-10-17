/**
 * This file is part of the CernVM File System.
 */

#include "file_processor.h"

#include <cassert>

#include "io_dispatcher.h"
#include "file.h"
#include "chunk.h"
#include "chunk_detector.h"
#include "../logging.h"

using namespace upload;

FileProcessor::FileProcessor(AbstractUploader *uploader,
                             const bool        enable_file_chunking,
                             const size_t      minimal_chunk_size,
                             const size_t      average_chunk_size,
                             const size_t      maximal_chunk_size) :
  io_dispatcher_(new IoDispatcher(uploader, this)),
  chunking_enabled_(enable_file_chunking),
  minimal_chunk_size_(minimal_chunk_size),
  average_chunk_size_(average_chunk_size),
  maximal_chunk_size_(maximal_chunk_size)
{
  assert (io_dispatcher_ != NULL);
  assert (!chunking_enabled_ || minimal_chunk_size_ > 0);
  assert (!chunking_enabled_ || average_chunk_size_ > 0);
  assert (!chunking_enabled_ || maximal_chunk_size_ > 0);
  assert (!chunking_enabled_ || minimal_chunk_size_ <= average_chunk_size_);
  assert (!chunking_enabled_ || average_chunk_size_ <= maximal_chunk_size_);
}


FileProcessor::~FileProcessor() {
  delete io_dispatcher_;
  io_dispatcher_ = NULL;
}


void FileProcessor::Process(const std::string  &local_path,
                            const bool          allow_chunking,
                            const std::string  &hash_suffix) {
  ChunkDetector *chunk_detector = (chunking_enabled_ && allow_chunking)
                                        ? new Xor32Detector(minimal_chunk_size_,
                                                            average_chunk_size_,
                                                            maximal_chunk_size_)
                                        : NULL;
  File *file = new File(local_path,
                        io_dispatcher_,
                        chunk_detector,
                        hash_suffix);

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Scheduling '%s' for processing ("
                                        "chunking: %s, hash_suffix: %s)",
           local_path.c_str(),
           ((allow_chunking) ? "true" : "false"),
           hash_suffix.c_str());
  io_dispatcher_->ScheduleRead(file);
}


void FileProcessor::FileDone(File *file) {
  assert (file != NULL);
  assert (! file->path().empty());
  assert (file->bulk_chunk() != NULL);
  assert (! file->bulk_chunk()->content_hash().IsNull());

  // extract crucial information from the Chunk structures and wrap them into
  // the global FileChunk data structure
  FileChunkList resulting_chunks;
  const ChunkVector &generated_chunks = file->chunks();
  ChunkVector::const_iterator i    = generated_chunks.begin();
  ChunkVector::const_iterator iend = generated_chunks.end();
  for (; i != iend; ++i) {
    Chunk *current_chunk = *i;
    resulting_chunks.PushBack(FileChunk(current_chunk->content_hash(),
                                        current_chunk->offset(),
                                        current_chunk->size()));
  }

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "File '%s' processed completely",
           file->path().c_str());
  NotifyListeners(SpoolerResult(0,
                                file->path(),
                                file->bulk_chunk()->content_hash(),
                                resulting_chunks));
}


void FileProcessor::WaitForProcessing() {
  io_dispatcher_->Wait();
}
