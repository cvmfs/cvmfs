/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "processor.h"

#include <tbb/parallel_invoke.h>

#include <algorithm>
#include <vector>

#include "../hash.h"
#include "chunk.h"
#include "file.h"
#include "io_dispatcher.h"

namespace upload {

tbb::task* ChunkProcessingTask::execute() {
  // Thread Safety:
  //   * Only one ChunkProcessingTask for any given Chunk at any given time
  //   * Parallel execution of more than one ChunkProcessingTask for chunks of
  //     the same File is possible
  //   * Multiple ChunkProcessingTasks might share the same CharBuffer

  assert(chunk_->IsInitialized());
  assert(buffer_->IsInitialized());

  // Get the position from where the current input buffer needs to be processed
  // Two different cases:
  //   -> The beginning of the CharBuffer, if the contained data lies in the
  //      middle of the currently processed Chunk
  //   -> At the position where the applicable data for the processed Chunk
  //      starts (i.e. Chunk's starting offset is in the middle of the buffer)
  const off_t internal_offset =
    std::max(off_t(0), chunk_->offset() - buffer_->base_offset());
  assert(internal_offset >= 0);
  assert(static_cast<size_t>(internal_offset) <= buffer_->used_bytes());
  const unsigned char *data = buffer_->ptr() + internal_offset;

  // Determine how many bytes need to be processed
  // Two different cases:
  //   -> The full CharBuffer, if the FileScrubbingTask did not find any Chunk
  //      cut marks in the current buffer (thus not defining the size of the
  //      associated Chunk)
  //   -> The end of the applicable data for the associated Chunk inside the
  //      CharBuffer (defined by the size of the Chunk)
  const size_t byte_count = (chunk_->size() == 0)
    ? buffer_->used_bytes() - internal_offset
    :   std::min(buffer_->base_offset()  + buffer_->used_bytes(),
                 chunk_->offset()        + chunk_->size())
      - std::max(buffer_->base_offset(), chunk_->offset());
  assert(byte_count <= buffer_->used_bytes() - internal_offset);

  // find out, if we are going to process the final block of the chunk
  const bool finalize = (
    (chunk_->IsFullyDefined()) &&
    (buffer_->base_offset() + internal_offset + byte_count
      == chunk_->offset() + chunk_->size()));

  // crunch the data
  Crunch(data, byte_count, finalize);

  // finalize the chunk if necessary
  if (finalize) {
    chunk_->Finalize();
  }

  // the TBB scheduler should figure out what to do next
  return NULL;
}


void ChunkProcessingTask::Crunch(const unsigned char  *data,
                                 const size_t          bytes,
                                 const bool            finalize) {
  shash::ContextPtr &ch_ctx = chunk_->content_hash_context();
  // We need to make a copy
  unsigned char* running_data = const_cast<unsigned char*>(data);
  size_t running_inputsize = bytes;
  bool done = false;
  // Loop through the output, copying data to the deflate buffer
  while (done == false) {
    // Request however much the compressor thinks we need
    size_t deflate_bound =
      chunk_->compressor()->DeflateBound(running_inputsize);
    CharBuffer *compress_buffer = chunk_->GetDeflateBuffer(deflate_bound);
    assert(compress_buffer != NULL);
    assert(compress_buffer->free_bytes() > 0);

    size_t outbufsize = compress_buffer->free_bytes();
    unsigned char* output_start = compress_buffer->free_space_ptr();

    // Do a single deflate
    done = chunk_->compressor()->Deflate(
      finalize, &running_data, &running_inputsize, &output_start, &outbufsize);

    // Now:
    //  outbufsize is the number of bytes used
    //  running_inputsize is the number of bytes left to be read in
    //  running_data is a pointer to the next part of input
    //
    compress_buffer->SetUsedBytes(compress_buffer->used_bytes() + outbufsize);

    // Update the hash
    shash::Update(output_start, outbufsize, ch_ctx);
  }
}


tbb::task* FileScrubbingTask::execute() {
  // Thread Safety:
  //   * Only one executing FileScrubbingTask per File at any time
  //   * Execution is done in-order, thus the File is processed as a stream of
  //     CharBuffers. Each CharBuffer will be processed in one FileScrubbingTask

  File *file = FileScrubbingTask::file();

  if (file->MightBecomeChunked()) {
    // find chunk cut marks in the current buffer and process all chunks that
    // are fully specified (i.e. not reaching beyond the current buffer)
    const CutMarks cut_marks = FindNextChunkCutMarks();
    CutMarks::const_iterator i    = cut_marks.begin();
    CutMarks::const_iterator iend = cut_marks.end();
    for (; i != iend; ++i) {
      Chunk *fully_defined_chunk = file->CreateNextChunk(*i);
      assert(fully_defined_chunk->IsFullyDefined());
      QueueForDeferredProcessing(fully_defined_chunk);
    }

    // if we reached the last buffer this input file will produce, all but the
    // last created chunk will be fully defined at this point
    if (IsLastBuffer()) {
      file->FullyDefineLastChunk();
    }

    // process the current chunk, i.e. the last created chunk that potentially
    // reaches beyond the current buffer or to the end of the file
    Chunk *current_chunk = file->current_chunk();
    if (current_chunk != NULL) {
      QueueForDeferredProcessing(current_chunk);
    }
  }

  // check if the file has a bulk chunk and continue processing it using the
  // current buffer
  if (file->HasBulkChunk()) {
    QueueForDeferredProcessing(file->bulk_chunk());
  }

  // wait for all scheduled chunk processing tasks on the current buffer
  SpawnTasksAndWaitForProcessing();

  // commit the chunks that have been finished during this processing step
  CommitFinishedChunks();

  // go on with the next file buffer
  return Finalize();
}


void FileScrubbingTask::SpawnTasksAndWaitForProcessing() {
  tbb::task_list tasks;
  std::vector<Chunk*>::const_iterator i    = chunks_to_process_.begin();
  std::vector<Chunk*>::const_iterator iend = chunks_to_process_.end();
  for (; i != iend; ++i) {
    tbb::task *chunk_processing_task =
      new(allocate_child()) ChunkProcessingTask(*i, buffer());
    tasks.push_back(*chunk_processing_task);
  }

  set_ref_count(chunks_to_process_.size() + 1);  // +1 for the wait
  spawn_and_wait_for_all(tasks);
}


void FileScrubbingTask::CommitFinishedChunks() const {
  std::vector<Chunk*>::const_iterator i    = chunks_to_process_.begin();
  std::vector<Chunk*>::const_iterator iend = chunks_to_process_.end();
  for (; i != iend; ++i) {
    Chunk *current_chunk = *i;
    if (current_chunk->IsFullyProcessed()) {
      current_chunk->ScheduleCommit();
    }
  }
}


FileScrubbingTask::CutMarks FileScrubbingTask::FindNextChunkCutMarks() {
  File       *file   = FileScrubbingTask::file();
  CharBuffer *buffer = FileScrubbingTask::buffer();

  const Chunk *current_chunk = file->current_chunk();
  assert(file->MightBecomeChunked());
  assert(current_chunk != NULL);
  assert(current_chunk->size() == 0);
  assert(current_chunk->offset() <= buffer->base_offset());
  assert(current_chunk->offset() <  buffer->base_offset() +
                                   static_cast<off_t>(buffer->used_bytes()));

  CutMarks result;
  off_t next_cut;
  while ((next_cut = file->FindNextCutMark(buffer)) != 0) {
    assert(next_cut > 0);
    if (static_cast<size_t>(next_cut) < file->size()) {
      result.push_back(next_cut);
    }
  }

  return result;
}


bool FileScrubbingTask::IsLastBuffer() const {
  const CharBuffer *buffer = FileScrubbingTask::buffer();
  return file()->size() == buffer->base_offset() + buffer->used_bytes();
}

}  // namespace upload
