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
  z_stream          &stream = chunk_->zlib_context();
  shash::ContextPtr &ch_ctx = chunk_->content_hash_context();

  // estimate how much space we are going to need approximately
  const size_t max_output_size = deflateBound(&stream, bytes);

  // state the input data in the zlib stream for the next compression step
  stream.avail_in = bytes;
  // sry, but zlib forces me...
  stream.next_in  = const_cast<unsigned char*>(data);
  const int flush = (finalize) ? Z_FINISH : Z_NO_FLUSH;

  int retcode = -1;
  while (true) {
    // obtain a destination CharBuffer for the compression results from the
    // currently processed Chunk.
    CharBuffer *compress_buffer = chunk_->GetDeflateBuffer(max_output_size);
    assert(compress_buffer != NULL);
    assert(compress_buffer->free_bytes() > 0);

    // initialize the zlib stream with the characteristics of the output buffer
    const CharBuffer::pointer_t output_start =
      compress_buffer->free_space_ptr();
    const size_t output_space = compress_buffer->free_bytes();
    stream.avail_out = output_space;
    stream.next_out = output_start;

    // do the compression step
    retcode = deflate(&stream, flush);
    assert(retcode == Z_OK || retcode == Z_STREAM_END);

    // check if zlib produced any bytes, update the used_bytes information in
    // the compression buffer and update the running content hash with the fresh
    // data
    const size_t bytes_produced = output_space - stream.avail_out;
    compress_buffer->SetUsedBytes(
      compress_buffer->used_bytes() + bytes_produced);
    shash::Update(output_start, bytes_produced, ch_ctx);

    // check if the compression for the given input data has finished and stop
    // the compression loop
    if ((flush == Z_NO_FLUSH && retcode == Z_OK && stream.avail_in == 0) ||
        (flush == Z_FINISH   && retcode == Z_STREAM_END))
    {
      break;
    }

    assert(stream.avail_out == 0);
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
    result.push_back(next_cut);
  }

  return result;
}


bool FileScrubbingTask::IsLastBuffer() const {
  const CharBuffer *buffer = FileScrubbingTask::buffer();
  return file()->size() == buffer->base_offset() + buffer->used_bytes();
}

}  // namespace upload
