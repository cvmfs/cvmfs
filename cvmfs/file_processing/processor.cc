/**
 * This file is part of the CernVM File System.
 */

#include "processor.h"

#include <tbb/parallel_invoke.h>

#include "io_dispatcher.h"
#include "file.h"
#include "chunk.h"

using namespace upload;

tbb::task* ChunkProcessingTask::execute() {
  // Thread Safety:
  //   * Only one ChunkProcessingTask for any given Chunk at any given time
  //   * Parallel execution of more than one ChunkProcessingTask for chunks of
  //     the same File is possible
  //   * Multiple ChunkProcessingTasks might share the same CharBuffer

  assert (chunk_->IsInitialized());
  assert (buffer_->IsInitialized());

  // Get the position from where the current input buffer needs to be processed
  // Two different cases:
  //   -> The beginning of the CharBuffer, if the contained data lies in the
  //      middle of the currently processed Chunk
  //   -> At the position where the applicable data for the processed Chunk
  //      starts (i.e. Chunk's starting offset is in the middle of the buffer)
  const off_t internal_offset =
    std::max(off_t(0), chunk_->offset() - buffer_->base_offset());
  assert (internal_offset >= 0);
  assert (static_cast<size_t>(internal_offset) <= buffer_->used_bytes());
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
  assert (byte_count <= buffer_->used_bytes() - internal_offset);

  // find out, if we are going to process the final block of the chunk
  const bool finalize = (
    (chunk_->IsFullyDefined()) &&
    (buffer_->base_offset() + internal_offset + byte_count
      == chunk_->offset() + chunk_->size())
  );

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
                                 const bool            finalize) const {
  z_stream  &stream   = chunk_->zlib_context();
  SHA_CTX   &sha1_ctx = chunk_->sha1_context();

  // estimate the size needed for the data to compress
  const size_t max_output_size = deflateBound(&stream, bytes);
  CharBuffer *compress_buffer  = new CharBuffer(max_output_size);

  // state the input data in the zlib stream for the next compression step
  stream.avail_in  = bytes;
  stream.next_in   = const_cast<unsigned char*>(data); // sry, but zlib forces me...
  const int flush = (finalize) ? Z_FINISH : Z_NO_FLUSH;

  int retcode = -1;
  while (true) {
    // state the current output buffer in the zlib stream
    stream.avail_out = compress_buffer->size();
    stream.next_out  = compress_buffer->ptr();

    // do the compression step
    retcode = deflate(&stream, flush);
    assert (retcode == Z_OK || retcode == Z_STREAM_END);

    // check if zlib produced any bytes, define the byte offset and size of the
    // produced output data Block, update the running content hash with the
    // fresh compression data and schedule it for writing
    const size_t bytes_produced = compress_buffer->size() - stream.avail_out;
    if (bytes_produced > 0) {
      compress_buffer->SetUsedBytes(bytes_produced);
      compress_buffer->SetBaseOffset(chunk_->compressed_size());
      chunk_->add_compressed_size(bytes_produced);
      const int sha_retcode = SHA1_Update(&sha1_ctx,
                                           compress_buffer->ptr(),
                                           compress_buffer->used_bytes());
      assert (sha_retcode == 1);
      chunk_->ScheduleWrite(compress_buffer);
    }

    // check if the compression for the given input data has finished and stop
    // the compression loop
    if ((flush == Z_NO_FLUSH && retcode == Z_OK) ||
        (flush == Z_FINISH   && retcode == Z_STREAM_END)) {
      break;
    }

    // when the estimated output buffer size was not sufficient we provide 4k
    // fallback buffers until all input data has been consumed
    if (stream.avail_out == 0) {
      compress_buffer = new CharBuffer(4096);
    }
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
      assert (fully_defined_chunk->IsFullyDefined());
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

  set_ref_count(chunks_to_process_.size() + 1); // +1 for the wait
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
  assert (file->MightBecomeChunked());
  assert (current_chunk != NULL);
  assert (current_chunk->size() == 0);
  assert (current_chunk->offset() <= buffer->base_offset());
  assert (current_chunk->offset() <  buffer->base_offset() +
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

