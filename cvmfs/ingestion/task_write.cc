/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "task_write.h"

#include <cstdlib>

#include "logging.h"


void TaskWrite::OnBlockComplete(
  const upload::UploaderResults &results,
  BlockItem *block_item)
{
  if (results.return_code != 0) {
    LogCvmfs(kLogSpooler, kLogStderr, "block upload failed (code: %d)",
      results.return_code);
    abort();
  }

  delete block_item;
}


void TaskWrite::OnChunkComplete(
  const upload::UploaderResults &results,
  ChunkItem *chunk_item)
{
  /*assert(chunk->IsFullyProcessed());
  assert(chunk->HasUploadStreamHandle());
  assert(chunk->bytes_written() == chunk->compressed_size());

  if (results.return_code != 0) {
    LogCvmfs(kLogSpooler, kLogStderr, "chunk upload failed (code: %d)",
      results.return_code);
    abort();
  }

  chunk->file()->ChunkCommitted(chunk);

  pthread_mutex_lock(&processing_done_mutex_);
  if (--chunks_in_flight_ == 0) {
      pthread_cond_signal(&processing_done_condition_);
  }
  pthread_mutex_unlock(&processing_done_mutex_);*/

  if (results.return_code != 0) {
    LogCvmfs(kLogSpooler, kLogStderr, "chunk upload failed (code: %d)",
             results.return_code);
    abort();
  }

  // register with file (needs to be thread-safe!)
  delete chunk_item;
}


void TaskWrite::Process(BlockItem *input_block) {
  ChunkItem *chunk_item = input_block->chunk_item();

  upload::UploadStreamHandle *handle = chunk_item->upload_handle();
  if (handle == NULL) {
    // The closure passed here, is called by the AbstractUploader as soon as
    // it successfully committed the complete chunk
    handle = uploader_->InitStreamedUpload(
      upload::AbstractUploader::MakeClosure(
        &TaskWrite::OnChunkComplete, this, chunk_item));
    assert(handle != NULL);
    chunk_item->set_upload_handle(handle);
  }

  switch (input_block->type()) {
    case BlockItem::kBlockData:
      uploader_->ScheduleUpload(
        handle,
        upload::AbstractUploader::UploadBuffer(
          input_block->size(), input_block->data()),
        upload::AbstractUploader::MakeClosure(
          &TaskWrite::OnBlockComplete, this, input_block));
      break;
    case BlockItem::kBlockStop:
      // TODO: fixup files with one chunk (which should become bulk chunk)
      // Either promote to bulk chunk or drop if there is a legacy bulk chunk
      uploader_->ScheduleCommit(handle, *chunk_item->hash_ptr());
      break;
    default:
      abort();
  }
}
