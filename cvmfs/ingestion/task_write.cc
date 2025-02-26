/**
 * This file is part of the CernVM File System.
 */


#include "task_write.h"

#include <cstdlib>

#include "upload_facility.h"
#include "util/exception.h"
#include "util/logging.h"


void TaskWrite::OnBlockComplete(
  const upload::UploaderResults &results,
  BlockItem *block_item)
{
  if (results.return_code != 0) {
    PANIC(kLogStderr, "block upload failed (code: %d)", results.return_code);
  }

  delete block_item;
}


void TaskWrite::OnChunkComplete(
  const upload::UploaderResults &results,
  ChunkItem *chunk_item)
{
  if (results.return_code != 0) {
    PANIC(kLogStderr, "chunk upload failed (code: %d)", results.return_code);
  }

  FileItem *file_item = chunk_item->file_item();
  file_item->RegisterChunk(FileChunk(*chunk_item->hash_ptr(),
                                     chunk_item->offset(),
                                     chunk_item->size()));
  delete chunk_item;

  if (file_item->IsProcessed()) {
    tubes_out_->DispatchAny(file_item);
  }
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
      // If there is a sole piece and a legacy bulk chunk, two times the same
      // chunk is being uploaded.  Well.  It doesn't hurt.
      if (chunk_item->IsSolePiece()) {
        chunk_item->MakeBulkChunk();
      }
      uploader_->ScheduleCommit(handle, *chunk_item->hash_ptr());
      delete input_block;
      break;
    default:
      PANIC(NULL);
  }
}
