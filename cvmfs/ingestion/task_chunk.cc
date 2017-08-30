/**
 * This file is part of the CernVM File System.
 */

#include <unistd.h>

#include <cassert>

#include "ingestion/task_chunk.h"

/**
 * The tags from the read stage in the pipeline and the tags given in the
 * chunking stage can safely overlap.  Nevertheless, debugging might be easier
 * if they don't.  So let's start with a high number.
 */
atomic_int64 TaskChunk::tag_seq_ = 2LLU << 32;

/**
 * Consumes the stream of input block and produces new output blocks according
 * to cut marks.  The input blocks correspond to chunks.
 */
void TaskChunk::Process(BlockItem *input_block) {
  FileItem *file_item = input_block->file_item();
  int64_t input_tag = input_block->tag();
  assert((file_item != NULL) && (input_tag >= 0));

  ChunkInfo chunk_info;
  TagMap::const_iterator iter = tag_map_.find(input_tag);
  if (iter == tag_map_.end()) {
    chunk_info = ChunkInfo(atomic_xadd64(&tag_seq_, 1), 0);
  } else {
    chunk_info = iter->second;
  }

  ChunkItem *chunk_item = new ChunkItem(file_item, chunk_info.offset);

  BlockItem *output_block = new BlockItem(chunk_info.output_tag);
  output_block->SetFileItem(file_item);
  output_block->SetChunkItem(chunk_item);
  switch (input_block->type()) {
    case BlockItem::kBlockStop:
      output_block->MakeStop();
      break;
    case BlockItem::kBlockData:
      output_block->MakeData(input_block->data(), input_block->size());
      chunk_info.offset += input_block->size();
      input_block->Discharge();
      break;
    default:
      abort();
  }

  tag_map_[input_tag] = chunk_info;
  delete input_block;
  tubes_out_->Dispatch(output_block);
}
