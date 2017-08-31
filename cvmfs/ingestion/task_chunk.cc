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
atomic_int64 TaskChunk::tag_seq_ = 2 << 28;

/**
 * Consumes the stream of input blocks and produces new output blocks according
 * to cut marks.  The input blocks correspond to chunks.
 */
void TaskChunk::Process(BlockItem *input_block) {
  FileItem *file_item = input_block->file_item();
  int64_t input_tag = input_block->tag();
  assert((file_item != NULL) && (input_tag >= 0));

  ChunkInfo chunk_info;
  // Do we see blocks of the file for the first time?
  TagMap::const_iterator iter_find = tag_map_.find(input_tag);
  if (iter_find == tag_map_.end()) {
    // We may have only regular chunks, only a bulk chunk, or both
    if (file_item->may_have_chunks()) {
      chunk_info.next_chunk = new ChunkItem(file_item, 0);
      chunk_info.output_tag_chunk = atomic_xadd64(&tag_seq_, 1);
      if (file_item->has_legacy_bulk_chunk()) {
        chunk_info.bulk_chunk = new ChunkItem(file_item, 0);
      }
    } else {
      chunk_info.bulk_chunk = new ChunkItem(file_item, 0);
    }

    if (chunk_info.bulk_chunk != NULL) {
      chunk_info.bulk_chunk->MakeBulkChunk();
      chunk_info.output_tag_bulk = atomic_xadd64(&tag_seq_, 1);
    }
    tag_map_[input_tag] = chunk_info;
  } else {
    chunk_info = iter_find->second;
  }
  assert((chunk_info.bulk_chunk != NULL) || (chunk_info.next_chunk != NULL));

  BlockItem *output_block_bulk = NULL;
  if (chunk_info.bulk_chunk != NULL) {
    output_block_bulk = new BlockItem(chunk_info.output_tag_bulk);
    output_block_bulk->SetFileItem(file_item);
    output_block_bulk->SetChunkItem(chunk_info.bulk_chunk);
  }

  ChunkDetector *chunk_detector = file_item->chunk_detector();
  switch (input_block->type()) {
    case BlockItem::kBlockStop:
      if (output_block_bulk) output_block_bulk->MakeStop();
      if (chunk_info.next_chunk != NULL) {
        BlockItem *block_stop = new BlockItem(chunk_info.output_tag_chunk);
        block_stop->SetFileItem(file_item);
        block_stop->SetChunkItem(chunk_info.next_chunk);
        block_stop->MakeStop();
        tubes_out_->Dispatch(block_stop);
      }
      tag_map_.erase(input_tag);
      break;

    case BlockItem::kBlockData:
      if (output_block_bulk) {
        if (chunk_info.next_chunk != NULL) {
          // Reserve zero-copy for the regular chunk
          output_block_bulk->MakeDataCopy(input_block->data(),
                                          input_block->size());
        } else {
          // There is only the bulk chunk, zero copy
          output_block_bulk->MakeData(input_block->data(), input_block->size());
        }
      }

      if (chunk_info.next_chunk != NULL) {
        unsigned offset_in_block = 0;
        uint64_t cut_mark = 0;
        while ((cut_mark = chunk_detector->FindNextCutMark(input_block)) != 0) {
          assert(cut_mark >= chunk_info.offset);
          uint64_t cut_mark_in_block = cut_mark - chunk_info.offset;
          assert(cut_mark_in_block >= offset_in_block);
          unsigned tail_size = cut_mark_in_block - offset_in_block;

          if (tail_size > 0) {
            BlockItem *block_tail = new BlockItem(chunk_info.output_tag_chunk);
            block_tail->SetFileItem(file_item);
            block_tail->SetChunkItem(chunk_info.next_chunk);
            block_tail->MakeDataCopy(input_block->data() + offset_in_block,
                                     tail_size);
            tubes_out_->Dispatch(block_tail);
          }

          BlockItem *block_stop = new BlockItem(chunk_info.output_tag_chunk);
          block_stop->SetFileItem(file_item);
          block_stop->SetChunkItem(chunk_info.next_chunk);
          block_stop->MakeStop();
          tubes_out_->Dispatch(block_stop);

          chunk_info.next_chunk = new ChunkItem(file_item, cut_mark);
          chunk_info.output_tag_chunk = atomic_xadd64(&tag_seq_, 1);
          offset_in_block = cut_mark_in_block;
        }

        if (offset_in_block == 0) {
          // Zero-copy
          output_block_bulk->MakeData(input_block->data(), input_block->size());
        } else {
          assert(input_block->size() >= offset_in_block);
          unsigned tail_size = input_block->size() - offset_in_block;
          if (tail_size > 0) {
            BlockItem *block_tail = new BlockItem(chunk_info.output_tag_chunk);
            block_tail->SetFileItem(file_item);
            block_tail->SetChunkItem(chunk_info.next_chunk);
            block_tail->MakeDataCopy(input_block->data() + offset_in_block,
                                     tail_size);
            tubes_out_->Dispatch(block_tail);
          }
          // Delete data from incoming block
          input_block->Reset();
        }
      }

      chunk_info.offset += input_block->size();
      tag_map_[input_tag] = chunk_info;
      input_block->Discharge();
      break;

    default:
      abort();
  }

  delete input_block;
  if (output_block_bulk) tubes_out_->Dispatch(output_block_bulk);
}
