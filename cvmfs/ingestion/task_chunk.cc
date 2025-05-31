/**
 * This file is part of the CernVM File System.
 */

#include "ingestion/task_chunk.h"

#include <unistd.h>

#include <cassert>

#include "util/exception.h"

/**
 * The tags from the read stage in the pipeline and the tags given in the
 * chunking stage can safely overlap.  Nevertheless, debugging might be easier
 * if they don't.  So let's start with a high number.
 */
atomic_int64 TaskChunk::tag_seq_ = 2 << 28;

/**
 * Consumes the stream of input blocks and produces new output blocks according
 * to cut marks.  The output blocks correspond to chunks.
 */
void TaskChunk::Process(BlockItem *input_block) {
  FileItem *file_item = input_block->file_item();
  const int64_t input_tag = input_block->tag();
  assert((file_item != NULL) && (input_tag >= 0));

  ChunkInfo chunk_info;
  // Do we see blocks of the file for the first time?
  if (!tag_map_.Lookup(input_tag, &chunk_info)) {
    // We may have only regular chunks, only a bulk chunk, or both.  We may
    // end up in a situation where we produced only a single non-bulk chunk.
    // This needs to be fixed up later in the pipeline by the write task.
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
      chunk_info.bulk_chunk->set_size(file_item->size());
      chunk_info.output_tag_bulk = atomic_xadd64(&tag_seq_, 1);
    }
    tag_map_.Insert(input_tag, chunk_info);
  }
  assert((chunk_info.bulk_chunk != NULL) || (chunk_info.next_chunk != NULL));

  BlockItem *output_block_bulk = NULL;
  if (chunk_info.bulk_chunk != NULL) {
    output_block_bulk = new BlockItem(chunk_info.output_tag_bulk, allocator_);
    output_block_bulk->SetFileItem(file_item);
    output_block_bulk->SetChunkItem(chunk_info.bulk_chunk);
  }

  ChunkDetector *chunk_detector = file_item->chunk_detector();
  switch (input_block->type()) {
    case BlockItem::kBlockStop:
      // End of the file, no more new chunks
      file_item->set_is_fully_chunked();
      if (output_block_bulk)
        output_block_bulk->MakeStop();
      if (chunk_info.next_chunk != NULL) {
        assert(file_item->size() >= chunk_info.next_chunk->offset());
        chunk_info.next_chunk->set_size(file_item->size()
                                        - chunk_info.next_chunk->offset());
        BlockItem *block_stop = new BlockItem(chunk_info.output_tag_chunk,
                                              allocator_);
        block_stop->SetFileItem(file_item);
        block_stop->SetChunkItem(chunk_info.next_chunk);
        block_stop->MakeStop();
        tubes_out_->Dispatch(block_stop);
      }
      tag_map_.Erase(input_tag);
      break;

    case BlockItem::kBlockData:
      if (output_block_bulk) {
        if (chunk_info.next_chunk != NULL) {
          // Reserve zero-copy for the regular chunk
          output_block_bulk->MakeDataCopy(input_block->data(),
                                          input_block->size());
        } else {
          // There is only the bulk chunk, zero copy
          output_block_bulk->MakeDataMove(input_block);
        }
      }

      if (chunk_info.next_chunk != NULL) {
        unsigned offset_in_block = 0;
        uint64_t cut_mark = 0;
        while ((cut_mark = chunk_detector->FindNextCutMark(input_block)) != 0) {
          assert(cut_mark >= chunk_info.offset + offset_in_block);
          const uint64_t cut_mark_in_block = cut_mark - chunk_info.offset;
          assert(cut_mark_in_block >= offset_in_block);
          assert(cut_mark_in_block <= input_block->size());
          const unsigned tail_size = cut_mark_in_block - offset_in_block;

          if (tail_size > 0) {
            BlockItem *block_tail = new BlockItem(chunk_info.output_tag_chunk,
                                                  allocator_);
            block_tail->SetFileItem(file_item);
            block_tail->SetChunkItem(chunk_info.next_chunk);
            block_tail->MakeDataCopy(input_block->data() + offset_in_block,
                                     tail_size);
            tubes_out_->Dispatch(block_tail);
          }

          assert(cut_mark >= chunk_info.next_chunk->offset());
          // If the cut mark happens to at the end of file, let the final
          // incoming stop block schedule dispatch of the chunk stop block
          if (cut_mark < file_item->size()) {
            chunk_info.next_chunk->set_size(cut_mark
                                            - chunk_info.next_chunk->offset());
            BlockItem *block_stop = new BlockItem(chunk_info.output_tag_chunk,
                                                  allocator_);
            block_stop->SetFileItem(file_item);
            block_stop->SetChunkItem(chunk_info.next_chunk);
            block_stop->MakeStop();
            tubes_out_->Dispatch(block_stop);

            chunk_info.next_chunk = new ChunkItem(file_item, cut_mark);
            chunk_info.output_tag_chunk = atomic_xadd64(&tag_seq_, 1);
          }
          offset_in_block = cut_mark_in_block;
        }
        chunk_info.offset += offset_in_block;

        assert(input_block->size() >= offset_in_block);
        const unsigned tail_size = input_block->size() - offset_in_block;
        if (tail_size > 0) {
          BlockItem *block_tail = new BlockItem(chunk_info.output_tag_chunk,
                                                allocator_);
          block_tail->SetFileItem(file_item);
          block_tail->SetChunkItem(chunk_info.next_chunk);
          block_tail->MakeDataCopy(input_block->data() + offset_in_block,
                                   tail_size);
          tubes_out_->Dispatch(block_tail);
          chunk_info.offset += tail_size;
        }

        // Delete data from incoming block
        input_block->Reset();
      }

      tag_map_.Insert(input_tag, chunk_info);
      break;

    default:
      PANIC(NULL);
  }

  delete input_block;
  if (output_block_bulk)
    tubes_out_->Dispatch(output_block_bulk);
}
