/**
 * This file is part of the CernVM File System.
 */


#include "task_compress.h"

#include <cstdlib>

#include "compression/compression.h"
#include "crypto/hash.h"
#include "ingestion/item.h"
#include "util/logging.h"
#include "util/smalloc.h"


/**
 * The data payload of the blocks is replaced by their compressed counterparts.
 * The block tags stay the same.
 *
 * Hashing is performed inline on each output block immediately before it is
 * dispatched to the write stage, eliminating the separate TaskHash stage and
 * the inter-thread tube round-trip it required.
 *
 * When the compressor is a pass-through (kNoCompression / EchoCompressor), the
 * input block is hashed and forwarded directly — no intermediate buffer is
 * allocated and no memcpy is performed.
 */
void TaskCompress::Process(BlockItem *input_block) {
  assert(input_block->chunk_item() != NULL);

  ChunkItem *chunk_item = input_block->chunk_item();
  zlib::Compressor *compressor = chunk_item->GetCompressor();
  const int64_t tag = input_block->tag();
  const bool flush = input_block->type() == BlockItem::kBlockStop;

  // ── Passthrough fast path (kNoCompression) ──────────────────────────────
  // Hash the raw input bytes and forward the block as-is with zero copies.
  if (compressor->IsPassthrough()) {
    if (input_block->type() == BlockItem::kBlockData) {
      shash::Update(input_block->data(), input_block->size(),
                    chunk_item->hash_ctx());
      tubes_out_->Dispatch(input_block);
    } else {
      // kBlockStop: no data — finalize hash, release compressor, send stop.
      chunk_item->ReleaseCompressor();
      shash::Final(chunk_item->hash_ctx(), chunk_item->hash_ptr());

      BlockItem *stop_block = new BlockItem(tag, allocator_);
      stop_block->MakeStop();
      stop_block->SetFileItem(input_block->file_item());
      stop_block->SetChunkItem(chunk_item);
      tubes_out_->Dispatch(stop_block);
      delete input_block;
    }
    return;
  }

  // ── Compression path (zlib or other) ────────────────────────────────────
  unsigned char *input_data = input_block->data();
  size_t remaining_in_input = input_block->size();

  BlockItem *output_block = NULL;
  if (!tag_map_.Lookup(tag, &output_block)) {
    // So far unseen chunk, start new stream of compressed blocks
    output_block = new BlockItem(tag, allocator_);
    output_block->SetFileItem(input_block->file_item());
    output_block->SetChunkItem(chunk_item);
    output_block->MakeData(kCompressedBlockSize);
    tag_map_.Insert(tag, output_block);
  }

  bool done = false;
  do {
    unsigned char *output_data = output_block->data() + output_block->size();
    assert(!output_block->IsFull());
    size_t remaining_in_output = output_block->capacity()
                                 - output_block->size();

    done = compressor->Deflate(flush, &input_data, &remaining_in_input,
                               &output_data, &remaining_in_output);
    // remaining_in_output is now number of consumed bytes
    output_block->set_size(output_block->size() + remaining_in_output);

    if (output_block->IsFull()) {
      // Hash this block's compressed bytes before sending it downstream.
      shash::Update(output_block->data(), output_block->size(),
                    chunk_item->hash_ctx());
      tubes_out_->Dispatch(output_block);
      output_block = new BlockItem(tag, allocator_);
      output_block->SetFileItem(input_block->file_item());
      output_block->SetChunkItem(chunk_item);
      output_block->MakeData(kCompressedBlockSize);
      tag_map_.Insert(tag, output_block);
    }
  } while ((remaining_in_input > 0) || (flush && !done));

  if (flush) {
    chunk_item->ReleaseCompressor();

    if (output_block->size() > 0) {
      // Hash the final (partial) output block before sending it downstream.
      shash::Update(output_block->data(), output_block->size(),
                    chunk_item->hash_ctx());
      tubes_out_->Dispatch(output_block);
    } else {
      delete output_block;
    }

    // All compressed bytes have been hashed; finalize the digest.
    shash::Final(chunk_item->hash_ctx(), chunk_item->hash_ptr());
    tag_map_.Erase(tag);

    BlockItem *stop_block = new BlockItem(tag, allocator_);
    stop_block->MakeStop();
    stop_block->SetFileItem(input_block->file_item());
    stop_block->SetChunkItem(chunk_item);
    tubes_out_->Dispatch(stop_block);
  }

  delete input_block;
}
