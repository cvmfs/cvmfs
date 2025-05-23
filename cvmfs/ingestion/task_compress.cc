/**
 * This file is part of the CernVM File System.
 */


#include "task_compress.h"

#include <cstdlib>

#include "compression/compression.h"
#include "util/logging.h"
#include "util/smalloc.h"


/**
 * The data payload of the blocks is replaced by their compressed counterparts.
 * The block tags stay the same.
 * TODO(jblomer): avoid memory copy with EchoCompressor
 */
void TaskCompress::Process(BlockItem *input_block) {
  assert(input_block->chunk_item() != NULL);

  zlib::Compressor *compressor = input_block->chunk_item()->GetCompressor();
  const int64_t tag = input_block->tag();
  const bool flush = input_block->type() == BlockItem::kBlockStop;
  unsigned char *input_data = input_block->data();
  size_t remaining_in_input = input_block->size();

  BlockItem *output_block = NULL;
  if (!tag_map_.Lookup(tag, &output_block)) {
    // So far unseen chunk, start new stream of compressed blocks
    output_block = new BlockItem(tag, allocator_);
    output_block->SetFileItem(input_block->file_item());
    output_block->SetChunkItem(input_block->chunk_item());
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
      tubes_out_->Dispatch(output_block);
      output_block = new BlockItem(tag, allocator_);
      output_block->SetFileItem(input_block->file_item());
      output_block->SetChunkItem(input_block->chunk_item());
      output_block->MakeData(kCompressedBlockSize);
      tag_map_.Insert(tag, output_block);
    }
  } while ((remaining_in_input > 0) || (flush && !done));

  if (flush) {
    input_block->chunk_item()->ReleaseCompressor();

    if (output_block->size() > 0)
      tubes_out_->Dispatch(output_block);
    else
      delete output_block;
    tag_map_.Erase(tag);

    BlockItem *stop_block = new BlockItem(tag, allocator_);
    stop_block->MakeStop();
    stop_block->SetFileItem(input_block->file_item());
    stop_block->SetChunkItem(input_block->chunk_item());
    tubes_out_->Dispatch(stop_block);
  }

  delete input_block;
}
