/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "task_compress.h"

#include <cstdlib>

#include "compression/compression.h"
#include "compression/input_mem.h"
#include "network/sink_mem.h"
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
  zlib::InputMem in_comp(input_block->data(), input_block->size());

  BlockItem *output_block = NULL;
  if (!tag_map_.Lookup(tag, &output_block)) {
    // So far unseen chunk, start new stream of compressed blocks
    output_block = new BlockItem(tag, allocator_);
    output_block->SetFileItem(input_block->file_item());
    output_block->SetChunkItem(input_block->chunk_item());
    output_block->MakeData(kCompressedBlockSize);
    tag_map_.Insert(tag, output_block);
  }

  cvmfs::MemSink out_comp;
  zlib::StreamStates ret_compress;
  do {
    assert(!output_block->IsFull());
    out_comp.Adopt(output_block->capacity(), output_block->size(),
                   output_block->data(), false);

    ret_compress = compressor->CompressStream(&in_comp, &out_comp, flush);
    output_block->set_size(out_comp.pos());

    if (ret_compress == zlib::kStreamOutBufFull) {
      assert(output_block->IsFull());
      tubes_out_->Dispatch(output_block);
      output_block = new BlockItem(tag, allocator_);
      output_block->SetFileItem(input_block->file_item());
      output_block->SetChunkItem(input_block->chunk_item());
      output_block->MakeData(kCompressedBlockSize);
      tag_map_.Insert(tag, output_block);
    }
  } while (ret_compress != zlib::kStreamEnd && ret_compress != zlib::kStreamContinue);

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
