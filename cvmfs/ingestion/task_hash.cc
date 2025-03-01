/**
 * This file is part of the CernVM File System.
 */


#include "task_hash.h"

#include <cstdlib>

#include "crypto/hash.h"
#include "util/exception.h"


void TaskHash::Process(BlockItem *input_block) {
  ChunkItem *chunk = input_block->chunk_item();
  assert(chunk != NULL);

  switch (input_block->type()) {
    case BlockItem::kBlockData:
      shash::Update(input_block->data(), input_block->size(),
                    chunk->hash_ctx());
      break;
    case BlockItem::kBlockStop:
      shash::Final(chunk->hash_ctx(), chunk->hash_ptr());
      break;
    default:
      PANIC(NULL);
  }

  tubes_out_->Dispatch(input_block);
}
