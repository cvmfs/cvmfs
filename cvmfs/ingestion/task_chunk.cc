/**
 * This file is part of the CernVM File System.
 */

#include "ingestion/task_chunk.h"


void TaskChunk::Process(BlockItem *item) {
  if (item->type() == BlockItem::kBlockStop) {
    delete item;
  }

  assert(item->type() == BlockItem::kBlockData);

}
