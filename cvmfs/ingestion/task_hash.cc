/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "task_hash.h"

#include "hash.h"


void TaskHash::Process(BlockItem *input_block) {
  assert(input_block->chunk_item() != NULL);
}
