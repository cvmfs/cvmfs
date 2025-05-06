/**
 * This file is part of the CernVM File System.
 */


#include "task_register.h"

#include <cassert>

#include "util/logging.h"

void TaskRegister::Process(FileItem *file_item) {
  assert(file_item != NULL);
  assert(!file_item->path().empty());
  assert(!file_item->has_legacy_bulk_chunk() ||
         !file_item->bulk_hash().IsNull());
  assert(file_item->nchunks_in_fly() == 0);
  assert((file_item->GetNumChunks() > 1) || !file_item->bulk_hash().IsNull());
  assert(file_item->GetNumChunks() != 1);
  assert(file_item->hash_suffix() == file_item->bulk_hash().suffix);
  assert(file_item->bulk_hash().algorithm == file_item->hash_algorithm());

  LogCvmfs(kLogSpooler, kLogVerboseMsg,
           "File '%s' processed (bulk hash: %s suffix: %c)",
           file_item->path().c_str(),
           file_item->bulk_hash().ToString().c_str(),
           file_item->hash_suffix());

  tube_ctr_inflight_pre_->PopFront();

  NotifyListeners(upload::SpoolerResult(0,
    file_item->path(),
    file_item->bulk_hash(),
    FileChunkList(*file_item->GetChunksPtr()),
    file_item->compression_algorithm()));

  delete file_item;

  tube_ctr_inflight_post_->PopFront();
}
