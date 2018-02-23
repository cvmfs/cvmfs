/**
 * This file is part of the CernVM File System.
 */

#include "ingestion/task_read.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include <cstring>

#include "logging.h"
#include "platform.h"
#include "smalloc.h"
#include "util/posix.h"


atomic_int64 TaskRead::tag_seq_ = 0;


void TaskRead::Process(FileItem *item) {
  if ((high_watermark_ > 0) && (BlockItem::managed_bytes() > high_watermark_)) {
    atomic_inc64(&n_block_);
    do {
      SafeSleepMs(kBusyWaitMs);
    } while (BlockItem::managed_bytes() > low_watermark_);
  }

  if (item->Open() == false) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to open %s (%d)",
             item->path().c_str(), errno);
    abort();
  }
  uint64_t size;
  if (item->GetSize(&size) == false) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to fstat %s (%d)",
             item->path().c_str(), errno);
    abort();
  }
  item->set_size(size);

  if (item->may_have_chunks()) {
    item->set_may_have_chunks(
      item->chunk_detector()->MightFindChunks(item->size()));
  }

  unsigned char *buffer[kBlockSize];
  uint64_t tag = atomic_xadd64(&tag_seq_, 1);
  ssize_t nbytes = -1;
  do {
    nbytes = item->Read(buffer, kBlockSize);
    if (nbytes < 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to read --- %s (%d)",
               item->path().c_str(), errno);
      abort();
    }

    BlockItem *block_item = new BlockItem(tag);
    block_item->SetFileItem(item);
    if (nbytes == 0) {
      block_item->MakeStop();
    } else {
      unsigned char *data_part =
        reinterpret_cast<unsigned char *>(smalloc(nbytes));
      memcpy(data_part, buffer, nbytes);
      block_item->MakeData(data_part, nbytes);
    }
    tubes_out_->Dispatch(block_item);
  } while (nbytes > 0);

  item->Close();
}


void TaskRead::SetWatermarks(uint64_t low, uint64_t high) {
  assert(high > low);
  assert(low > 0);
  low_watermark_ = low;
  high_watermark_ = high;
}
