/**
 * This file is part of the CernVM File System.
 */

#include "ingestion/task_read.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include <cstring>

#include "backoff.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"
#include "util/smalloc.h"


atomic_int64 TaskRead::tag_seq_ = 0;


void TaskRead::Process(FileItem *item) {
  BackoffThrottle throttle(kThrottleInitMs, kThrottleMaxMs, kThrottleResetMs);
  if ((high_watermark_ > 0) && (BlockItem::managed_bytes() > high_watermark_)) {
    atomic_inc64(&n_block_);
    do {
      throttle.Throttle();
    } while (BlockItem::managed_bytes() > low_watermark_);
  }

  if (item->Open() == false) {
    PANIC(kLogStderr, "failed to open %s (%d)", item->path().c_str(), errno);
  }
  uint64_t size;
  if (item->GetSize(&size) == false) {
    PANIC(kLogStderr, "failed to fstat %s (%d)", item->path().c_str(), errno);
  }
  item->set_size(size);

  if (item->may_have_chunks()) {
    item->set_may_have_chunks(
        item->chunk_detector()->MightFindChunks(item->size()));
  }

  unsigned char *buffer[kBlockSize];
  const uint64_t tag = atomic_xadd64(&tag_seq_, 1);
  ssize_t nbytes = -1;
  unsigned cnt = 0;
  do {
    nbytes = item->Read(buffer, kBlockSize);
    if (nbytes < 0) {
      PANIC(kLogStderr, "failed to read %s (%d)", item->path().c_str(), errno);
    }

    BlockItem *block_item = new BlockItem(tag, allocator_);
    block_item->SetFileItem(item);
    if (nbytes == 0) {
      item->Close();
      block_item->MakeStop();
    } else {
      block_item->MakeDataCopy(reinterpret_cast<unsigned char *>(buffer),
                               nbytes);
    }
    tubes_out_->Dispatch(block_item);

    cnt++;
    if ((cnt % 32) == 0) {
      if ((high_watermark_ > 0)
          && (BlockItem::managed_bytes() > high_watermark_)) {
        throttle.Throttle();
      }
    }
  } while (nbytes > 0);
}


void TaskRead::SetWatermarks(uint64_t low, uint64_t high) {
  assert(high > low);
  assert(low > 0);
  low_watermark_ = low;
  high_watermark_ = high;
}
