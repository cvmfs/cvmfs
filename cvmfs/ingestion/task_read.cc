/**
 * This file is part of the CernVM File System.
 */

#include "ingestion/task_read.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include <cstring>

#include "backoff.h"
#include "logging.h"
#include "platform.h"
#include "smalloc.h"
#include "util/posix.h"


atomic_int64 TaskRead::tag_seq_ = 0;


void TaskRead::Process(FileItem *item) {
  BackoffThrottle throttle(kThrottleInitMs, kThrottleMaxMs, kThrottleResetMs);
  if ((high_watermark_ > 0) && (BlockItem::managed_bytes() > high_watermark_)) {
    atomic_inc64(&n_block_);
    do {
      throttle.Throttle();
    } while (BlockItem::managed_bytes() > low_watermark_);
  }

  int fd = -1;
  fd = open(item->path().c_str(), O_RDONLY);
  if (fd < 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to open %s (%d)",
             item->path().c_str(), errno);
    abort();
  }
  platform_stat64 info;
  int retval = platform_fstat(fd, &info);
  if (retval != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to fstat %s (%d)",
             item->path().c_str(), errno);
    abort();
  }
  item->set_size(info.st_size);

  if (item->may_have_chunks()) {
    item->set_may_have_chunks(
      item->chunk_detector()->MightFindChunks(item->size()));
  }

  unsigned char *buffer[kBlockSize];
  uint64_t tag = atomic_xadd64(&tag_seq_, 1);
  ssize_t nbytes = -1;
  unsigned cnt = 0;
  do {
    nbytes = SafeRead(fd, buffer, kBlockSize);
    if (nbytes < 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to read %s (%d)",
               item->path().c_str(), errno);
      abort();
    }

    BlockItem *block_item = new BlockItem(tag, allocator_);
    block_item->SetFileItem(item);
    if (nbytes == 0) {
      block_item->MakeStop();
    } else {
      block_item->MakeDataCopy(
        reinterpret_cast<unsigned char *>(buffer), nbytes);
    }
    tubes_out_->Dispatch(block_item);

    cnt++;
    if ((cnt % 32) == 0) {
      if ((high_watermark_ > 0) &&
          (BlockItem::managed_bytes() > high_watermark_))
      {
        throttle.Throttle();
      }
    }
  } while (nbytes > 0);

  close(fd);
}


void TaskRead::SetWatermarks(uint64_t low, uint64_t high) {
  assert(high > low);
  assert(low > 0);
  low_watermark_ = low;
  high_watermark_ = high;
}
