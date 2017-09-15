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
  do {
    nbytes = SafeRead(fd, buffer, kBlockSize);
    if (nbytes < 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to read %s (%d)",
               item->path().c_str(), errno);
      abort();
    }

    // TODO(jblomer): block on a maximum number of bytes in flight
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

  close(fd);
}
