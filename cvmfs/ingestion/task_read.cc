/**
 * This file is part of the CernVM File System.
 */

#include "ingestion/task_read.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include <cstring>

#include "logging.h"
#include "smalloc.h"
#include "util/posix.h"


void TaskRead::Process(FileItem *item) {
  int fd = -1;
  unsigned char *buffer[kBlockSize];
  BlockItem *block_next = new BlockItem();
  BlockItem *block_current = NULL;

  fd = open(item->path().c_str(), O_RDONLY);
  if (fd < 0) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to open %s (%d)",
             item->path().c_str(), errno);
    abort();
  }

  ssize_t nbytes = -1;
  do {
    nbytes = SafeRead(fd, buffer, kBlockSize);
    if (nbytes < 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to read %s (%d)",
               item->path().c_str(), errno);
      abort();
    }

    if (nbytes == 0) {
      block_next->MakeStop();
    } else {
      block_current = block_next;
      // TODO(jblomer): block on a maximum number of bytes in flight
      block_next = new BlockItem();

      unsigned char *data_part =
        reinterpret_cast<unsigned char *>(smalloc(nbytes));
      memcpy(data_part, buffer, nbytes);
      block_current->MakeData(data_part, nbytes, block_next);
      if (nbytes < kBlockSize) {
        block_next->MakeStop();
      }
      block_current->Progress(tube_out_);
    }
  } while (nbytes == kBlockSize);
  block_next->Progress(tube_out_);

  close(fd);
}
