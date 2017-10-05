/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_TASK_READ_H_
#define CVMFS_INGESTION_TASK_READ_H_

#include "atomic.h"
#include "ingestion/item.h"
#include "ingestion/task.h"
#include "ingestion/tube.h"
#include "util/posix.h"

class TaskRead : public TubeConsumer<FileItem> {
 public:
  static const unsigned kBlockSize = kPageSize * 4;

  TaskRead(Tube<FileItem> *tube_in, TubeGroup<BlockItem> *tubes_out)
    : TubeConsumer<FileItem>(tube_in), tubes_out_(tubes_out) { }

 protected:
  virtual void Process(FileItem *item);

 private:
  /**
   * Every new file increases the tag sequence counter that is used to annotate
   * BlockItems.
   */
  static atomic_int64 tag_seq_;

  TubeGroup<BlockItem> *tubes_out_;
};

#endif  // CVMFS_INGESTION_TASK_READ_H_
