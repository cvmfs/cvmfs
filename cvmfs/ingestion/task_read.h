/**
 * This file is part of the CernVM File System.
 */

#include "ingestion/item.h"
#include "ingestion/task.h"
#include "ingestion/tube.h"
#include "util/posix.h"

class TaskRead : public TubeConsumer<FileItem> {
 public:
  TaskRead(Tube<FileItem> *tube_in, Tube<BlockItem> *tube_out)
    : TubeConsumer<FileItem>(tube_in), tube_out_(tube_out) { }

 protected:
  virtual void Process(FileItem *item);

 private:
  static const unsigned kBlockSize = kPageSize * 4;

  Tube<BlockItem> *tube_out_;
};
