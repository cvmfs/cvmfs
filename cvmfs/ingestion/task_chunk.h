/**
 * This file is part of the CernVM File System.
 */

#include "ingestion/item.h"
#include "ingestion/task.h"
#include "ingestion/tube.h"
#include "util/posix.h"

class TaskChunk : public TubeConsumer<BlockItem> {
 public:
  TaskChunk(Tube<BlockItem> *tube_in, Tube<BlockItem> *tube_out)
    : TubeConsumer<BlockItem>(tube_in), tube_out_(tube_out) { }

 protected:
  virtual void Process(BlockItem *item);

 private:
  Tube<BlockItem> *tube_out_;
};
