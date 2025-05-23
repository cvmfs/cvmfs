/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_TASK_HASH_H_
#define CVMFS_INGESTION_TASK_HASH_H_

#include "ingestion/item.h"
#include "ingestion/task.h"

class TaskHash : public TubeConsumer<BlockItem> {
 public:
  TaskHash(Tube<BlockItem> *tube_in, TubeGroup<BlockItem> *tubes_out)
      : TubeConsumer<BlockItem>(tube_in), tubes_out_(tubes_out) { }

 protected:
  virtual void Process(BlockItem *input_block);

 private:
  TubeGroup<BlockItem> *tubes_out_;
};

#endif  // CVMFS_INGESTION_TASK_HASH_H_
