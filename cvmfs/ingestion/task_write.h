/**
 * This file is part of the CernVM File System.
 */

 #ifndef CVMFS_INGESTION_TASK_WRITE_H_
 #define CVMFS_INGESTION_TASK_WRITE_H_

 #include "ingestion/item.h"
 #include "ingestion/task.h"

class TaskWrite : public TubeConsumer<BlockItem> {
 public:
  TaskWrite(Tube<BlockItem> *tube_in) : TubeConsumer<BlockItem>(tube_in) { }

 protected:
  virtual void Process(BlockItem *input_block);
};

#endif  // CVMFS_INGESTION_TASK_WRITE_H_
