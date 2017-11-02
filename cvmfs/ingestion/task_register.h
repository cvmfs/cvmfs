/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_TASK_REGISTER_H_
#define CVMFS_INGESTION_TASK_REGISTER_H_

#include "ingestion/item.h"
#include "ingestion/tube.h"
#include "ingestion/task.h"

/**
 * Runs the callback to register processed files in the file catalog.
 * Encapsulated in a task so that only a single thread operates on the file
 * catalog, which is serialized anyway.
 */
class TaskRegister : public TubeConsumer<FileItem> {
 public:
  TaskRegister(Tube<FileItem> *tube_in, Tube<FileItem> *tube_counter)
    : TubeConsumer<FileItem>(tube_in)
    , tube_counter_(tube_counter)
  { }

 protected:
  virtual void Process(FileItem *processed_file);

 private:
  Tube<FileItem> *tube_counter_;
};  // class TaskRegister

#endif  // CVMFS_INGESTION_TASK_REGISTER_H_
