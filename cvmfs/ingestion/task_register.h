/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_TASK_REGISTER_H_
#define CVMFS_INGESTION_TASK_REGISTER_H_

#include "ingestion/item.h"
#include "ingestion/task.h"
#include "upload_spooler_result.h"
#include "util/concurrency.h"
#include "util/tube.h"

/**
 * Runs the callback to register processed files in the file catalog.
 * Encapsulated in a task so that only a single thread operates on the file
 * catalog, which is serialized anyway.
 */
class TaskRegister : public TubeConsumer<FileItem>,
                     public Observable<upload::SpoolerResult> {
 public:
  TaskRegister(Tube<FileItem> *tube_in,
               Tube<FileItem> *tube_ctr_inflight_pre,
               Tube<FileItem> *tube_ctr_inflight_post)
      : TubeConsumer<FileItem>(tube_in)
      , tube_ctr_inflight_pre_(tube_ctr_inflight_pre)
      , tube_ctr_inflight_post_(tube_ctr_inflight_post) { }

 protected:
  virtual void Process(FileItem *file_item);

 private:
  Tube<FileItem> *tube_ctr_inflight_pre_;
  Tube<FileItem> *tube_ctr_inflight_post_;
};  // class TaskRegister

#endif  // CVMFS_INGESTION_TASK_REGISTER_H_
