/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_PIPELINE_H_
#define CVMFS_INGESTION_PIPELINE_H_

#include "ingestion/item.h"
#include "ingestion/task.h"
#include "ingestion/tube.h"


class IngestionPipeline {
 public:
  IngestionPipeline();

  void Spawn();

 private:
  bool spawned_;
  Tube<FileItem> input_;

  TubeConsumerGroup<FileItem> tasks_read_;
  TubeGroup<BlockItem> tubes_chunk_;

  TubeConsumerGroup<BlockItem> tasks_write_;
};  // class IngestionPipeline

#endif  // CVMFS_INGESTION_PIPELINE_H_
