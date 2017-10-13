/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_INGESTION_TASK_WRITE_H_
#define CVMFS_INGESTION_TASK_WRITE_H_

#include "ingestion/item.h"
#include "ingestion/task.h"
#include "upload_facility.h"


class TaskWrite : public TubeConsumer<BlockItem> {
 public:
  TaskWrite(Tube<BlockItem> *tube_in, upload::AbstractUploader *uploader)
    : TubeConsumer<BlockItem>(tube_in)
    , uploader_(uploader) { }

 protected:
  virtual void Process(BlockItem *input_block);

 private:
  void OnBlockComplete(const upload::UploaderResults &results,
                       BlockItem *block_item);
  void OnChunkComplete(const upload::UploaderResults &results,
                       ChunkItem *chunk_item);

  upload::AbstractUploader *uploader_;
};

#endif  // CVMFS_INGESTION_TASK_WRITE_H_
