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
  TaskWrite(Tube<BlockItem> *tube_in,
            TubeGroup<FileItem> *tubes_out,
            upload::AbstractUploader *uploader)
      : TubeConsumer<BlockItem>(tube_in)
      , tubes_out_(tubes_out)
      , uploader_(uploader) { }

 protected:
  virtual void Process(BlockItem *input_block);

 private:
  void OnBlockComplete(const upload::UploaderResults &results,
                       BlockItem *block_item);
  void OnChunkComplete(const upload::UploaderResults &results,
                       ChunkItem *chunk_item);

  TubeGroup<FileItem> *tubes_out_;
  upload::AbstractUploader *uploader_;
};

#endif  // CVMFS_INGESTION_TASK_WRITE_H_
