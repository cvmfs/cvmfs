/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "pipeline.h"

#include "ingestion/task_read.h"

IngestionPipeline::IngestionPipeline() : spawned_(false) {


  tasks_read_.TakeConsumer(new TaskRead(&input_, &tubes_chunk_));
}
