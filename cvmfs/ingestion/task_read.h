/**
 * This file is part of the CernVM File System.
 */

#include "ingestion/item.h"
#include "ingestion/task.h"

class TaskRead : public TubeConsumer<FileItem> {
 protected:
  virtual void Process(FileItem *item);
};
