/**
 * This file is part of the CernVM File System.
 */

#include "ingestion/task_read.h"

void TaskRead::Process(FileItem *item) {
  FileItem *next_item;

  while (true) {
    next_item = tube_->Pop();

    delete next_item;
  }
}
