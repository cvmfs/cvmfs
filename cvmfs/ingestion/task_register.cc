/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "task_register.h"

void TaskRegister::Process(FileItem *processed_file) {
  // TODO(jblomer) run callback
  delete processed_file;
}
