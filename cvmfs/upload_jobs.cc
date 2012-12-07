/**
 * This file is part of the CernVM File System.
 */

#include "upload_jobs.h"

#include "upload.h"

using namespace upload;

void Job::Done(const int return_code) {
  return_code_ = return_code;
  delegate_->JobFinishedCallback(this);
}
