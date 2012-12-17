// /**
//  * This file is part of the CernVM File System.
//  */

// #include "upload_jobs.h"

// #include "upload.h"

// using namespace upload;

// void Job::Done(const int return_code) {
//   // the job is done... inform the spooler asynchronously.
//   // Keep in mind: This is called from a PushWorker thread and might
//   // produce races and other nasty threading issues.
//   return_code_ = return_code;
//   delegate_->JobFinishedCallback(this);
// }
