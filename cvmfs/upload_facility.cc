/**
 * This file is part of the CernVM File System.
 */

#include "upload_facility.h"

#include "upload_local.h"
#include "upload_s3.h"
#include "util.h"

using namespace upload;

void AbstractUploader::RegisterPlugins() {
  RegisterPlugin<LocalUploader>();
  RegisterPlugin<S3Uploader>();
}


AbstractUploader::AbstractUploader(const SpoolerDefinition& spooler_definition) :
  spooler_definition_(spooler_definition),
  torn_down_(false),
  jobs_in_flight_(spooler_definition.number_of_threads * 100) {}


bool AbstractUploader::Initialize() {
  // late initialization of the writer_thread_ field. This is necessary, since
  // AbstractUploader::WriteThread is pure virtual and relies on a concrete sub-
  // class being initialized before the writer_thread_ starts running
  tbb::tbb_thread thread(&ThreadProxy<AbstractUploader>,
                          this,
                         &AbstractUploader::WriteThread);

  // tbb::tbb_thread assignment operator 'moves' the thread handle so _does not_
  // 'copy' the operating system thread construct. The assertions check for this
  // behaviour.
  assert (! writer_thread_.joinable());
  writer_thread_ = thread;
  assert (writer_thread_.joinable());
  assert (! thread.joinable());

  // wait for the thread to call back...
  return thread_started_executing_.Get();
}


void AbstractUploader::TearDown() {
  assert (! torn_down_);
  upload_queue_.push(UploadJob()); // Termination signal
  writer_thread_.join();
  torn_down_ = true;
}


void AbstractUploader::WaitForUpload() const {
  jobs_in_flight_.WaitForZero();
}
