/**
 * This file is part of the CernVM File System.
 */

#include "upload_facility.h"

#include <cassert>

#include "upload_gateway.h"
#include "upload_local.h"
#include "upload_s3.h"

namespace upload {

void AbstractUploader::RegisterPlugins() {
  RegisterPlugin<LocalUploader>();
  RegisterPlugin<S3Uploader>();
  RegisterPlugin<GatewayUploader>();
}

AbstractUploader::AbstractUploader(const SpoolerDefinition &spooler_definition)
    : spooler_definition_(spooler_definition),
      torn_down_(false),
      jobs_in_flight_(spooler_definition.number_of_concurrent_uploads) {}

bool AbstractUploader::Initialize() {
  // late initialization of the writer_thread_ field. This is necessary, since
  // AbstractUploader::WriteThread is pure virtual and relies on a concrete sub-
  // class being initialized before the writer_thread_ starts running
  tbb::tbb_thread thread(&ThreadProxy<AbstractUploader>, this,
                         &AbstractUploader::WriteThread);

  // tbb::tbb_thread assignment operator 'moves' the thread handle so _does not_
  // 'copy' the operating system thread construct. The assertions check for this
  // behaviour.
  assert(!writer_thread_.joinable());
  writer_thread_.swap(thread);
  assert(writer_thread_.joinable());
  assert(!thread.joinable());

  // wait for the thread to call back...
  return thread_started_executing_.Get();
}

bool AbstractUploader::FinalizeSession(bool /*commit*/,
                                       const std::string & /*old_catalog*/,
                                       const std::string & /*new_catalog*/) {
  return true;
}

int AbstractUploader::CreateAndOpenTemporaryChunkFile(std::string *path) const {
  const std::string tmp_path =
      CreateTempPath(spooler_definition_.temporary_path + "/" + "chunk", 0644);
  if (tmp_path.empty()) {
    LogCvmfs(kLogSpooler, kLogStderr,
             "Failed to create temp file for upload of file chunk (errno: %d).",
             errno);
    return -1;
  }

  const int tmp_fd = open(tmp_path.c_str(), O_WRONLY);
  if (tmp_fd < 0) {
    LogCvmfs(kLogSpooler, kLogStderr,
             "Failed to open temp file '%s' for upload of file chunk "
             "(errno: %d)",
             tmp_path.c_str(), errno);
    unlink(tmp_path.c_str());
  } else {
    *path = tmp_path;
  }

  return tmp_fd;
}

void AbstractUploader::WorkerThread() {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Uploader WorkerThread started.");
  while (PerformJob() != JobStatus::kTerminate) {
  }
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Uploader WorkerThread exited.");
}

AbstractUploader::JobStatus::State AbstractUploader::DispatchJob(
    const UploadJob &job) {
  switch (job.type) {
    case UploadJob::Upload:
      StreamedUpload(job.stream_handle, job.buffer, job.callback);
      return JobStatus::kOk;

    case UploadJob::Commit:
      FinalizeStreamedUpload(job.stream_handle, job.content_hash);
      return JobStatus::kOk;

    case UploadJob::Terminate:
      return JobStatus::kTerminate;

    default:
      const bool unknown_job_type = false;
      assert(unknown_job_type);
      return JobStatus::kTerminate;
  }
}

void AbstractUploader::TearDown() {
  assert(!torn_down_);
  upload_queue_.push(UploadJob());  // Termination signal
  writer_thread_.join();
  torn_down_ = true;
}

void AbstractUploader::WaitForUpload() const { jobs_in_flight_.WaitForZero(); }

}  // namespace upload
