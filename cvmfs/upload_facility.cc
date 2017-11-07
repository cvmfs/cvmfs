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
    jobs_in_flight_(spooler_definition.number_of_concurrent_uploads)
{ }


bool AbstractUploader::Initialize() {
  for (unsigned i = 0; i < GetNumTasks(); ++i) {
    tasks_upload_.TakeConsumer(new TaskUpload(this));
  }
  tasks_upload_.Spawn();
  return true;
}


bool AbstractUploader::FinalizeSession(
  bool /*commit*/,
  const std::string & /*old_root_hash*/,
  const std::string & /*new_root_hash*/)
{
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

void AbstractUploader::TearDown() {
  tasks_upload_.Terminate();
}

void AbstractUploader::WaitForUpload() const { jobs_in_flight_.WaitForZero(); }


//------------------------------------------------------------------------------


void TaskUpload::Process(AbstractUploader::UploadJob *upload_job) {
  switch (upload_job->type) {
    case AbstractUploader::UploadJob::Upload:
      uploader_->StreamedUpload(
        upload_job->stream_handle, upload_job->buffer, upload_job->callback);
      break;

    case AbstractUploader::UploadJob::Commit:
      uploader_->FinalizeStreamedUpload(
        upload_job->stream_handle, upload_job->content_hash);
      break;

    default:
      abort();
  }

  delete upload_job;
}

}  // namespace upload
