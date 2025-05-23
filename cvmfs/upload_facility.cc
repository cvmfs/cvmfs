/**
 * This file is part of the CernVM File System.
 */

#include "upload_facility.h"

#include <cassert>

#include "upload_gateway.h"
#include "upload_local.h"
#include "upload_s3.h"
#include "util/exception.h"

namespace upload {

atomic_int64 UploadStreamHandle::g_upload_stream_tag = 0;

AbstractUploader::UploadJob::UploadJob(UploadStreamHandle *handle,
                                       UploadBuffer buffer,
                                       const CallbackTN *callback)
    : type(Upload)
    , stream_handle(handle)
    , tag_(handle->tag)
    , buffer(buffer)
    , callback(callback) { }

AbstractUploader::UploadJob::UploadJob(UploadStreamHandle *handle,
                                       const shash::Any &content_hash)
    : type(Commit)
    , stream_handle(handle)
    , tag_(handle->tag)
    , buffer()
    , callback(NULL)
    , content_hash(content_hash) { }

void AbstractUploader::RegisterPlugins() {
  RegisterPlugin<LocalUploader>();
  RegisterPlugin<S3Uploader>();
  RegisterPlugin<GatewayUploader>();
}

AbstractUploader::AbstractUploader(const SpoolerDefinition &spooler_definition)
    : spooler_definition_(spooler_definition)
    , num_upload_tasks_(spooler_definition.num_upload_tasks)
    , jobs_in_flight_(spooler_definition.number_of_concurrent_uploads) { }


bool AbstractUploader::Initialize() {
  for (unsigned i = 0; i < GetNumTasks(); ++i) {
    Tube<UploadJob> *t = new Tube<UploadJob>();
    tubes_upload_.TakeTube(t);
    tasks_upload_.TakeConsumer(new TaskUpload(this, t));
  }
  tubes_upload_.Activate();
  tasks_upload_.Spawn();
  return true;
}

bool AbstractUploader::FinalizeSession(bool /*commit*/,
                                       const std::string & /*old_root_hash*/,
                                       const std::string & /*new_root_hash*/,
                                       const RepositoryTag & /*tag*/) {
  return true;
}


int AbstractUploader::CreateAndOpenTemporaryChunkFile(std::string *path) const {
  const std::string tmp_path = CreateTempPath(
      spooler_definition_.temporary_path + "/" + "chunk", 0644);
  if (tmp_path.empty()) {
    LogCvmfs(kLogSpooler, kLogStderr,
             "Failed to create temp file in %s for upload of file chunk"
             " (errno: %d).",
             spooler_definition_.temporary_path.c_str(), errno);
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

void AbstractUploader::TearDown() { tasks_upload_.Terminate(); }

void AbstractUploader::WaitForUpload() const { jobs_in_flight_.WaitForZero(); }

void AbstractUploader::InitCounters(perf::StatisticsTemplate *statistics) {
  counters_ = new UploadCounters(*statistics);
}

void AbstractUploader::CountUploadedChunks() const {
  if (counters_.IsValid()) {
    perf::Inc(counters_->n_chunks_added);
  }
}

void AbstractUploader::DecUploadedChunks() const {
  if (counters_.IsValid()) {
    perf::Dec(counters_->n_chunks_added);
  }
}

void AbstractUploader::CountUploadedBytes(int64_t bytes_written) const {
  if (counters_.IsValid()) {
    perf::Xadd(counters_->sz_uploaded_bytes, bytes_written);
  }
}

void AbstractUploader::CountDuplicates() const {
  if (counters_.IsValid()) {
    perf::Inc(counters_->n_chunks_duplicated);
  }
}

void AbstractUploader::CountUploadedCatalogs() const {
  if (counters_.IsValid()) {
    perf::Inc(counters_->n_catalogs_added);
  }
}

void AbstractUploader::CountUploadedCatalogBytes(int64_t bytes_written) const {
  if (counters_.IsValid()) {
    perf::Xadd(counters_->sz_uploaded_catalog_bytes, bytes_written);
  }
}

//------------------------------------------------------------------------------


void TaskUpload::Process(AbstractUploader::UploadJob *upload_job) {
  switch (upload_job->type) {
    case AbstractUploader::UploadJob::Upload:
      uploader_->StreamedUpload(upload_job->stream_handle, upload_job->buffer,
                                upload_job->callback);
      break;

    case AbstractUploader::UploadJob::Commit:
      uploader_->FinalizeStreamedUpload(upload_job->stream_handle,
                                        upload_job->content_hash);
      break;

    default:
      PANIC(NULL);
  }

  delete upload_job;
}

}  // namespace upload
