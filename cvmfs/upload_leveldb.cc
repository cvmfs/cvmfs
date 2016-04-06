/**
 * This file is part of the CernVM File System.
 */

#include "upload_leveldb.h"

namespace upload {

LevelDbUploader::LevelDbUploader(const SpoolerDefinition &spooler_definition) :
  AbstractUploader(spooler_definition)
{
  assert(spooler_definition.IsValid() &&
         spooler_definition.driver_type == SpoolerDefinition::LevelDb);
}

bool LevelDbUploader::WillHandle(const SpoolerDefinition &spooler_definition) {
  return spooler_definition.driver_type == SpoolerDefinition::LevelDb;
}


unsigned int LevelDbUploader::GetNumberOfErrors() const {
  return 0;  // TODO(rmeusel): implement me
}


void LevelDbUploader::WorkerThread() {
  bool running = true;

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "LevelDb WorkerThread started.");

  while (running) {
    UploadJob job = AcquireNewJob();
    switch (job.type) {
      case UploadJob::Upload:
        Upload(job.stream_handle,
               job.buffer,
               job.callback);
        break;
      case UploadJob::Commit:
        FinalizeStreamedUpload(job.stream_handle, job.content_hash);
        break;
      case UploadJob::Terminate:
        running = false;
        break;
      default:
        const bool unknown_job_type = false;
        assert(unknown_job_type);
        break;
    }
  }

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "LevelDb WorkerThread exited.");
}


void LevelDbUploader::FileUpload(
  const std::string &local_path,
  const std::string &remote_path,
  const CallbackTN   *callback
) {
  Respond(callback, UploaderResults(1, local_path));
}


UploadStreamHandle* LevelDbUploader::InitStreamedUpload(
                                                   const CallbackTN *callback) {
  return new LevelDbStreamHandle(callback);
}


void LevelDbUploader::Upload(UploadStreamHandle  *handle,
                           CharBuffer          *buffer,
                           const CallbackTN    *callback) {
  Respond(callback, UploaderResults(1, buffer));
}


void LevelDbUploader::FinalizeStreamedUpload(UploadStreamHandle  *handle,
                                           const shash::Any    &content_hash) {
  LevelDbStreamHandle *leveldb_handle =
                                      static_cast<LevelDbStreamHandle*>(handle);
  const CallbackTN *callback = handle->commit_callback;
  delete leveldb_handle;

  Respond(callback, UploaderResults(1));
}


bool LevelDbUploader::Remove(const std::string& file_to_delete) {
  return false;
}


bool LevelDbUploader::Peek(const std::string& path) const {
  return false;
}


bool LevelDbUploader::PlaceBootstrappingShortcut(const shash::Any &object) const {
  return false;
}


}  // namespace upload
