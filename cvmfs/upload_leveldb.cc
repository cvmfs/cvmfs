/**
 * This file is part of the CernVM File System.
 */

#include "upload_leveldb.h"

#include "options.h"
#include "util/posix.h"
#include "util/string.h"

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


bool LevelDbUploader::Initialize() {
  const SpoolerDefinition  &sd          = spooler_definition();
  const std::string         config_path = sd.spooler_configuration;

  return ParseConfiguration(config_path) && AbstractUploader::Initialize();
}


bool LevelDbUploader::ParseConfiguration(const std::string &config_path) {
  if (!FileExists(config_path)) {
    LogCvmfs(kLogUploadLevelDb, kLogStderr,
             "LevelDB uploader config file not found at '%s'",
             config_path.c_str());
    return false;
  }

  BashOptionsManager options_manager;
  options_manager.ParsePath(config_path, false);

  if (!options_manager.GetValue("CVMFS_LEVELDB_STORAGE", &base_path_)) {
    LogCvmfs(kLogUploadLevelDb, kLogStderr,
             "Failed to parse CVMFS_LEVELDB_STORAGE from '%s'",
             config_path.c_str());
    return false;
  }

  std::string leveldb_count_str;
  if (!options_manager.GetValue("CVMFS_LEVELDB_COUNT", &leveldb_count_str)) {
    LogCvmfs(kLogUploadLevelDb, kLogStderr,
             "Failed to parse CVMFS_LEVELDB_COUNT from '%s'",
             config_path.c_str());
    return false;
  }

  database_count_ = String2Uint64(leveldb_count_str);
  if (database_count_ < 1 || database_count_ > 255) {
    LogCvmfs(kLogUploadLevelDb, kLogStderr,
             "Fail, invalid CVMFS_LEVELDB_COUNT given: '%s'.",
             leveldb_count_str.c_str());
    LogCvmfs(kLogUploadLevelDb, kLogStderr,
             "CVMFS_LEVELDB_COUNT should be in range 1-255.");
    return false;
  }

  return true;
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
