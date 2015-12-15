/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "upload_local.h"

#include <errno.h>

#include <string>

#include "compression.h"
#include "file_processing/char_buffer.h"
#include "logging.h"
#include "util.h"


namespace upload {

LocalUploader::LocalUploader(const SpoolerDefinition &spooler_definition) :
  AbstractUploader(spooler_definition),
  backend_file_mode_(default_backend_file_mode_ ^ GetUmask()),
  upstream_path_(spooler_definition.spooler_configuration),
  temporary_path_(spooler_definition.temporary_path)
{
  assert(spooler_definition.IsValid() &&
         spooler_definition.driver_type == SpoolerDefinition::Local);

  atomic_init32(&copy_errors_);
}


bool LocalUploader::WillHandle(const SpoolerDefinition &spooler_definition) {
  return spooler_definition.driver_type == SpoolerDefinition::Local;
}


unsigned int LocalUploader::GetNumberOfErrors() const {
  return atomic_read32(&copy_errors_);
}


void LocalUploader::WorkerThread() {
  bool running = true;

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Local WorkerThread started.");

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

  LogCvmfs(kLogSpooler, kLogVerboseMsg, "Local WorkerThread exited.");
}


void LocalUploader::FileUpload(
  const std::string &local_path,
  const std::string &remote_path,
  const CallbackTN   *callback
) {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "FileUpload call started.");

  // create destination in backend storage temporary directory
  std::string tmp_path = CreateTempPath(temporary_path_ + "/upload", 0666);
  if (tmp_path.empty()) {
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "failed to create temp path for "
                                          "upload of file '%s' (errno: %d)",
             local_path.c_str(), errno);
    atomic_inc32(&copy_errors_);
    Respond(callback, UploaderResults(1, local_path));
    return;
  }

  // copy file into controlled temporary directory location
  int retval  = CopyPath2Path(local_path, tmp_path);
  int retcode = retval ? 0 : 100;
  if (retcode != 0) {
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "failed to copy file '%s' to staging "
                                          "area: '%s'",
             local_path.c_str(), tmp_path.c_str());
    atomic_inc32(&copy_errors_);
    Respond(callback, UploaderResults(retcode, local_path));
    return;
  }

  // move the file in place (atomic operation)
  retcode = Move(tmp_path, remote_path);
  if (retcode != 0) {
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "failed to move file '%s' from the "
                                          "staging area to the final location: "
                                          "'%s'",
             tmp_path.c_str(), remote_path.c_str());
    atomic_inc32(&copy_errors_);
    Respond(callback, UploaderResults(retcode, local_path));
    return;
  }
  Respond(callback, UploaderResults(retcode, local_path));
}


int LocalUploader::CreateAndOpenTemporaryChunkFile(std::string *path) const {
  const std::string tmp_path = CreateTempPath(temporary_path_ + "/" + "chunk",
                                              0644);
  if (tmp_path.empty()) {
    LogCvmfs(kLogSpooler, kLogStderr,
             "Failed to create temp file for upload of file chunk (errno: %d).",
             errno);
    atomic_inc32(&copy_errors_);
    return -1;
  }

  const int tmp_fd = open(tmp_path.c_str(), O_WRONLY);
  if (tmp_fd < 0) {
    LogCvmfs(kLogSpooler, kLogStderr,
             "Failed to open temp file '%s' for upload of file chunk "
             "(errno: %d)", tmp_path.c_str(), errno);
    unlink(tmp_path.c_str());
    atomic_inc32(&copy_errors_);
    return tmp_fd;
  }

  *path = tmp_path;
  return tmp_fd;
}


UploadStreamHandle* LocalUploader::InitStreamedUpload(
                                                   const CallbackTN *callback) {
  std::string tmp_path;
  const int tmp_fd = CreateAndOpenTemporaryChunkFile(&tmp_path);
  if (tmp_fd < 0) {
    return NULL;
  }

  return new LocalStreamHandle(callback, tmp_fd, tmp_path);
}


void LocalUploader::Upload(UploadStreamHandle  *handle,
                           CharBuffer          *buffer,
                           const CallbackTN    *callback) {
  assert(buffer->IsInitialized());
  LocalStreamHandle *local_handle = static_cast<LocalStreamHandle*>(handle);

  const size_t bytes_written = write(local_handle->file_descriptor,
                                     buffer->ptr(),
                                     buffer->used_bytes());
  if (bytes_written != buffer->used_bytes()) {
    const int cpy_errno = errno;
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "failed to write %d bytes to '%s' "
                                          "(errno: %d)",
             buffer->used_bytes(),
             local_handle->temporary_path.c_str(),
             cpy_errno);
    atomic_inc32(&copy_errors_);
    Respond(callback, UploaderResults(cpy_errno, buffer));
    return;
  }

  Respond(callback, UploaderResults(0, buffer));
}


void LocalUploader::FinalizeStreamedUpload(UploadStreamHandle  *handle,
                                           const shash::Any    &content_hash) {
  int retval = 0;
  LocalStreamHandle *local_handle = static_cast<LocalStreamHandle*>(handle);

  retval = close(local_handle->file_descriptor);
  if (retval != 0) {
    const int cpy_errno = errno;
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "failed to close temp file '%s' "
                                          "(errno: %d)",
             local_handle->temporary_path.c_str(), cpy_errno);
    atomic_inc32(&copy_errors_);
    Respond(handle->commit_callback, UploaderResults(cpy_errno));
    return;
  }

  const std::string final_path = "data/" + content_hash.MakePath();
  if (!Peek(final_path)) {
    retval = Move(local_handle->temporary_path, final_path);
    if (retval != 0) {
      const int cpy_errno = errno;
      LogCvmfs(kLogSpooler, kLogVerboseMsg, "failed to move temp file '%s' to "
                                            "final location '%s' (errno: %d)",
               local_handle->temporary_path.c_str(),
               final_path.c_str(),
               cpy_errno);
      atomic_inc32(&copy_errors_);
      Respond(handle->commit_callback, UploaderResults(cpy_errno));
      return;
    }
  } else {
    const int retval = unlink(local_handle->temporary_path.c_str());
    if (retval != 0) {
      LogCvmfs(kLogSpooler, kLogVerboseMsg, "failed to remove temporary '%s' "
                                            "(errno: %d)",
               local_handle->temporary_path.c_str(), errno);
    }
  }

  const CallbackTN *callback = handle->commit_callback;
  delete local_handle;

  Respond(callback, UploaderResults(0));
}


bool LocalUploader::Remove(const std::string& file_to_delete) {
  const int retval = unlink((upstream_path_ + "/" + file_to_delete).c_str());
  return retval == 0 || errno == ENOENT;
}


bool LocalUploader::Peek(const std::string& path) const {
  return FileExists(upstream_path_ + "/" + path);
}


bool LocalUploader::PlaceBootstrappingShortcut(const shash::Any &object) const {
  const std::string src  = "data/" + object.MakePath();
  const std::string dest = upstream_path_ + "/" + object.MakeAlternativePath();
  return SymlinkForced(src, dest);
}


int LocalUploader::Move(const std::string &local_path,
                        const std::string &remote_path) const {
  const std::string destination_path = upstream_path_ + "/" + remote_path;

  // make sure the file has the right permissions
  int retval  = chmod(local_path.c_str(), backend_file_mode_);
  int retcode = (retval == 0) ? 0 : 101;
  if (retcode != 0) {
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "failed to set file permission '%s' "
                                          "errno: %d",
             local_path.c_str(), errno);
    return retcode;
  }

  // move the file in place
  retval  = rename(local_path.c_str(), destination_path.c_str());
  retcode = (retval == 0) ? 0 : errno;
  if (retcode != 0) {
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "failed to move file '%s' to '%s' "
                                          "errno: %d",
             local_path.c_str(), remote_path.c_str(), errno);
  }

  return retcode;
}

}  // namespace upload
