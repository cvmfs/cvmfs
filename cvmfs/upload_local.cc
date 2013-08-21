/**
 * This file is part of the CernVM File System.
 */

#include "upload_local.h"

#include <errno.h>

#include "logging.h"
#include "compression.h"
#include "util.h"

#include "upload_file_processing/char_buffer.h"

using namespace upload;


LocalUploader::LocalUploader(const SpoolerDefinition &spooler_definition) :
  AbstractUploader(spooler_definition),
  upstream_path_(spooler_definition.spooler_configuration),
  temporary_path_(spooler_definition.temporary_path)
{
  assert (spooler_definition.IsValid() &&
          spooler_definition.driver_type == SpoolerDefinition::Local);

  atomic_init32(&copy_errors_);
}


bool LocalUploader::WillHandle(const SpoolerDefinition &spooler_definition) {
  return spooler_definition.driver_type == SpoolerDefinition::Local;
}


unsigned int LocalUploader::GetNumberOfErrors() const {
  return atomic_read32(&copy_errors_);
}


void LocalUploader::Upload(const std::string &local_path,
                           const std::string &remote_path,
                           const callback_t   *callback) {
  // create destination in backend storage temporary directory
  std::string tmp_path = CreateTempPath(temporary_path_ + "/upload", 0666);
  if (tmp_path.empty()) {
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "failed to create temp path for "
                                          "upload of file '%s'",
             local_path.c_str());
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
  }
  Respond(callback, UploaderResults(retcode, local_path));
}


void LocalUploader::Upload(const std::string  &local_path,
                           const hash::Any    &content_hash,
                           const std::string  &hash_suffix,
                           const callback_t   *callback) {
  const int retcode = Move(local_path,
                           "data" + content_hash.MakePath(1,2) + hash_suffix);
  if (retcode != 0) {
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "failed to move file '%s' to the "
                                          "final location: '%s'",
             local_path.c_str(), content_hash.ToString().c_str());
    atomic_inc32(&copy_errors_);
  }
  Respond(callback, UploaderResults(retcode, local_path));
}


UploadStreamHandle* LocalUploader::InitStreamedUpload(
                                                 const callback_t   *callback) {
  const std::string tmp_path = CreateTempPath(temporary_path_ + "/" + "chunk",
                                              0644);
  assert (! tmp_path.empty());

  const int tmp_fd = open(tmp_path.c_str(), O_WRONLY);
  assert (tmp_fd >= 0);

  return new LocalStreamHandle(callback, tmp_fd, tmp_path);
}


void LocalUploader::Upload(UploadStreamHandle  *handle,
                           CharBuffer          *buffer,
                           const callback_t    *callback) {
  assert (buffer->IsInitialized());
  LocalStreamHandle *local_handle = static_cast<LocalStreamHandle*>(handle);

  const size_t bytes_written = write(local_handle->file_descriptor,
                                     buffer->ptr(),
                                     buffer->used_bytes());
  if (bytes_written != buffer->used_bytes()) {
    LogCvmfs(kLogSpooler, kLogVerboseMsg, "failed to write %d bytes to '%s' "
                                          "(errno: %d)",
             buffer->used_bytes(), local_handle->temporary_path.c_str(), errno);
    Respond(callback, UploaderResults(errno, buffer));
    return;
  }

  Respond(callback, UploaderResults(0, buffer));
}


void LocalUploader::FinalizeStreamedUpload(UploadStreamHandle *handle,
                                           const hash::Any     content_hash,
                                           const std::string   hash_suffix) {
  int retval = 0;
  LocalStreamHandle *local_handle = static_cast<LocalStreamHandle*>(handle);

  retval = close(local_handle->file_descriptor);
  if (retval != 0) {
    Respond(handle->commit_callback, UploaderResults(errno));
    return;
  }

  const std::string final_path = upstream_path_ + "/data" +
                                 content_hash.MakePath(1, 2) +
                                 hash_suffix;

  retval = rename(local_handle->temporary_path.c_str(), final_path.c_str());
  if (retval != 0) {
    Respond(handle->commit_callback, UploaderResults(errno));
    return;
  }

  delete local_handle;

  Respond(handle->commit_callback, UploaderResults(0));
}


bool LocalUploader::Remove(const std::string& file_to_delete) {
  if (! Peek(file_to_delete)) {
    return false;
  }

  const int retval = unlink((upstream_path_ + "/" + file_to_delete).c_str());
  return retval == 0;
}


bool LocalUploader::Peek(const std::string& path) const {
  return FileExists(upstream_path_ + "/" + path);
}


int LocalUploader::Move(const std::string &local_path,
                        const std::string &remote_path) const {
  const std::string destination_path = upstream_path_ + "/" + remote_path;

  // make sure the file has the right permissions
  int retval  = chmod(local_path.c_str(), 0666);
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
