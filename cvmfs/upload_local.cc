/**
 * This file is part of the CernVM File System.
 */

#include "upload_local.h"


#include <errno.h>

#include <string>

#include "compression/compression.h"
#include "util/logging.h"
#include "util/posix.h"

namespace upload {

LocalUploader::LocalUploader(const SpoolerDefinition &spooler_definition)
    : AbstractUploader(spooler_definition),
      backend_file_mode_(default_backend_file_mode_ ^ GetUmask()),
      backend_dir_mode_(default_backend_dir_mode_ ^ GetUmask()),
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

bool LocalUploader::Create() {
  return MakeCacheDirectories(upstream_path_ + "/data", backend_dir_mode_) &&
         MkdirDeep(upstream_path_ + "/stats", backend_dir_mode_, false);
}

void LocalUploader::DoUpload(const std::string &remote_path,
                             IngestionSource *source,
                             const CallbackTN *callback) {
  LogCvmfs(kLogSpooler, kLogVerboseMsg, "FileUpload call started.");

  // create destination in backend storage temporary directory
  std::string tmp_path;
  FILE *ftmp = CreateTempFile(temporary_path_ + "/upload", 0666, "w",
                              &tmp_path);
  if (ftmp == NULL) {
    LogCvmfs(kLogSpooler, kLogVerboseMsg,
             "failed to create temp path for "
             "upload of file '%s' (errno: %d)",
             source->GetPath().c_str(), errno);
    atomic_inc32(&copy_errors_);
    Respond(callback, UploaderResults(1, source->GetPath()));
    return;
  }

  // copy file into controlled temporary directory location
  bool rvb = source->Open();
  if (!rvb) {
    fclose(ftmp);
    unlink(tmp_path.c_str());
    atomic_inc32(&copy_errors_);
    Respond(callback, UploaderResults(100, source->GetPath()));
    return;
  }
  unsigned char buffer[kPageSize];
  ssize_t rbytes;
  do {
    rbytes = source->Read(buffer, kPageSize);
    size_t wbytes = 0;
    if (rbytes > 0) {
      wbytes = fwrite(buffer, 1, rbytes, ftmp);
    }
    if ((rbytes < 0) || (static_cast<size_t>(rbytes) != wbytes)) {
      source->Close();
      fclose(ftmp);
      unlink(tmp_path.c_str());
      atomic_inc32(&copy_errors_);
      Respond(callback, UploaderResults(100, source->GetPath()));
      return;
    }
  } while (rbytes == kPageSize);
  source->Close();
  fclose(ftmp);

  // move the file in place (atomic operation)
  int rvi = Move(tmp_path, remote_path);
  if (rvi != 0) {
    LogCvmfs(kLogSpooler, kLogVerboseMsg,
             "failed to move file '%s' from the "
             "staging area to the final location: "
             "'%s'",
             tmp_path.c_str(), remote_path.c_str());
    unlink(tmp_path.c_str());
    atomic_inc32(&copy_errors_);
    Respond(callback, UploaderResults(rvi, source->GetPath()));
    return;
  }

  Respond(callback, UploaderResults(rvi, source->GetPath()));
}

UploadStreamHandle *LocalUploader::InitStreamedUpload(
    const CallbackTN *callback) {
  std::string tmp_path;
  const int tmp_fd = CreateAndOpenTemporaryChunkFile(&tmp_path);
  if (tmp_fd < 0) {
    atomic_inc32(&copy_errors_);
    return NULL;
  }

  return new LocalStreamHandle(callback, tmp_fd, tmp_path);
}

void LocalUploader::StreamedUpload(UploadStreamHandle *handle,
                                   UploadBuffer buffer,
                                   const CallbackTN *callback) {
  LocalStreamHandle *local_handle = static_cast<LocalStreamHandle *>(handle);

  const size_t bytes_written =
      write(local_handle->file_descriptor, buffer.data, buffer.size);
  if (bytes_written != buffer.size) {
    const int cpy_errno = errno;
    LogCvmfs(kLogSpooler, kLogVerboseMsg,
             "failed to write %lu bytes to '%s' "
             "(errno: %d)",
             buffer.size, local_handle->temporary_path.c_str(),
             cpy_errno);
    atomic_inc32(&copy_errors_);
    Respond(callback,
            UploaderResults(UploaderResults::kBufferUpload, cpy_errno));
    return;
  }

  Respond(callback, UploaderResults(UploaderResults::kBufferUpload, 0));
}

void LocalUploader::FinalizeStreamedUpload(UploadStreamHandle *handle,
                                           const shash::Any &content_hash) {
  int retval = 0;
  LocalStreamHandle *local_handle = static_cast<LocalStreamHandle *>(handle);

  retval = close(local_handle->file_descriptor);
  if (retval != 0) {
    const int cpy_errno = errno;
    LogCvmfs(kLogSpooler, kLogVerboseMsg,
             "failed to close temp file '%s' "
             "(errno: %d)",
             local_handle->temporary_path.c_str(), cpy_errno);
    atomic_inc32(&copy_errors_);
    Respond(handle->commit_callback,
            UploaderResults(UploaderResults::kChunkCommit, cpy_errno));
    return;
  }

  std::string final_path;
  if (local_handle->remote_path != "") {
    final_path = local_handle->remote_path;
  } else {
    final_path = "data/" + content_hash.MakePath();
  }
  if (!Peek(final_path)) {
    retval = Move(local_handle->temporary_path, final_path);
    if (retval != 0) {
      const int cpy_errno = errno;
      LogCvmfs(kLogSpooler, kLogVerboseMsg,
               "failed to move temp file '%s' to "
               "final location '%s' (errno: %d)",
               local_handle->temporary_path.c_str(), final_path.c_str(),
               cpy_errno);
      atomic_inc32(&copy_errors_);
      Respond(handle->commit_callback,
              UploaderResults(UploaderResults::kChunkCommit, cpy_errno));
      return;
    }
    if (!content_hash.HasSuffix()
        || content_hash.suffix == shash::kSuffixPartial) {
      CountUploadedChunks();
      CountUploadedBytes(GetFileSize(upstream_path_ + "/" + final_path));
    } else if (content_hash.suffix == shash::kSuffixCatalog) {
      CountUploadedCatalogs();
      CountUploadedCatalogBytes(GetFileSize(upstream_path_ + "/" + final_path));
    }
  } else {
    const int retval = unlink(local_handle->temporary_path.c_str());
    if (retval != 0) {
      LogCvmfs(kLogSpooler, kLogVerboseMsg,
               "failed to remove temporary file '%s' (errno: %d)",
               local_handle->temporary_path.c_str(), errno);
    }
    CountDuplicates();
  }

  const CallbackTN *callback = handle->commit_callback;
  delete local_handle;

  Respond(callback, UploaderResults(UploaderResults::kChunkCommit, 0));
}

/**
 * TODO(jblomer): investigate if parallelism increases the GC speed on local
 * disks.
 */
void LocalUploader::DoRemoveAsync(const std::string &file_to_delete) {
  const int retval = unlink((upstream_path_ + "/" + file_to_delete).c_str());
  if ((retval != 0) && (errno != ENOENT))
    atomic_inc32(&copy_errors_);
  Respond(NULL, UploaderResults());
}

bool LocalUploader::Peek(const std::string &path) {
  bool retval = FileExists(upstream_path_ + "/" + path);
  return retval;
}

bool LocalUploader::Mkdir(const std::string &path) {
  return MkdirDeep(upstream_path_ + "/" + path, backend_dir_mode_, false);
}

bool LocalUploader::PlaceBootstrappingShortcut(const shash::Any &object) {
  const std::string src = "data/" + object.MakePath();
  const std::string dest = upstream_path_ + "/" + object.MakeAlternativePath();
  return SymlinkForced(src, dest);
}

int LocalUploader::Move(const std::string &local_path,
                        const std::string &remote_path) const {
  const std::string destination_path = upstream_path_ + "/" + remote_path;

  // make sure the file has the right permissions
  int retval = chmod(local_path.c_str(), backend_file_mode_);
  int retcode = (retval == 0) ? 0 : 101;
  if (retcode != 0) {
    LogCvmfs(kLogSpooler, kLogVerboseMsg,
             "failed to set file permission '%s' "
             "errno: %d",
             local_path.c_str(), errno);
    return retcode;
  }

  // move the file in place
  retval = rename(local_path.c_str(), destination_path.c_str());
  retcode = (retval == 0) ? 0 : errno;
  if (retcode != 0) {
    LogCvmfs(kLogSpooler, kLogVerboseMsg,
             "failed to move file '%s' to '%s' "
             "errno: %d",
             local_path.c_str(), remote_path.c_str(), errno);
  }

  return retcode;
}

int64_t LocalUploader::DoGetObjectSize(const std::string &file_name) {
  return GetFileSize(upstream_path_ + "/" + file_name);
}

}  // namespace upload
