/**
 * This file is part of the CernVM File System.
 */

#include "upload_s3.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#ifdef _POSIX_PRIORITY_SCHEDULING
#include <sched.h>
#endif
#include <unistd.h>

#include <string>
#include <vector>

#include "compression.h"
#include "logging.h"
#include "options.h"
#include "s3fanout.h"
#include "util/posix.h"
#include "util/string.h"

namespace upload {

S3Uploader::S3Uploader(const SpoolerDefinition &spooler_definition)
  : AbstractUploader(spooler_definition)
  , dns_buckets_(true)
  , num_parallel_uploads_(kDefaultNumParallelUploads)
  , num_retries_(kDefaultNumRetries)
  , timeout_sec_(kDefaultTimeoutSec)
  , authz_method_(s3fanout::kAuthzAwsV2)
  , peek_before_put_(true)
  , temporary_path_(spooler_definition.temporary_path)
{
  assert(spooler_definition.IsValid() &&
         spooler_definition.driver_type == SpoolerDefinition::S3);

  atomic_init32(&io_errors_);
  atomic_init32(&terminate_);

  if (!ParseSpoolerDefinition(spooler_definition)) {
    abort();
  }

  s3fanout::S3FanoutManager::S3Config s3config;
  s3config.access_key = access_key_;
  s3config.secret_key = secret_key_;
  s3config.hostname_port = host_name_port_;
  s3config.authz_method = authz_method_;
  s3config.region = region_;
  s3config.bucket = bucket_;
  s3config.dns_buckets = dns_buckets_;
  s3config.pool_max_handles = num_parallel_uploads_;
  s3config.opt_timeout_sec = timeout_sec_;
  s3config.opt_max_retries = num_retries_;
  s3config.opt_backoff_init_ms = kDefaultBackoffInitMs;
  s3config.opt_backoff_max_ms = kDefaultBackoffMaxMs;

  s3fanout_mgr_ = new s3fanout::S3FanoutManager(s3config);
  s3fanout_mgr_->Spawn();

  int retval = pthread_create(
    &thread_collect_results_, NULL, MainCollectResults, this);
  assert(retval == 0);
}


S3Uploader::~S3Uploader() {
  atomic_inc32(&terminate_);
  pthread_join(thread_collect_results_, NULL);
}


bool S3Uploader::ParseSpoolerDefinition(
  const SpoolerDefinition &spooler_definition)
{
  const std::vector<std::string> config =
      SplitString(spooler_definition.spooler_configuration, '@');
  if (config.size() != 2) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Failed to parse spooler configuration string '%s'.\n"
             "Provide: <repo_alias>@/path/to/s3.conf",
             spooler_definition.spooler_configuration.c_str());
    return false;
  }
  repository_alias_              = config[0];
  const std::string &config_path = config[1];

  if (!FileExists(config_path)) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Cannot find S3 config file at '%s'",
             config_path.c_str());
    return false;
  }

  // Parse S3 configuration
  BashOptionsManager options_manager = BashOptionsManager(
    new DefaultOptionsTemplateManager(repository_alias_));
  options_manager.ParsePath(config_path, false);
  std::string parameter;

  if (!options_manager.GetValue("CVMFS_S3_HOST", &host_name_)) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Failed to parse CVMFS_S3_HOST from '%s'",
             config_path.c_str());
    return false;
  }
  if (options_manager.GetValue("CVMFS_S3_PORT", &parameter)) {
    host_name_port_ = host_name_ + ":" + parameter;
  } else {
    host_name_port_ = host_name_ + ":" + StringifyInt(kDefaultPort);
  }

  if (!options_manager.GetValue("CVMFS_S3_ACCESS_KEY", &access_key_)) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Failed to parse CVMFS_S3_ACCESS_KEY from '%s'.",
             config_path.c_str());
    return false;
  }
  if (!options_manager.GetValue("CVMFS_S3_SECRET_KEY", &secret_key_)) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Failed to parse CVMFS_S3_SECRET_KEY from '%s'.",
             config_path.c_str());
    return false;
  }
  if (!options_manager.GetValue("CVMFS_S3_BUCKET", &bucket_)) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Failed to parse CVMFS_S3_BUCKET from '%s'.",
             config_path.c_str());
    return false;
  }
  if (options_manager.GetValue("CVMFS_S3_DNS_BUCKETS", &parameter)) {
    if (parameter == "false") {
      dns_buckets_ = false;
    }
  }
  if (options_manager.GetValue("CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS",
                               &parameter))
  {
    num_parallel_uploads_ = String2Uint64(parameter);
  }
  if (options_manager.GetValue("CVMFS_S3_MAX_RETRIES", &parameter)) {
    num_retries_ = String2Uint64(parameter);
  }
  if (options_manager.GetValue("CVMFS_S3_TIMEOUT", &parameter)) {
    timeout_sec_ = String2Uint64(parameter);
  }
  if (options_manager.GetValue("CVMFS_S3_REGION", &region_)) {
    authz_method_ = s3fanout::kAuthzAwsV4;
  }
  if (options_manager.GetValue("CVMFS_S3_PEEK_BEFORE_PUT", &parameter)) {
    peek_before_put_ = options_manager.IsOn(parameter);
  }

  return true;
}


bool S3Uploader::WillHandle(const SpoolerDefinition &spooler_definition) {
  return spooler_definition.driver_type == SpoolerDefinition::S3;
}


unsigned int S3Uploader::GetNumberOfErrors() const {
  return atomic_read32(&io_errors_);
}


/**
 * Worker thread takes care of requesting new jobs and cleaning old ones.
 */
void *S3Uploader::MainCollectResults(void *data) {
  LogCvmfs(kLogUploadS3, kLogDebug, "Upload_S3 WorkerThread started.");
  S3Uploader *uploader = reinterpret_cast<S3Uploader *>(data);

  std::vector<s3fanout::JobInfo *> jobs;
  while (atomic_read32(&uploader->terminate_) == 0) {
    jobs.clear();
    uploader->s3fanout_mgr_->PopCompletedJobs(&jobs);
    for (unsigned i = 0; i < jobs.size(); ++i) {
      // Report completed job
      s3fanout::JobInfo *info = jobs[i];
      int reply_code = 0;
      if (info->error_code != s3fanout::kFailOk) {
        if ((info->request != s3fanout::JobInfo::kReqHeadOnly) ||
            (info->error_code != s3fanout::kFailNotFound))
        {
          LogCvmfs(kLogUploadS3, kLogStderr,
                   "Upload job for '%s' failed. (error code: %d - %s)",
                   info->object_key.c_str(),
                   info->error_code,
                   s3fanout::Code2Ascii(info->error_code));
          reply_code = 99;
          atomic_inc32(&uploader->io_errors_);
        }
      }
      if (info->request == s3fanout::JobInfo::kReqDelete) {
        uploader->Respond(NULL, UploaderResults());
      } else if (info->request == s3fanout::JobInfo::kReqHeadOnly) {
        if (info->error_code == s3fanout::kFailNotFound) reply_code = 1;
        uploader->Respond(static_cast<CallbackTN*>(info->callback),
                          UploaderResults(UploaderResults::kLookup,
                                          reply_code));
      } else {
        if (info->request == s3fanout::JobInfo::kReqHeadPut) {
          // The HEAD request was not transformed into a PUT request, thus this
          // was a duplicate
          // Uploaded catalogs are always unique ->
          // assume this was a regular file and decrease appropriate counters
          uploader->CountDuplicates();
          uploader->DecUploadedChunks();
          uploader->CountUploadedBytes(-(info->payload_size));
        }
        if (info->origin == s3fanout::kOriginMem) {
          uploader->Respond(static_cast<CallbackTN*>(info->callback),
                            UploaderResults(UploaderResults::kChunkCommit,
                                            reply_code));
        } else {
          uploader->Respond(static_cast<CallbackTN*>(info->callback),
                            UploaderResults(reply_code, info->origin_path));
        }

        assert(info->mmf == NULL);
        assert(info->origin_file == NULL);
      }
      delete info;
    }
#ifdef _POSIX_PRIORITY_SCHEDULING
    sched_yield();
#endif
  }

  LogCvmfs(kLogUploadS3, kLogDebug, "Upload_S3 WorkerThread finished.");
  return NULL;
}


void S3Uploader::FileUpload(
  const std::string &local_path,
  const std::string &remote_path,
  const CallbackTN  *callback
) {
  s3fanout::JobInfo *info =
    new s3fanout::JobInfo(repository_alias_ + "/" + remote_path,
                          const_cast<void*>(
                              static_cast<void const*>(callback)),
                          local_path);

  if (HasPrefix(remote_path, ".cvmfs", false /*ignore_case*/)) {
    info->request = s3fanout::JobInfo::kReqPutDotCvmfs;
  } else {
    if (peek_before_put_)
      info->request = s3fanout::JobInfo::kReqHeadPut;
  }

  UploadJobInfo(info);
  LogCvmfs(kLogUploadS3, kLogDebug, "Uploading from file finished: %s",
           local_path.c_str());
}


void S3Uploader::UploadJobInfo(s3fanout::JobInfo *info) {
  LogCvmfs(kLogUploadS3, kLogDebug,
           "Uploading from %s:\n"
           "--> Object: '%s'\n"
           "--> Bucket: '%s'\n"
           "--> Host:   '%s'\n",
           info->origin_mem.data != NULL ? "buffer" : "file",
           info->object_key.c_str(),
           bucket_.c_str(),
           host_name_port_.c_str());

  s3fanout_mgr_->PushNewJob(info);
}


UploadStreamHandle *S3Uploader::InitStreamedUpload(const CallbackTN *callback) {
  std::string tmp_path;
  const int tmp_fd = CreateAndOpenTemporaryChunkFile(&tmp_path);

  LogCvmfs(kLogUploadS3, kLogDebug, "InitStreamedUpload: %s", tmp_path.c_str());

  if (tmp_fd < 0) {
    LogCvmfs(kLogUploadS3, kLogStderr, "Failed to open file (%d), %s",
             errno, strerror(errno));
    atomic_inc32(&io_errors_);

    return NULL;
  }

  return new S3StreamHandle(callback, tmp_fd, tmp_path);
}


void S3Uploader::StreamedUpload(
  UploadStreamHandle  *handle,
  UploadBuffer        buffer,
  const CallbackTN    *callback)
{
  S3StreamHandle *local_handle = static_cast<S3StreamHandle*>(handle);

  LogCvmfs(kLogUploadS3, kLogDebug, "Upload target = %s",
           local_handle->temporary_path.c_str());

  if (!SafeWrite(local_handle->file_descriptor, buffer.data, buffer.size)) {
    const int cpy_errno = errno;
    LogCvmfs(kLogUploadS3, kLogStderr,
             "failed to write %d bytes to '%s' (errno: %d)",
             buffer.size,
             local_handle->temporary_path.c_str(),
             cpy_errno);
    atomic_inc32(&io_errors_);
    Respond(callback,
            UploaderResults(UploaderResults::kBufferUpload, cpy_errno));
    return;
  }
  Respond(callback, UploaderResults(UploaderResults::kBufferUpload, 0));
}


void S3Uploader::FinalizeStreamedUpload(
  UploadStreamHandle  *handle,
  const shash::Any    &content_hash)
{
  int retval = 0;
  S3StreamHandle *local_handle = static_cast<S3StreamHandle*>(handle);

  retval = close(local_handle->file_descriptor);
  if (retval != 0) {
    const int cpy_errno = errno;
    LogCvmfs(kLogUploadS3, kLogStderr,
             "failed to close temp file '%s' (errno: %d)",
             local_handle->temporary_path.c_str(), cpy_errno);
    atomic_inc32(&io_errors_);
    Respond(handle->commit_callback,
            UploaderResults(UploaderResults::kChunkCommit, cpy_errno));
    return;
  }

  // Open the file for reading
  MemoryMappedFile *mmf = new MemoryMappedFile(local_handle->temporary_path);
  if (!mmf->Map()) {
    LogCvmfs(kLogUploadS3, kLogStderr, "Failed to upload %s",
             local_handle->temporary_path.c_str());
    delete mmf;
    atomic_inc32(&io_errors_);
    Respond(handle->commit_callback,
            UploaderResults(100, local_handle->temporary_path));
    return;
  }

  // New file name based on content hash
  std::string final_path(
    repository_alias_ + "/data/" + content_hash.MakePath());

  s3fanout::JobInfo *info =
      new s3fanout::JobInfo(final_path,
                            const_cast<void*>(
                                static_cast<void const*>(
                                    handle->commit_callback)),
                            mmf,
                            reinterpret_cast<unsigned char *>(mmf->buffer()),
                            static_cast<size_t>(mmf->size()));
  assert(info != NULL);
  size_t bytes_uploaded = mmf->size();

  if (peek_before_put_)
      info->request = s3fanout::JobInfo::kReqHeadPut;
  UploadJobInfo(info);

  LogCvmfs(kLogUploadS3, kLogDebug, "Uploading from stream finished: %s",
           local_handle->temporary_path.c_str());

  // Remove the temporary file
  retval = unlink(local_handle->temporary_path.c_str());
  assert(retval == 0);
  delete local_handle;

  // Update statistics counters
  if (!content_hash.HasSuffix() ||
      content_hash.suffix == shash::kSuffixPartial) {
    CountUploadedChunks();
    CountUploadedBytes(bytes_uploaded);
  } else if (content_hash.suffix == shash::kSuffixCatalog) {
    CountUploadedCatalogs();
    CountUploadedCatalogBytes(bytes_uploaded);
  }
}


s3fanout::JobInfo *S3Uploader::CreateJobInfo(const std::string& path) const {
  return new s3fanout::JobInfo(path,
                               NULL,
                               NULL,
                               NULL,
                               0);
}


void S3Uploader::DoRemoveAsync(const std::string& file_to_delete) {
  const std::string mangled_path = repository_alias_ + "/" + file_to_delete;
  s3fanout::JobInfo *info = CreateJobInfo(mangled_path);

  info->request = s3fanout::JobInfo::kReqDelete;

  LogCvmfs(kLogUploadS3, kLogDebug, "Asynchronously removing %s/%s",
           bucket_.c_str(), info->object_key.c_str());
  s3fanout_mgr_->PushNewJob(info);
}


void S3Uploader::OnPeekComplete(
  const upload::UploaderResults &results,
  PeekCtrl *ctrl)
{
  ctrl->exists = (results.return_code == 0);
  char c = 'c';
  WritePipe(ctrl->pipe_wait[1], &c, 1);
}


bool S3Uploader::Peek(const std::string& path) {
  const std::string mangled_path = repository_alias_ + "/" + path;
  s3fanout::JobInfo *info = CreateJobInfo(mangled_path);

  PeekCtrl peek_ctrl;
  MakePipe(peek_ctrl.pipe_wait);
  info->request = s3fanout::JobInfo::kReqHeadOnly;
  info->callback = const_cast<void*>(static_cast<void const*>(MakeClosure(
    &S3Uploader::OnPeekComplete, this, &peek_ctrl)));

  IncJobsInFlight();
  UploadJobInfo(info);
  char c;
  ReadPipe(peek_ctrl.pipe_wait[0], &c, 1);
  assert(c == 'c');
  ClosePipe(peek_ctrl.pipe_wait);

  return peek_ctrl.exists;
}


bool S3Uploader::PlaceBootstrappingShortcut(const shash::Any &object) {
  return false;  // TODO(rmeusel): implement
}


int64_t S3Uploader::DoGetObjectSize(const std::string &file_name) {
  // TODO(dosarudaniel): use a head request for byte count
  // Re-enable 661 integration test when done
  return -EOPNOTSUPP;
}

}  // namespace upload
