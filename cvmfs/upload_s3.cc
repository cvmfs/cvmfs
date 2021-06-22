/**
 * This file is part of the CernVM File System.
 */

#include "upload_s3.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "compression.h"
#include "logging.h"
#include "options.h"
#include "s3fanout.h"
#include "util/exception.h"
#include "util/posix.h"
#include "util/string.h"

namespace upload {

void S3Uploader::RequestCtrl::WaitFor() {
  char c;
  ReadPipe(pipe_wait[0], &c, 1);
  assert(c == 'c');
  ClosePipe(pipe_wait);
}


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

  if (!ParseSpoolerDefinition(spooler_definition)) {
    PANIC(kLogStderr, "Error in parsing the spooler definition");
  }

  s3fanout::S3FanoutManager::S3Config s3config;
  s3config.access_key = access_key_;
  s3config.secret_key = secret_key_;
  s3config.hostname_port = host_name_port_;
  s3config.authz_method = authz_method_;
  s3config.region = region_;
  s3config.flavor = flavor_;
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
  // Signal termination to our own worker thread
  s3fanout_mgr_->PushCompletedJob(NULL);
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
    host_name_port_ = host_name_;
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
  if (options_manager.GetValue("CVMFS_S3_FLAVOR", &flavor_)) {
    if (flavor_ == "azure") {
      authz_method_ = s3fanout::kAuthzAzure;
    }
  }
  if (options_manager.GetValue("CVMFS_S3_PEEK_BEFORE_PUT", &parameter)) {
    peek_before_put_ = options_manager.IsOn(parameter);
  }

  return true;
}


bool S3Uploader::WillHandle(const SpoolerDefinition &spooler_definition) {
  return spooler_definition.driver_type == SpoolerDefinition::S3;
}


bool S3Uploader::Create() {
  if (!dns_buckets_)
    return false;

  s3fanout::JobInfo *info = CreateJobInfo("");
  info->request = s3fanout::JobInfo::kReqPutBucket;
  std::string request_content;
  if (!region_.empty()) {
    request_content =
      std::string("<CreateBucketConfiguration xmlns="
        "\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        "<LocationConstraint>") + region_ + "</LocationConstraint>"
        "</CreateBucketConfiguration>";
    info->origin->Append(request_content.data(), request_content.length());
    info->origin->Commit();
  }

  RequestCtrl req_ctrl;
  MakePipe(req_ctrl.pipe_wait);
  info->callback = const_cast<void*>(static_cast<void const*>(MakeClosure(
    &S3Uploader::OnReqComplete, this, &req_ctrl)));

  IncJobsInFlight();
  UploadJobInfo(info);
  req_ctrl.WaitFor();

  return req_ctrl.return_code == 0;
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

  while (true) {
    s3fanout::JobInfo *info = uploader->s3fanout_mgr_->PopCompletedJob();
    if (!info)
      break;
    // Report completed job
    int reply_code = 0;
    if (info->error_code != s3fanout::kFailOk) {
      if ((info->request != s3fanout::JobInfo::kReqHeadOnly) ||
          (info->error_code != s3fanout::kFailNotFound)) {
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
      uploader->Respond(static_cast<CallbackTN*>(info->callback),
                        UploaderResults(UploaderResults::kChunkCommit,
                                        reply_code));

      assert(!info->origin.IsValid());
    }
    delete info;
  }

  LogCvmfs(kLogUploadS3, kLogDebug, "Upload_S3 WorkerThread finished.");
  return NULL;
}


void S3Uploader::DoUpload(
  const std::string &remote_path,
  IngestionSource *source,
  const CallbackTN *callback
) {
  bool rvb = source->Open();
  if (!rvb) {
    Respond(callback, UploaderResults(100, source->GetPath()));
    return;
  }
  uint64_t size;
  rvb = source->GetSize(&size);
  assert(rvb);

  FileBackedBuffer *origin =
    FileBackedBuffer::Create(kInMemoryObjectThreshold,
                             spooler_definition().temporary_path);

  unsigned char buffer[kPageSize];
  ssize_t nbytes;
  do {
    nbytes = source->Read(buffer, kPageSize);
    if (nbytes > 0) origin->Append(buffer, nbytes);
    if (nbytes < 0) {
      source->Close();
      delete origin;
      Respond(callback, UploaderResults(100, source->GetPath()));
      return;
    }
  } while (nbytes == kPageSize);
  source->Close();
  origin->Commit();

  s3fanout::JobInfo *info =
    new s3fanout::JobInfo(repository_alias_ + "/" + remote_path,
                          const_cast<void*>(
                              static_cast<void const*>(callback)),
                          origin);

  if (HasPrefix(remote_path, ".cvmfs", false /*ignore_case*/)) {
    info->request = s3fanout::JobInfo::kReqPutDotCvmfs;
  } else if (HasSuffix(remote_path, ".html", false)) {
    info->request = s3fanout::JobInfo::kReqPutHtml;
  } else {
    if (peek_before_put_)
      info->request = s3fanout::JobInfo::kReqHeadPut;
  }

  RequestCtrl req_ctrl;
  MakePipe(req_ctrl.pipe_wait);
  req_ctrl.callback_forward = callback;
  req_ctrl.original_path = source->GetPath();
  info->callback = const_cast<void*>(static_cast<void const*>(MakeClosure(
    &S3Uploader::OnReqComplete, this, &req_ctrl)));

  UploadJobInfo(info);
  req_ctrl.WaitFor();
  LogCvmfs(kLogUploadS3, kLogDebug, "Uploading from source finished: %s",
           source->GetPath().c_str());
}


void S3Uploader::UploadJobInfo(s3fanout::JobInfo *info) {
  LogCvmfs(kLogUploadS3, kLogDebug,
           "Uploading:\n"
           "--> Object: '%s'\n"
           "--> Bucket: '%s'\n"
           "--> Host:   '%s'\n",
           info->object_key.c_str(),
           bucket_.c_str(),
           host_name_port_.c_str());

  s3fanout_mgr_->PushNewJob(info);
}


UploadStreamHandle *S3Uploader::InitStreamedUpload(const CallbackTN *callback) {
  return new S3StreamHandle(callback, kInMemoryObjectThreshold,
                            spooler_definition().temporary_path);
}


void S3Uploader::StreamedUpload(
  UploadStreamHandle  *handle,
  UploadBuffer        buffer,
  const CallbackTN    *callback)
{
  S3StreamHandle *s3_handle = static_cast<S3StreamHandle*>(handle);

  s3_handle->buffer->Append(buffer.data, buffer.size);
  Respond(callback, UploaderResults(UploaderResults::kBufferUpload, 0));
}


void S3Uploader::FinalizeStreamedUpload(
  UploadStreamHandle  *handle,
  const shash::Any    &content_hash)
{
  S3StreamHandle *s3_handle = static_cast<S3StreamHandle*>(handle);

  // New file name based on content hash or remote_path override
  std::string final_path;
  if (s3_handle->remote_path != "") {
    final_path = repository_alias_ + "/" + s3_handle->remote_path;
  } else {
    final_path = repository_alias_ + "/data/" + content_hash.MakePath();
  }

  s3_handle->buffer->Commit();

  size_t bytes_uploaded = s3_handle->buffer->GetSize();

  s3fanout::JobInfo *info =
      new s3fanout::JobInfo(final_path,
                            const_cast<void*>(
                                static_cast<void const*>(
                                    handle->commit_callback)),
                            s3_handle->buffer.Release());

  if (peek_before_put_)
      info->request = s3fanout::JobInfo::kReqHeadPut;
  UploadJobInfo(info);

  // Remove the temporary file
  delete s3_handle;

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
  FileBackedBuffer *buf = FileBackedBuffer::Create(kInMemoryObjectThreshold);
  return new s3fanout::JobInfo(path, NULL, buf);
}


void S3Uploader::DoRemoveAsync(const std::string& file_to_delete) {
  const std::string mangled_path = repository_alias_ + "/" + file_to_delete;
  s3fanout::JobInfo *info = CreateJobInfo(mangled_path);

  info->request = s3fanout::JobInfo::kReqDelete;

  LogCvmfs(kLogUploadS3, kLogDebug, "Asynchronously removing %s/%s",
           bucket_.c_str(), info->object_key.c_str());
  s3fanout_mgr_->PushNewJob(info);
}


void S3Uploader::OnReqComplete(
  const upload::UploaderResults &results,
  RequestCtrl *ctrl)
{
  ctrl->return_code = results.return_code;
  if (ctrl->callback_forward != NULL) {
    // We are already in Respond() so we must not call it again
    upload::UploaderResults fix_path(results.return_code, ctrl->original_path);
    (*(ctrl->callback_forward))(fix_path);
    delete ctrl->callback_forward;
    ctrl->callback_forward = NULL;
  }
  char c = 'c';
  WritePipe(ctrl->pipe_wait[1], &c, 1);
}


bool S3Uploader::Peek(const std::string& path) {
  const std::string mangled_path = repository_alias_ + "/" + path;
  s3fanout::JobInfo *info = CreateJobInfo(mangled_path);

  RequestCtrl req_ctrl;
  MakePipe(req_ctrl.pipe_wait);
  info->request = s3fanout::JobInfo::kReqHeadOnly;
  info->callback = const_cast<void*>(static_cast<void const*>(MakeClosure(
    &S3Uploader::OnReqComplete, this, &req_ctrl)));

  IncJobsInFlight();
  UploadJobInfo(info);
  req_ctrl.WaitFor();

  return req_ctrl.return_code == 0;
}


// noop: no mkdir needed in S3 storage
bool S3Uploader::Mkdir(const std::string &path) {
  return true;
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
