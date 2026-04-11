/**
 * This file is part of the CernVM File System.
 *
 * Prototype: GatewayS3Uploader
 *
 * Subclasses GatewayUploader to route content-addressed data objects
 * directly to S3, while catalogs and named files continue through the
 * gateway.
 */

#include "upload_gateway_s3.h"

#include <cassert>

#include "options.h"
#include "util/file_backed_buffer.h"
#include "util/logging.h"
#include "util/string.h"

namespace upload {

GatewayS3Uploader::GatewayS3Uploader(
    const SpoolerDefinition &spooler_definition,
    const std::string &s3_config_path,
    const std::string &repo_alias)
    : GatewayUploader(spooler_definition)
    , s3_config_path_(s3_config_path)
    , repo_alias_(repo_alias)
    , s3fanout_mgr_()
    , collector_running_(false) {
  atomic_init32(&s3_errors_);
}

GatewayS3Uploader::~GatewayS3Uploader() {
  if (collector_running_) {
    s3fanout_mgr_->PushCompletedJob(NULL);  // signal termination
    pthread_join(thread_collect_results_, NULL);
  }
}

bool GatewayS3Uploader::InitS3Manager() {
  // Parse S3 config file using the same format as the native S3 backend.  The
  // template manager takes the fqrn, which is what @fqrn@/@org@ in the config
  // file expand to -- the native backend passes the repository alias here too.
  BashOptionsManager options_manager = BashOptionsManager(
      new DefaultOptionsTemplateManager(repo_alias_));
  options_manager.ParsePath(s3_config_path_, false);
  std::string parameter;

  s3fanout::S3FanoutManager::S3Config s3config;

  if (!options_manager.GetValue("CVMFS_S3_HOST", &parameter)) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "GatewayS3: Missing CVMFS_S3_HOST in '%s'",
             s3_config_path_.c_str());
    return false;
  }
  s3config.hostname_port = parameter;

  if (!options_manager.GetValue("CVMFS_S3_ACCESS_KEY", &parameter)) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "GatewayS3: Missing CVMFS_S3_ACCESS_KEY in '%s'",
             s3_config_path_.c_str());
    return false;
  }
  s3config.access_key = parameter;

  if (!options_manager.GetValue("CVMFS_S3_SECRET_KEY", &parameter)) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "GatewayS3: Missing CVMFS_S3_SECRET_KEY in '%s'",
             s3_config_path_.c_str());
    return false;
  }
  s3config.secret_key = parameter;

  if (!options_manager.GetValue("CVMFS_S3_BUCKET", &parameter)) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "GatewayS3: Missing CVMFS_S3_BUCKET in '%s'",
             s3_config_path_.c_str());
    return false;
  }
  s3config.bucket = parameter;

  s3config.dns_buckets = true;
  if (options_manager.GetValue("CVMFS_S3_DNS_BUCKETS", &parameter)) {
    if (parameter == "false") s3config.dns_buckets = false;
  }

  if (options_manager.GetValue("CVMFS_S3_REGION", &parameter)) {
    s3config.region = parameter;
    s3config.authz_method = s3fanout::kAuthzAwsV4;
  }

  if (options_manager.GetValue("CVMFS_S3_FLAVOR", &parameter)) {
    if (parameter == "azure") {
      s3config.authz_method = s3fanout::kAuthzAzure;
    } else if (parameter == "awsv2") {
      s3config.authz_method = s3fanout::kAuthzAwsV2;
    } else if (parameter == "awsv4") {
      s3config.authz_method = s3fanout::kAuthzAwsV4;
    }
  }

  s3config.pool_max_handles = 16;
  if (options_manager.GetValue("CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS",
                               &parameter)) {
    s3config.pool_max_handles = String2Uint64(parameter);
  }

  if (options_manager.GetValue("CVMFS_S3_TIMEOUT", &parameter)) {
    s3config.opt_timeout_sec = String2Uint64(parameter);
  }

  bool use_https = false;
  if (options_manager.GetValue("CVMFS_S3_USE_HTTPS", &parameter)) {
    use_https = options_manager.IsOn(parameter);
  }
  s3config.protocol = use_https ? "https" : "http";

  if (options_manager.GetValue("CVMFS_S3_PORT", &parameter)) {
    // Reconstruct hostname_port with explicit port
    std::string host;
    options_manager.GetValue("CVMFS_S3_HOST", &host);
    s3config.hostname_port = host + ":" + parameter;
  }

  if (options_manager.IsDefined("CVMFS_S3_PROXY")) {
    options_manager.GetValue("CVMFS_S3_PROXY", &s3config.proxy);
  }

  s3fanout_mgr_ = new s3fanout::S3FanoutManager(s3config);
  s3fanout_mgr_->Spawn();

  const int retval = pthread_create(&thread_collect_results_, NULL,
                                    MainCollectResults, this);
  assert(retval == 0);
  collector_running_ = true;

  return true;
}

bool GatewayS3Uploader::Initialize() {
  // Initialize the base GatewayUploader (session context, etc.)
  if (!GatewayUploader::Initialize()) {
    return false;
  }

  // Initialize S3 direct upload
  if (!InitS3Manager()) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "GatewayS3Uploader: failed to initialize S3 fanout manager");
    return false;
  }

  LogCvmfs(kLogUploadGateway, kLogStdout,
           "GatewayS3Uploader: initialized (data→S3, catalogs→gateway)");
  return true;
}

void GatewayS3Uploader::WaitForUpload() const {
  // GatewayUploader::WaitForUpload() overrides the AbstractUploader version and
  // only drains the gateway session, which knows nothing about the objects we
  // handed to the S3 fanout manager.  Those are tracked by jobs_in_flight_,
  // decremented in Respond() from the collector thread, so wait for that too --
  // otherwise the manifest is committed while data chunks are still in flight.
  AbstractUploader::WaitForUpload();
  GatewayUploader::WaitForUpload();
}

unsigned int GatewayS3Uploader::GetNumberOfErrors() const {
  return GatewayUploader::GetNumberOfErrors() + atomic_read32(&s3_errors_);
}

void GatewayS3Uploader::FinalizeStreamedUpload(
    UploadStreamHandle *handle,
    const shash::Any &content_hash) {
  GatewayStreamHandle *gw_handle =
      dynamic_cast<GatewayStreamHandle *>(handle);
  assert(gw_handle);

  const bool is_catalog = content_hash.HasSuffix()
      && content_hash.suffix == shash::kSuffixCatalog;

  if (is_catalog) {
    // Route catalogs through the gateway (normal path)
    LogCvmfs(kLogUploadGateway, kLogDebug,
             "GatewayS3: routing catalog %s to gateway",
             content_hash.ToString().c_str());
    GatewayUploader::FinalizeStreamedUpload(handle, content_hash);
    return;
  }

  // Data chunk → upload directly to S3
  LogCvmfs(kLogUploadS3, kLogDebug,
           "GatewayS3: routing data %s to S3",
           content_hash.ToString().c_str());

  // Extract the data from the gateway bucket.  The bucket has been filled
  // by GatewayUploader::StreamedUpload via ObjectPack::AddToBucket.
  const std::string object_key =
      repo_alias_ + "/data/" + content_hash.MakePath();

  // Create an S3 job from the bucket data
  FileBackedBuffer *buf = FileBackedBuffer::Create(500 * 1024);
  buf->Append(gw_handle->bucket->content, gw_handle->bucket->size);
  buf->Commit();

  // The content now lives in buf; hand the bucket back to the session.  It is
  // never committed to an ObjectPack, so without this it would stay in the
  // session's active handles for the rest of the publish, keeping the whole
  // ingested payload in memory and being copied into every following pack.
  session_context_->DiscardBucket(gw_handle->bucket);
  gw_handle->bucket = NULL;

  const size_t bytes_uploaded = buf->GetSize();

  s3fanout::JobInfo *info = new s3fanout::JobInfo(
      object_key,
      const_cast<void *>(
          static_cast<void const *>(gw_handle->commit_callback)),
      buf);

  info->request = s3fanout::JobInfo::kReqHeadPut;
  s3fanout_mgr_->PushNewJob(info);

  // Update statistics counters
  if (!content_hash.HasSuffix()
      || content_hash.suffix == shash::kSuffixPartial) {
    CountUploadedChunks();
    CountUploadedBytes(bytes_uploaded);
  }

  delete gw_handle;
}

void *GatewayS3Uploader::MainCollectResults(void *data) {
  LogCvmfs(kLogUploadS3, kLogDebug,
           "GatewayS3 S3 WorkerThread started.");
  GatewayS3Uploader *uploader =
      reinterpret_cast<GatewayS3Uploader *>(data);

  while (true) {
    s3fanout::JobInfo *info = uploader->s3fanout_mgr_->PopCompletedJob();
    if (!info)
      break;

    int reply_code = 0;
    if (info->error_code != s3fanout::kFailOk) {
      LogCvmfs(kLogUploadS3, kLogStderr,
               "GatewayS3: S3 upload of '%s' failed (error: %d - %s)",
               info->object_key.c_str(), info->error_code,
               s3fanout::Code2Ascii(info->error_code));
      reply_code = 99;
      atomic_inc32(&uploader->s3_errors_);
    }

    if (info->request == s3fanout::JobInfo::kReqHeadPut
        && info->error_code == s3fanout::kFailOk) {
      // HEAD indicated object exists — duplicate, no actual upload
      uploader->CountDuplicates();
    }

    uploader->Respond(
        static_cast<CallbackTN *>(info->callback),
        UploaderResults(UploaderResults::kChunkCommit, reply_code));

    delete info;
  }

  LogCvmfs(kLogUploadS3, kLogDebug,
           "GatewayS3 S3 WorkerThread finished.");
  return NULL;
}

}  // namespace upload
