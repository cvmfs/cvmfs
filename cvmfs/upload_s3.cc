/**
 * This file is part of the CernVM File System.
 */

#include "upload_s3.h"

#include <errno.h>
#include <inttypes.h>
#ifdef _POSIX_PRIORITY_SCHEDULING
#include <sched.h>
#endif
#include <unistd.h>

#include <sstream>  // TODO(jblomer): remove me
#include <string>
#include <vector>

#include "compression.h"
#include "file_processing/char_buffer.h"
#include "logging.h"
#include "options.h"
#include "s3fanout.h"
#include "util.h"

namespace upload {

S3Uploader::S3Uploader(const SpoolerDefinition &spooler_definition)
    : AbstractUploader(spooler_definition),
      temporary_path_(spooler_definition.temporary_path) {
  if (!ParseSpoolerDefinition(spooler_definition)) {
    abort();
  }

  assert(spooler_definition.IsValid() &&
         spooler_definition.driver_type == SpoolerDefinition::S3);

  s3fanout_mgr_.Init(max_num_parallel_uploads_);
  s3fanout_mgr_.Spawn();

  atomic_init32(&copy_errors_);
}

S3Uploader::~S3Uploader() {
  s3fanout_mgr_.Fini();
}

bool S3Uploader::ParseSpoolerDefinition(
    const SpoolerDefinition &spooler_definition) {
  // parse spooler configuration
  const std::vector<std::string> config =
      SplitString(spooler_definition.spooler_configuration, '@');
  if (config.size() != 2) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Failed to parse spooler configuration "
             "string '%s'.\n"
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
  // TODO(sheikkila): separate option handling and sanity checks
  OptionsManager *options_manager = new BashOptionsManager();
  options_manager->ParsePath(config_path, false);
  std::string parameter;
  std::string s3_host;
  if (!options_manager->GetValue("CVMFS_S3_HOST", &s3_host)) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Failed to parse CVMFS_S3_HOST from '%s'",
             config_path.c_str());
    return false;
  }
  const std::string kStandardPort = "80";
  std::string s3_port = kStandardPort;
  options_manager->GetValue("CVMFS_S3_PORT", &s3_port);
  int s3_buckets_per_account = 1;
  if (options_manager->GetValue("CVMFS_S3_BUCKETS_PER_ACCOUNT", &parameter)) {
    s3_buckets_per_account = String2Uint64(parameter);
    if (s3_buckets_per_account < 1 || s3_buckets_per_account > 100) {
      LogCvmfs(kLogUploadS3, kLogStderr,
               "Fail, invalid CVMFS_S3_BUCKETS_PER_ACCOUNT "
               "given: '%d'.",
               s3_buckets_per_account);
      LogCvmfs(kLogUploadS3, kLogStderr,
               "CVMFS_S3_BUCKETS_PER_ACCOUNT should be in range 1-100.");
      return false;
    }
  }
  std::string s3_access_key;
  if (!options_manager->GetValue("CVMFS_S3_ACCESS_KEY", &s3_access_key)) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Failed to parse CVMFS_S3_ACCESS_KEY from '%s'.",
             config_path.c_str());
    return false;
  }
  std::string s3_secret_key;
  if (!options_manager->GetValue("CVMFS_S3_SECRET_KEY", &s3_secret_key)) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Failed to parse CVMFS_S3_SECRET_KEY from '%s'.",
             config_path.c_str());
    return false;
  }
  if (!options_manager->GetValue("CVMFS_S3_BUCKET", &bucket_body_name_)) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Failed to parse CVMFS_S3_BUCKET from '%s'.",
             config_path.c_str());
    return false;
  }
  if (!options_manager->GetValue("CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS",
                         &parameter)) {
    LogCvmfs(kLogUploadS3, kLogStderr, "Failed to parse "
             "CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS "
             "from '%s'.",
             config_path.c_str());
    return false;
  }
  max_num_parallel_uploads_ = String2Uint64(parameter);
  delete options_manager;
  options_manager = NULL;

  std::vector<std::string> s3_access_keys = SplitString(s3_access_key, ':');
  std::vector<std::string> s3_secret_keys = SplitString(s3_secret_key, ':');
  if (s3_access_keys.size() != s3_secret_keys.size()) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Failure, number of accounts does not match");
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Specify keys like this: "
             "CVMFS_S3_ACCESS_KEY=key1:key2:key3:...");
    return false;
  }

  unsigned s3_num_accounts = s3_access_keys.size();
  number_of_buckets_ = s3_buckets_per_account * s3_num_accounts;
  host_name_         = s3_host;
  full_host_name_    = s3_host + ":" + s3_port;
  for (unsigned i = 0; i < s3_num_accounts; i++) {
    keys_.push_back(std::make_pair(s3_access_keys.at(i), s3_secret_keys.at(i)));
  }

  return true;
}


bool S3Uploader::WillHandle(const SpoolerDefinition &spooler_definition) {
  return spooler_definition.driver_type == SpoolerDefinition::S3;
}


unsigned int S3Uploader::GetNumberOfErrors() const {
  return atomic_read32(&copy_errors_);
}


/**
 * Worker thread takes care of requesting new jobs and cleaning old
 * ones.
 */
void S3Uploader::WorkerThread() {
  LogCvmfs(kLogUploadS3, kLogDebug, "Upload_S3 WorkerThread started.");

  bool running = true;
  while (running) {
    UploadJob job;

    // Try to get new job
    bool newjob = TryToAcquireNewJob(&job);
    if (newjob) {
      switch (job.type) {
        case UploadJob::Upload:
          Upload(job.stream_handle,
                 job.buffer,
                 job.callback);
          break;
        case UploadJob::Commit:
          // Note, this block until upload is possible
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

    // Get and report completed jobs
    std::vector<s3fanout::JobInfo *> jobs;
    jobs.clear();
    s3fanout_mgr_.PopCompletedJobs(&jobs);
    std::vector<s3fanout::JobInfo*>::iterator             it    = jobs.begin();
    const std::vector<s3fanout::JobInfo*>::const_iterator itend = jobs.end();
    for (; it != itend; ++it) {
      // Report completed job
      s3fanout::JobInfo *info = *it;
      int reply_code = 0;
      if (info->error_code != s3fanout::kFailOk) {
        LogCvmfs(kLogUploadS3, kLogStderr, "Upload job for '%s' failed. "
                                           "(error code: %d - %s)",
                 info->object_key.c_str(), info->error_code,
                 s3fanout::Code2Ascii(info->error_code));
        reply_code = 99;
      }
      if (info->origin == s3fanout::kOriginMem) {
        Respond(static_cast<CallbackTN*>(info->callback),
                UploaderResults(reply_code));
      } else {
        Respond(static_cast<CallbackTN*>(info->callback),
                UploaderResults(reply_code, info->origin_path));
      }
      assert(info->mmf == NULL);
      assert(info->origin_file == NULL);
    }
#ifdef _POSIX_PRIORITY_SCHEDULING
    sched_yield();
#endif
  }

  LogCvmfs(kLogUploadS3, kLogDebug, "Upload_S3 WorkerThread finished.");
}


/**
 * Returns the access/secret key index of requested bucket
 *
 * @param use_bucket Bucket to use between 0-(number_of_buckets_-1)
 * @return Index to the key to be used
 */
int S3Uploader::GetKeyIndex(unsigned int use_bucket) const {
  if (use_bucket >= static_cast<unsigned int>(number_of_buckets_)) {
    return 0;
  }
  return use_bucket % keys_.size();
}


/**
 * Gets S3 access key, S3 secret key and the bucket to be used.
 *
 * @param filename This is used to determine which keys and bucket to use
 * @param access_key S3 access key to be used
 * @param secret_key S3 secret key to be used
 * @param bucket_name Name of the bucket to be used
 * @return 0 if all ok
 */
int S3Uploader::GetKeysAndBucket(const std::string &filename,
                                 std::string       *access_key,
                                 std::string       *secret_key,
                                 std::string       *bucket_name) const {
  unsigned int use_bucket = SelectBucket(filename);
  *bucket_name = GetBucketName(use_bucket);

  int k = GetKeyIndex(use_bucket);
  *access_key = keys_.at(k).first;
  *secret_key = keys_.at(k).second;

  return 0;
}


/**
 * Returns bucket name based on bucket number
 *
 * @param use_bucket Bucket to use between 0-(number_of_buckets_-1)
 * @return bucket name
 */
std::string S3Uploader::GetBucketName(unsigned int use_bucket) const {
  std::stringstream ss;
  if (use_bucket >= static_cast<unsigned int>(number_of_buckets_)) {
    ss << bucket_body_name_ << "-1-1";
    return ss.str();
  }

  int i = use_bucket / keys_.size();
  int k = GetKeyIndex(use_bucket);
  ss << bucket_body_name_ << "-" << (k + 1) << "-" << (i + 1);

  return ss.str();
}


/**
 * Chooses a bucket according to filename. The bucket is chosen by
 * taking a modulo of a number that is calculated as a sum from a
 * hexadecimal presentation of the filename.
 *
 * @param rem_filename Filename to map into bucket
 * @return bucket index, between 0 and (number_of_buckets_-1)
 */
int S3Uploader::SelectBucket(const std::string &rem_filename) const {
  unsigned int use_bucket = 0;
  unsigned int cutlength  = 3;  // Process filename in parts of this length
  std::string hex_filename;     // Filename with only valid hex-symbols

  // Accept only hex chars
  for (unsigned i = 0; i < rem_filename.length(); i++) {
    char w = rem_filename.at(i);
    if ((w >= 48 && w <= 57) ||
        (w >= 65  && w <= 70) ||
        (w >= 97  && w <= 102)) {
      hex_filename.push_back(w);
    } else {
      hex_filename.push_back('6');
    }
  }

  // Calculate number based on the filename
  uint64_t xt = 0;
  uint64_t x = 0;
  while (hex_filename.length() > cutlength) {
    std::stringstream ss;
    ss.clear();
    ss << std::hex << hex_filename.substr(0, cutlength);
    ss >> xt;
    x += xt;
    hex_filename = hex_filename.substr(cutlength);
  }
  if (hex_filename.length() > 0) {
    std::stringstream ss;
    ss.clear();
    ss << std::hex << hex_filename;
    ss >> xt;
    x += xt;
  }

  // Choose the bucket wih modulo
  use_bucket = x % number_of_buckets_;

  return use_bucket;
}


void S3Uploader::FileUpload(
  const std::string &local_path,
  const std::string &remote_path,
  const CallbackTN  *callback
) {
  // Choose S3 account and bucket based on the target
  std::string access_key, secret_key, bucket_name;
  const std::string mangled_filename = repository_alias_ + "/" + remote_path;
  GetKeysAndBucket(mangled_filename, &access_key, &secret_key, &bucket_name);

  s3fanout::JobInfo *info =
      new s3fanout::JobInfo(access_key,
                            secret_key,
                            full_host_name_,
                            bucket_name,
                            mangled_filename,
                            const_cast<void*>(
                                static_cast<void const*>(callback)),
                            local_path);

  if (remote_path.compare(0, 6, ".cvmfs") == 0) {
    info->request = s3fanout::JobInfo::kReqPutNoCache;
  } else {
#ifndef S3_UPLOAD_OBJECTS_EVEN_IF_THEY_EXIST
    info->request = s3fanout::JobInfo::kReqHead;
#endif
  }

  // Upload job
  const bool retval = UploadJobInfo(info);
  assert(retval);

  LogCvmfs(kLogUploadS3, kLogDebug,
           "Uploading from file finished: %s",
           local_path.c_str());
}


bool S3Uploader::UploadJobInfo(s3fanout::JobInfo *info) {
  LogCvmfs(kLogUploadS3, kLogDebug,
           "Uploading from %s:\n"
           "--> Object: '%s'\n"
           "--> Bucket: '%s'\n"
           "--> Host:   '%s'\n",
           info->origin_mem.data != NULL ? "buffer" : "file",
           info->object_key.c_str(),
           info->bucket.c_str(),
           info->hostname.c_str());

  if (s3fanout_mgr_.PushNewJob(info) != 0) {
    LogCvmfs(kLogUploadS3, kLogStderr, "Failed to upload object: %s" ,
             info->object_key.c_str());
    return false;
  }

  return true;
}


/**
 * Creates and opens a temporary file.
 *
 * @param path The created file path will be saved here
 * return file id, -1 if failure
 */
int S3Uploader::CreateAndOpenTemporaryChunkFile(std::string *path) const {
  const std::string tmp_path = CreateTempPath(temporary_path_ + "/chunk",
                                              kDefaultFileMode);
  if (tmp_path.empty()) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Failed to create temp file for "
             "upload of file chunk.");
    atomic_inc32(&copy_errors_);
    return -1;
  }

  const int tmp_fd = open(tmp_path.c_str(), O_WRONLY);
  if (tmp_fd < 0) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Failed to open temp file '%s' for "
             "upload of file chunk (errno: %d)",
             tmp_path.c_str(), errno);
    unlink(tmp_path.c_str());
    atomic_inc32(&copy_errors_);
    return tmp_fd;
  }

  *path = tmp_path;
  return tmp_fd;
}


UploadStreamHandle *S3Uploader::InitStreamedUpload(const CallbackTN *callback) {
  std::string tmp_path;
  const int tmp_fd = CreateAndOpenTemporaryChunkFile(&tmp_path);

  LogCvmfs(kLogUploadS3, kLogDebug,
           "InitStreamedUpload: %s", tmp_path.c_str());

  if (tmp_fd < 0) {
    LogCvmfs(kLogUploadS3, kLogStderr,
             "Failed to open file (%d), %s",
             errno, strerror(errno));

    return NULL;
  }

  return new S3StreamHandle(callback, tmp_fd, tmp_path);
}


void S3Uploader::Upload(UploadStreamHandle  *handle,
                        CharBuffer          *buffer,
                        const CallbackTN    *callback) {
  assert(buffer->IsInitialized());
  S3StreamHandle *local_handle = static_cast<S3StreamHandle*>(handle);

  LogCvmfs(kLogUploadS3, kLogDebug, "Upload target = %s",
           local_handle->temporary_path.c_str());

  const size_t bytes_written = write(local_handle->file_descriptor,
                                     buffer->ptr(),
                                     buffer->used_bytes());
  if (bytes_written != buffer->used_bytes()) {
    const int cpy_errno = errno;
    LogCvmfs(kLogUploadS3, kLogStderr, "failed to write %d bytes to '%s' "
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


void S3Uploader::FinalizeStreamedUpload(UploadStreamHandle  *handle,
                                        const shash::Any    &content_hash) {
  int retval = 0;
  S3StreamHandle *local_handle = static_cast<S3StreamHandle*>(handle);

  retval = close(local_handle->file_descriptor);
  if (retval != 0) {
    const int cpy_errno = errno;
    LogCvmfs(kLogUploadS3, kLogStderr, "failed to close temp file '%s' "
             "(errno: %d)",
             local_handle->temporary_path.c_str(), cpy_errno);
    atomic_inc32(&copy_errors_);
    Respond(handle->commit_callback, UploaderResults(cpy_errno));
    return;
  }

  // Open the file for reading
  MemoryMappedFile *mmf = new MemoryMappedFile(local_handle->temporary_path);
  if (!mmf->Map()) {
    LogCvmfs(kLogUploadS3, kLogStderr, "Failed to upload %s",
             local_handle->temporary_path.c_str());
    delete mmf;
    atomic_inc32(&copy_errors_);
    Respond(handle->commit_callback,
            UploaderResults(100, local_handle->temporary_path));
    return;
  }

  // New file name based on content hash
  std::string final_path("data/" + content_hash.MakePath());

  // Choose S3 account and bucket based on the filename
  std::string access_key, secret_key, bucket_name;
  const std::string mangled_filename = repository_alias_ + "/" + final_path;
  GetKeysAndBucket(mangled_filename, &access_key, &secret_key, &bucket_name);

  s3fanout::JobInfo *info =
      new s3fanout::JobInfo(access_key,
                            secret_key,
                            full_host_name_,
                            bucket_name,
                            mangled_filename,
                            const_cast<void*>(
                                static_cast<void const*>(
                                    handle->commit_callback)),
                            mmf,
                            reinterpret_cast<unsigned char *>(mmf->buffer()),
                            static_cast<size_t>(mmf->size()));
  assert(info != NULL);

  const bool retval2 = UploadJobInfo(info);
  assert(retval2);

  LogCvmfs(kLogUploadS3, kLogDebug,
           "Uploading from stream finished: %s",
           local_handle->temporary_path.c_str());

  // Remove the temporary file
  retval = remove(local_handle->temporary_path.c_str());
  assert(retval == 0);
  delete local_handle;
}


s3fanout::JobInfo *S3Uploader::CreateJobInfo(const std::string& path) const {
  std::string access_key, secret_key, bucket_name;
  GetKeysAndBucket(path, &access_key, &secret_key, &bucket_name);

  return new s3fanout::JobInfo(access_key,
                               secret_key,
                               full_host_name_,
                               bucket_name,
                               path,
                               NULL,
                               NULL,
                               NULL,
                               0);
}


bool S3Uploader::Remove(const std::string& file_to_delete) {
  const std::string mangled_path = repository_alias_ + "/" + file_to_delete;
  s3fanout::JobInfo *info = CreateJobInfo(mangled_path);

  info->request = s3fanout::JobInfo::kReqDelete;
  bool retme = s3fanout_mgr_.DoSingleJob(info);

  delete info;
  return retme;
}


bool S3Uploader::Peek(const std::string& path) const {
  const std::string mangled_path = repository_alias_ + "/" + path;
  s3fanout::JobInfo *info = CreateJobInfo(mangled_path);

  info->request = s3fanout::JobInfo::kReqHead;
  bool retme = s3fanout_mgr_.DoSingleJob(info);

  delete info;
  return retme;
}


bool S3Uploader::PlaceBootstrappingShortcut(const shash::Any &object) const {
  return false;  // TODO(rmeusel): implement
}

}  // namespace upload
