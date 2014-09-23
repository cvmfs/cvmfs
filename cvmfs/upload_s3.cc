/**
 * This file is part of the CernVM File System.
 */

#include "upload_s3.h"

#include <errno.h>
#include <unistd.h>
#ifdef _POSIX_PRIORITY_SCHEDULING
#include <sched.h>
#endif

#include <vector>
#include <string>
#include <sstream>  // TODO: remove me

#include "compression.h"
#include "file_processing/char_buffer.h"
#include "logging.h"
#include "options.h"
#include "s3fanout.h"
#include "util.h"

using namespace upload;

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
    LogCvmfs(kLogSpooler, kLogStderr,
             "Failed to parse spooler configuration "
             "string '%s'.\n"
             "Provide: <repo_alias>@/path/to/s3.conf",
             spooler_definition.spooler_configuration.c_str());
    return false;
  }
  repository_alias_              = config[0];
  const std::string &config_path = config[1];

  if (!FileExists(config_path)) {
    LogCvmfs(kLogSpooler, kLogStderr,
             "Cannot find S3 config file at '%s'",
             config_path.c_str());
    return false;
  }

  // Parse S3 configuration
  // TODO: separate option handling and sanity checks
  options::Init();
  options::ParsePath(config_path, false);
  std::string parameter;
  std::string s3_host;
  if (!options::GetValue("S3_HOST", &s3_host)) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to parse S3_HOST from '%s'",
             config_path.c_str());
    return false;
  }
  const std::string kStandardPort = "80";
  std::string s3_port = kStandardPort;
  options::GetValue("S3_PORT", &s3_port);
  int s3_buckets_per_account = 1;
  if (options::GetValue("S3_BUCKETS_PER_ACCOUNT", &parameter)) {
    s3_buckets_per_account = String2Uint64(parameter);
    if (s3_buckets_per_account < 1 || s3_buckets_per_account > 100) {
      LogCvmfs(kLogSpooler, kLogStderr, "Fail, invalid S3_BUCKETS_PER_ACCOUNT "
               "given: '%d'.",
               s3_buckets_per_account);
      LogCvmfs(kLogSpooler, kLogStderr,
               "S3_BUCKETS_PER_ACCOUNT should be in range 1-100.");
      return false;
    }
  }
  std::string s3_access_key;
  if (!options::GetValue("S3_ACCESS_KEY", &s3_access_key)) {
    LogCvmfs(kLogSpooler, kLogStderr,
             "Failed to parse S3_ACCESS_KEY from '%s'.",
             config_path.c_str());
    return false;
  }
  std::string s3_secret_key;
  if (!options::GetValue("S3_SECRET_KEY", &s3_secret_key)) {
    LogCvmfs(kLogSpooler, kLogStderr,
             "Failed to parse S3_SECRET_KEY from '%s'.",
             config_path.c_str());
    return false;
  }
  if (!options::GetValue("S3_BUCKET", &bucket_body_name_)) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to parse S3_BUCKET from '%s'.",
             config_path.c_str());
    return false;
  }
  if (!options::GetValue("S3_MAX_NUMBER_OF_PARALLELL_CONNECTIONS",
                         &parameter)) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to parse "
             "S3_MAX_NUMBER_OF_PARALLELL_CONNECTIONS "
             "from '%s'.",
             config_path.c_str());
    return false;
  }
  max_num_parallel_uploads_ = String2Uint64(parameter);
  options::Fini();

  std::vector<std::string> s3_access_keys = SplitString(s3_access_key, ':');
  std::vector<std::string> s3_secret_keys = SplitString(s3_secret_key, ':');
  if (s3_access_keys.size() != s3_secret_keys.size()) {
    LogCvmfs(kLogSpooler, kLogStderr,
             "Failure, number of accounts does not match");
    LogCvmfs(kLogSpooler, kLogStderr,
             "Specify keys like this: "
             "S3_ACCESS_KEY=key1:key2:key3:...");
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
  LogCvmfs(kLogS3Fanout, kLogDebug, "Upload_S3 WorkerThread started.");

  bool running = true;
  while (running) {
    UploadJob job;

    // Try to get new job
    bool newjob = TryToAcquireNewJob(job);
    if (newjob) {
      switch (job.type) {
        case UploadJob::Upload:
          Upload(job.stream_handle,
                 job.buffer,
                 job.callback);
          break;
        case UploadJob::Commit:
          // Note, this block until upload is possible
          FinalizeStreamedUpload(job.stream_handle,
                                 job.content_hash,
                                 job.hash_suffix);
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

    // Get and clean completed jobs
    std::vector<s3fanout::JobInfo *> jobs;
    jobs.clear();
    s3fanout_mgr_.PopCompletedJobs(&jobs);
    std::vector<s3fanout::JobInfo*>::iterator             it    = jobs.begin();
    const std::vector<s3fanout::JobInfo*>::const_iterator itend = jobs.end();
    for (; it != itend; ++it) {
      // Report and clean completed jobs
      s3fanout::JobInfo *info = *it;
      if (info->error_code == s3fanout::kFailOk) {
        Respond(static_cast<callback_t*>(info->callback),
                UploaderResults(0));
      } else {
        LogCvmfs(kLogS3Fanout, kLogStderr, "Upload job for '%s' failed. "
                                           "(error code: %d - %s)",
                 info->origin_path.c_str(), info->error_code,
                 s3fanout::Code2Ascii(info->error_code));

        Respond(static_cast<callback_t*>(info->callback),
                UploaderResults(99, info->mmf->file_path()));
      }
      info->mmf->Unmap();
      delete info->mmf;
    }
#ifdef _POSIX_PRIORITY_SCHEDULING
    sched_yield();
#endif
  }

  LogCvmfs(kLogS3Fanout, kLogDebug, "Upload_S3 WorkerThread finished.");
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
  unsigned long xt = 0, x = 0;
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


void S3Uploader::FileUpload(const std::string &local_path,
                            const std::string &remote_path,
                            const callback_t  *callback) {
  // Check that we can read the given file
  MemoryMappedFile *mmf = new MemoryMappedFile(local_path);
  if (!mmf->Map()) {
    LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to upload %s",
             local_path.c_str());
    atomic_inc32(&copy_errors_);
    Respond(callback, UploaderResults(100, local_path));
    return;
  }

  // Try to upload the file
  const bool retval = UploadFile(remote_path,
                                 reinterpret_cast<char*>(mmf->buffer()),
                                 mmf->size(), callback, mmf);
  assert(retval);

  LogCvmfs(kLogS3Fanout, kLogDebug,
           "Uploading from file finished: %s",
           local_path.c_str());
}


/**
 * Request file to be uploaded to S3. Non-blocking request, i.e. does
 * not wait for completion.
 *
 * @param filename The name to be used for storing in S3
 * @param buff Data to be stored
 * @param size_of_file Size of the data to be stored
 * @param callback Callback that is called when upload is completed
 * @param mmf Memory mapped file (if one is used)
 * @return 0 if all is ok
 */
bool S3Uploader::UploadFile(const std::string &filename,
                            char              *buff,
                            unsigned long     size_of_file,
                            const callback_t  *callback,
                            MemoryMappedFile  *mmf)
{
  // Choose S3 account and bucket based on the filename
  std::string access_key, secret_key, bucket_name;
  const std::string mangled_filename = repository_alias_ + "/" + filename;
  GetKeysAndBucket(mangled_filename, &access_key, &secret_key, &bucket_name);

  s3fanout::JobInfo *info = new s3fanout::JobInfo(access_key,
                                                  secret_key,
                                                  full_host_name_,
                                                  bucket_name,
                                                  mangled_filename,
                                                  (unsigned char*)buff,
                                                  size_of_file);
  info->request        = s3fanout::JobInfo::kReqPut;
#ifndef S3_UPLOAD_OBJECTS_EVEN_IF_THEY_EXIST
  if (filename.substr(0, 1) != ".") {
    info->request        = s3fanout::JobInfo::kReqHead;
  }
#endif
  info->origin_mem.pos = 0;
  info->callback       = const_cast<void*>(static_cast<void const*>(callback));
  info->mmf            = mmf;

  LogCvmfs(kLogS3Fanout, kLogDebug,
           "Uploading file:\n"
           "--> File:        '%s'\n"
           "--> Hostname:    '%s'\n"
           "--> Bucket:      '%s'\n"
           "--> File size:   '%d'\n",
           mangled_filename.c_str(),
           full_host_name_.c_str(),
           bucket_name.c_str(),
           mmf->size());

  if (s3fanout_mgr_.PushNewJob(info) != 0) {
    LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to upload file: %s" ,
             mangled_filename.data());
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
    LogCvmfs(kLogS3Fanout, kLogStderr,
             "Failed to create temp file for "
             "upload of file chunk.");
    atomic_inc32(&copy_errors_);
    return -1;
  }

  const int tmp_fd = open(tmp_path.c_str(), O_WRONLY);
  if (tmp_fd < 0) {
    LogCvmfs(kLogS3Fanout, kLogStderr,
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


UploadStreamHandle *S3Uploader::InitStreamedUpload(const callback_t *callback) {
  std::string tmp_path;
  const int tmp_fd = CreateAndOpenTemporaryChunkFile(&tmp_path);

  LogCvmfs(kLogS3Fanout, kLogDebug,
           "InitStreamedUpload: %s", tmp_path.c_str());

  if (tmp_fd < 0) {
    LogCvmfs(kLogS3Fanout, kLogStderr,
             "Failed to open file (%d), %s",
             errno, strerror(errno));

    return NULL;
  }

  return new S3StreamHandle(callback, tmp_fd, tmp_path);
}


void S3Uploader::Upload(UploadStreamHandle  *handle,
                        CharBuffer          *buffer,
                        const callback_t    *callback) {
  assert(buffer->IsInitialized());
  S3StreamHandle *local_handle = static_cast<S3StreamHandle*>(handle);

  LogCvmfs(kLogS3Fanout, kLogDebug, "Upload target = %s",
           local_handle->temporary_path.c_str());

  const size_t bytes_written = write(local_handle->file_descriptor,
                                     buffer->ptr(),
                                     buffer->used_bytes());
  if (bytes_written != buffer->used_bytes()) {
    const int cpy_errno = errno;
    LogCvmfs(kLogS3Fanout, kLogStderr, "failed to write %d bytes to '%s' "
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


void S3Uploader::FinalizeStreamedUpload(UploadStreamHandle *handle,
                                        const shash::Any   &content_hash,
                                        const std::string  &hash_suffix) {
  int retval = 0;
  S3StreamHandle *local_handle = static_cast<S3StreamHandle*>(handle);

  retval = close(local_handle->file_descriptor);
  if (retval != 0) {
    const int cpy_errno = errno;
    LogCvmfs(kLogS3Fanout, kLogStderr, "failed to close temp file '%s' "
             "(errno: %d)",
             local_handle->temporary_path.c_str(), cpy_errno);
    atomic_inc32(&copy_errors_);
    Respond(handle->commit_callback, UploaderResults(cpy_errno));
    return;
  }

  // Open the file for reading
  MemoryMappedFile *mmf = new MemoryMappedFile(local_handle->temporary_path);
  if (!mmf->Map()) {
    LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to upload %s",
             local_handle->temporary_path.c_str());
    atomic_inc32(&copy_errors_);
    Respond(handle->commit_callback,
            UploaderResults(100, local_handle->temporary_path));
    return;
  }

  // New file name based on content hash
  std::string final_path("data" +
                         content_hash.MakePath(1, 2) +
                         hash_suffix);

  // Request upload
  const callback_t *callback = handle->commit_callback;
  const bool retval_b = UploadFile(final_path,
                                   reinterpret_cast<char*>(mmf->buffer()),
                                   static_cast<long unsigned int>(mmf->size()),
                                   callback, mmf);
  assert(retval_b);

  LogCvmfs(kLogS3Fanout, kLogDebug,
           "Uploading from stream finished: %s",
           local_handle->temporary_path.c_str());

  // Remove the temporary file
  remove(local_handle->temporary_path.c_str());
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
                               0);
}


bool S3Uploader::Remove(const std::string& file_to_delete) {
  s3fanout::JobInfo *info = CreateJobInfo(file_to_delete);

  info->request = s3fanout::JobInfo::kReqDelete;
  bool retme = s3fanout_mgr_.DoSingleJob(info);

  delete info;
  return retme;
}


bool S3Uploader::Peek(const std::string& path) const {
  s3fanout::JobInfo *info = CreateJobInfo(path);

  info->request = s3fanout::JobInfo::kReqHead;
  bool retme = s3fanout_mgr_.DoSingleJob(info);

  delete info;
  return retme;
}
