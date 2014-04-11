/**
 * This file is part of the CernVM File System.
 */
#include "upload_s3.h"

#include "s3fanout.h"
#include "logging.h"
#include "compression.h"
#include "util.h"
#include "file_processing/char_buffer.h"
#include "options.h"

#include <sstream>

using namespace upload;


S3Uploader::S3Uploader(const SpoolerDefinition &spooler_definition) :
  AbstractUploader(spooler_definition),
  upstream_path_(spooler_definition.spooler_configuration),
  temporary_path_(spooler_definition.temporary_path)
{
  if (! ParseSpoolerDefinition(spooler_definition)) {
    abort();
  }

  assert (spooler_definition.IsValid() &&
          spooler_definition.driver_type == SpoolerDefinition::S3);

  s3fanout_mgr = s3fanout::S3FanoutManager::Instance(maximum_number_of_parallell_uploads_);

  atomic_init32(&copy_errors_);
}


bool S3Uploader::ParseSpoolerDefinition(const SpoolerDefinition &spooler_definition) {
  // Spooler Configuration Scheme:
  // <host name>[:port]@<config_file>

  std::vector<std::string> config = SplitString(spooler_definition.spooler_configuration, '@');
  if (config.size() != 2) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to parse S3 spooler definition "
	     "string: %s",
             spooler_definition.spooler_configuration.c_str());
    return false;
  }

  std::vector<std::string> host = SplitString(config[0], ':');
  if (host.empty() || host.size() > 2) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to parse S3 host: %s",
             config[0].c_str());
    return false;
  }

  // Parse S3 configuration
  options::Init();
  options::ParsePath(config[1]);
  std::string parameter;   
  int s3_accounts=1;
  if (options::GetValue("S3_ACCOUNTS", &parameter)) {
    s3_accounts = String2Uint64(parameter);    
  }
  int s3_buckets_per_account=1;
  if (options::GetValue("S3_BUCKETS_PER_ACCOUNT", &parameter)) {
    s3_buckets_per_account = String2Uint64(parameter);
    if(s3_buckets_per_account < 1 || s3_buckets_per_account > 100) {
      LogCvmfs(kLogSpooler, kLogStderr, "Fail, invalid S3_BUCKETS_PER_ACCOUNT given: '%d'.", 
	       s3_buckets_per_account);
      LogCvmfs(kLogSpooler, kLogStderr, "S3_BUCKETS_PER_ACCOUNT should be in range 1-100.");
      return false;
    }
  }
  std::string s3_access_key;
  if (!options::GetValue("S3_ACCESS_KEY", &s3_access_key)) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to parse S3_ACCESS_KEY from '%s'.", 
	     config[1].c_str());
    return false;
  }
  std::string s3_secret_key;
  if (!options::GetValue("S3_SECRET_KEY", &s3_secret_key)) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to parse S3_SECRET_KEY from '%s'.", 
	     config[1].c_str());
    return false;
  }
  if (!options::GetValue("S3_BUCKET", &bucket_body_name_)) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to parse S3_BUCKET from '%s'.", 
	     config[1].c_str());
    return false;
  }
  if (!options::GetValue("MAX_NUMBER_OF_PARALLELL_CONNECTIONS", &parameter)) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to parse MAX_NUMBER_OF_PARALLELL_CONNECTIONS from '%s'.", 
	     config[1].c_str());
    return false;
  }
  maximum_number_of_parallell_uploads_ = String2Uint64(parameter);
  options::Fini();

  const std::string kStandardPort = "80";
  number_of_buckets_ = s3_buckets_per_account*s3_accounts;
  host_name_         = host[0];
  full_host_name_    = host[0] + ":" + ((host.size() == 2) ? host[1] : kStandardPort);
  if(s3_accounts==1) {
    keys_.push_back(std::make_pair(s3_access_key, s3_secret_key)); 
  }else if(s3_accounts>1) {
    std::vector<std::string> s3_access_keys = SplitString(s3_access_key, ':');
    std::vector<std::string> s3_secret_keys = SplitString(s3_secret_key, ':');
    if(s3_access_keys.size() != (unsigned int)s3_accounts ||
       s3_secret_keys.size() != (unsigned int)s3_accounts) {
      LogCvmfs(kLogSpooler, kLogStderr, "Fail, number of accounts does not match with number of keys: '%d'.", 
	       s3_accounts);
      LogCvmfs(kLogSpooler, kLogStderr, "Specify keys like this: S3_ACCESS_KEY=key1:key2:key3:...");
      return false;
    }
    for (int i=0; s3_accounts>i; i++) {
      keys_.push_back(std::make_pair(s3_access_keys.at(i), s3_secret_keys.at(i))); 
    }
  }else {
    LogCvmfs(kLogSpooler, kLogStderr, "Fail, invalid number of accounts specified: '%d'.", 
	     s3_accounts);
    return false;
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
    if(newjob) {
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
	assert (unknown_job_type);
	break;
      }
    }

    // Get and clean completed jobs
    std::vector<s3fanout::JobInfo*> jobs;
    jobs.clear();
    s3fanout_mgr->PopCompletedJobs(jobs);
    for(std::vector<s3fanout::JobInfo*>::iterator it = jobs.begin(); it != jobs.end(); ++it) {
      // Report and clean completed jobs
      s3fanout::JobInfo *info = *it;
      if(info->error_code == s3fanout::kFailOk) {
	Respond((callback_t*)info->callback, UploaderResults(0));
      } else {
	Respond((callback_t*)info->callback, UploaderResults(99, info->mmf->file_path()));
      }
      info->mmf->Unmap();
      delete info->mmf;
    }

    usleep(100);
  }

  LogCvmfs(kLogS3Fanout, kLogDebug, "Upload_S3 WorkerThread finished.");
}


/**
 * Returns the access/secret key index of requested bucket
 *
 * @param use_bucket Bucket to use between 0-(number_of_buckets_-1)
 * @return Index to the key to be used
 */
int S3Uploader::getKeyIndex(unsigned int use_bucket) {
  if(use_bucket >= (unsigned int)number_of_buckets_) {
    return 0;
  }
  return use_bucket%keys_.size();
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
int S3Uploader::getKeysAndBucket(const std::string filename,
				 std::string &access_key,
				 std::string &secret_key,
				 std::string &bucket_name) {
  unsigned int use_bucket = select_bucket(filename); 
  bucket_name = getBucketName(use_bucket);  

  int k = getKeyIndex(use_bucket);
  access_key = keys_.at(k).first;
  secret_key = keys_.at(k).second;

  return 0;
}


/**
 * Returns bucket name based on bucket number
 *
 * @param use_bucket Bucket to use between 0-(number_of_buckets_-1)
 * @return bucket name
 */
std::string S3Uploader::getBucketName(unsigned int use_bucket) {
  std::stringstream ss;
  if(use_bucket >= (unsigned int)number_of_buckets_) {
    ss<<bucket_body_name_<<"-1-1";
    return ss.str();
  }

  int i = use_bucket/keys_.size();
  int k = getKeyIndex(use_bucket);
  ss <<bucket_body_name_<<"-"<<k+1<<"-"<<i+1;

  return ss.str();
}

/**
 * Choose bucket according to filename
 *
 * @param rem_filename Filename to map into bucket
 * @return bucket index, between 0 and (number_of_buckets_-1)
 */
int S3Uploader::select_bucket(std::string rem_filename) {
    unsigned int use_bucket = 0;
    unsigned int cutlength= 3;     // Process filename in parts of this length
    std::string hex_filename;      // Filename with only valid hex-symbols

    // Accept only hex chars
    for(unsigned int i =0;rem_filename.length()>i;i++) {
      char w = rem_filename.at(i);
      if((w >= 48 && w <= 57) || 
	 (w >=65  && w <= 70) ||
	 (w >=97  && w <= 102)) {
	hex_filename.push_back(w);
      }else{
	hex_filename.push_back('6');
      }
    }

    // Calculate number based on the filename
    unsigned long xt = 0, x = 0; 
    while(hex_filename.length() > cutlength) {
      std::stringstream ss;
      ss.clear();
      ss << std::hex << hex_filename.substr(0,cutlength); 
      ss >> xt;
      x += xt;
      hex_filename = hex_filename.substr(cutlength);
    }
    if(hex_filename.length() > 0) {
      std::stringstream ss;
      ss.clear();
      ss << std::hex << hex_filename;
      ss >> xt;
      x += xt;
    }

    // Choose the bucket wih modulo
    use_bucket = x%number_of_buckets_;

    return use_bucket;
}


void S3Uploader::FileUpload(const std::string &local_path,
			    const std::string &remote_path,
			    const callback_t  *callback) {

  // Check that we can read the given file
  MemoryMappedFile *mmf = new MemoryMappedFile(local_path);
  if (! mmf->Map()) {
    LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to upload %s",
	     local_path.c_str());
    atomic_inc32(&copy_errors_);
    Respond(callback, UploaderResults(100, local_path));
    return;
  }

  // Try to upload the file
  assert(uploadFile(remote_path, (char*)mmf->buffer(), 
		    mmf->size(), callback, mmf) == 0);

  LogCvmfs(kLogS3Fanout, kLogDebug, "Uploading from file finished: %s", local_path.c_str());
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
int S3Uploader::uploadFile(std::string       filename, 
			   char              *buff, 
			   unsigned long     size_of_file,
			   const callback_t  *callback,
			   MemoryMappedFile  *mmf) {

  // Choose S3 account and bucket based on the filename
  std::string access_key, secret_key, bucket_name;
  getKeysAndBucket(filename, access_key, secret_key, bucket_name);

  s3fanout::JobInfo *info = new s3fanout::JobInfo(access_key,
						  secret_key,
						  full_host_name_,
						  bucket_name,
						  filename,
						  (unsigned char*)buff, 
						  size_of_file);
  info->request = s3fanout::JobInfo::kReqPut;
  info->origin_mem.pos = 0;
  info->callback = (void *)callback;
  info->mmf = mmf;

  LogCvmfs(kLogS3Fanout, kLogDebug, 
	   "Uploading file:\n"
	   "--> File:        '%s'\n"
	   "--> Hostname:    '%s'\n"
	   "--> Bucket:      '%s'\n"
	   "--> File size:   '%d'\n", 
	   filename.c_str(),
	   full_host_name_.c_str(),
	   bucket_name.c_str(),	   
	   mmf->size());  

  if(s3fanout_mgr->PushNewJob(info) != 0) {
    LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to upload file: %s" ,
	     filename.data());
    return -1;
  }

  return 0;
}


/**
 * Creates and opens a temporary file.
 *
 * @param path The created file path will be saved here
 * return file id, -1 if failure
 */
int S3Uploader::CreateAndOpenTemporaryChunkFile(std::string *path) const {
  const std::string tmp_path = CreateTempPath(temporary_path_ + "/" + "chunk",
                                              0644);
  if (tmp_path.empty()) {
    LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to create temp file for "
                                          "upload of file chunk.");
    atomic_inc32(&copy_errors_);
    return -1;
  }

  const int tmp_fd = open(tmp_path.c_str(), O_WRONLY);
  if (tmp_fd < 0) {
    LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to open temp file '%s' for "
                                       "upload of file chunk (errno: %d)",
             tmp_path.c_str(), errno);
    unlink(tmp_path.c_str());
    atomic_inc32(&copy_errors_);
    return tmp_fd;
  }

  *path = tmp_path;
  return tmp_fd;
}


UploadStreamHandle* S3Uploader::InitStreamedUpload(const callback_t *callback) {
  std::string tmp_path;
  const int tmp_fd = CreateAndOpenTemporaryChunkFile(&tmp_path);

  LogCvmfs(kLogS3Fanout, kLogDebug, "InitStreamedUpload: %s", tmp_path.c_str());

  if (tmp_fd < 0) {
    LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to open file (%d), %s",
	     errno,strerror(errno));

    return NULL;
  }

  return new S3StreamHandle(callback, tmp_fd, tmp_path);
}


void S3Uploader::Upload(UploadStreamHandle  *handle,
			CharBuffer          *buffer,
			const callback_t    *callback) {
  assert (buffer->IsInitialized());
  S3StreamHandle *local_handle = static_cast<S3StreamHandle*>(handle);

  LogCvmfs(kLogS3Fanout, kLogDebug, "Upload target = %s", local_handle->temporary_path.c_str());

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
					const shash::Any    content_hash,
					const std::string   hash_suffix) {
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
  if (! mmf->Map()) {
    LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to upload %s",
	     local_handle->temporary_path.c_str());
    atomic_inc32(&copy_errors_);
    Respond(handle->commit_callback, UploaderResults(100, local_handle->temporary_path));
    return;
  }

  // New file name based on content hash
  std::string final_path("data" +
			 content_hash.MakePath(1, 2) +
			 hash_suffix);

  // Request upload
  const callback_t *callback = handle->commit_callback;
  assert(uploadFile(final_path, 
		    (char*)mmf->buffer(), 
		    (long unsigned int)mmf->size(),
		    callback, mmf) == 0);

  LogCvmfs(kLogS3Fanout, kLogDebug, "Uploading from stream finished: %s", local_handle->temporary_path.c_str());

  // Remove the temporary file
  remove(local_handle->temporary_path.c_str());
  delete local_handle;
}


bool S3Uploader::Remove(const std::string& file_to_delete) {
  if (! Peek(file_to_delete)) {
    return false;
  }

  LogCvmfs(kLogS3Fanout, kLogStderr, "Error: Remove is not implemented");
  return false;
}


bool S3Uploader::Peek(const std::string& path) const {
  LogCvmfs(kLogS3Fanout, kLogStderr, "Error: Peek is not implemented");
  return false;
}


int S3Uploader::Move(const std::string &local_path,
                        const std::string &remote_path) const {
  LogCvmfs(kLogS3Fanout, kLogStderr, "Error: Move is not implemented");
  return -1;
}
