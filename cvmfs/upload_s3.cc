/**
 * This file is part of the CernVM File System.
 */
#include "upload_s3.h"

#include "s3fanout.h"
#include "logging.h"
#include "compression.h"
#include "util.h"
#include "file_processing/char_buffer.h"

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

  s3fanout_mgr = s3fanout::S3FanoutManager::Instance();

  atomic_init32(&copy_errors_);
}


bool S3Uploader::ParseSpoolerDefinition(const SpoolerDefinition &spooler_definition) {
  // Default Spooler Configuration Scheme:
  // <host name>[:port]@<access key>@<secret key>@<bucket name>

  std::vector<std::string> config = SplitString(spooler_definition.spooler_configuration, '@');
  if (config.size() != 4) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to parse S3 spooler definition "
	     "string: %s",
             spooler_definition.spooler_configuration.c_str());
    return false;
  }

  std::vector<std::string> host = SplitString(config[0], ':');
  if (host.empty() || host.size() > 2) {
    LogCvmfs(kLogSpooler, kLogStderr, "Failed to parse S3 host: %s",
             config[0].c_str());
    return true;
  }

  const std::string kStandardPort = "80";
  number_of_buckets_ = 1;
  host_name_         = host[0];
  full_host_name_    = host[0] + ":" + ((host.size() == 2) ? host[1] : kStandardPort);
  keys_.push_back(std::make_pair(config[1], config[2]));
  bucket_body_name_  = config[3];

  return true;
}


bool S3Uploader::WillHandle(const SpoolerDefinition &spooler_definition) {
  return spooler_definition.driver_type == SpoolerDefinition::S3;
}


unsigned int S3Uploader::GetNumberOfErrors() const {
  return atomic_read32(&copy_errors_);
}


void S3Uploader::WorkerThread() {

  LogCvmfs(kLogS3Fanout, kLogDebug, "Upload_S3 WorkerThread started.");

  bool running = true;
  while (running) {
    UploadJob job; 

    //LogCvmfs(kLogS3Fanout, kLogDebug, "Upload_S3 running.");

    // Check for new jobs, if they can be pushed in
    bool newjob=true;
    while(newjob) {
      if(s3fanout_mgr->PushAcquire() != 0)
	break;
      newjob = TryToAcquireNewJob(job);

      if(newjob) {
	switch (job.type) {
	case UploadJob::Upload:
	  s3fanout_mgr->PushRelease();
	  Upload(job.stream_handle,
		 job.buffer,
		 job.callback);
	  break;
	case UploadJob::Commit:
	  FinalizeStreamedUpload(job.stream_handle,
				 job.content_hash,
				 job.hash_suffix);
	  break;
	case UploadJob::Terminate:
	  s3fanout_mgr->PushRelease();
	  running = false;
	  break;
	default:
	  const bool unknown_job_type = false;
	  assert (unknown_job_type);
	  break;
	}
      }else{
	s3fanout_mgr->PushRelease();
      }
    }

    // Check CURL activity
    std::vector<s3fanout::JobInfo*> jobs;
    jobs.clear();
    s3fanout_mgr->GetCompletedJobs(jobs);
    for(std::vector<s3fanout::JobInfo*>::iterator it = jobs.begin(); it != jobs.end(); ++it) {
      // Report and clean completed jobs
      s3fanout::JobInfo *info = *it;
      Respond((callback_t*)info->callback, UploaderResults(0));
      info->mmf->Unmap();
      delete info->mmf;
    }

    usleep(100);
  }

  LogCvmfs(kLogS3Fanout, kLogDebug, "Upload_S3 WorkerThread finished.");
}


void S3Uploader::FileUpload(const std::string &local_path,
			    const std::string &remote_path,
			    const callback_t  *callback) {

  while(s3fanout_mgr->PushAcquire() != 0) {
    usleep(100*1000);
  }

  // Check that we can read the given file
  MemoryMappedFile *mmf = new MemoryMappedFile(local_path);
  if (! mmf->Map()) {
    LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to upload %s",
	     local_path.c_str());
    atomic_inc32(&copy_errors_);
    Respond(callback, UploaderResults(100, local_path));
    return;
  }

  LogCvmfs(kLogS3Fanout, kLogDebug,
	   "Uploading from file:\n"
	   "--> File:       %s\n"
	   "--> Hostname:   %s\n"
	   "--> Bucket:     %s\n"
	   "--> File size:  %d\n", 
	   remote_path.data(),
	   full_host_name_.c_str(),
	   bucket_body_name_.c_str(),
	   mmf->size());

  // Try to upload the file
  int res = -1, retries = 5;
  do {
    if(retries < 5) {
      usleep(50*1000);
    }
    res = uploadFile(remote_path, (char*)mmf->buffer(), mmf->size(),
		     0, callback, mmf);
  }while(retries-- > 0 && res != 0);

  LogCvmfs(kLogS3Fanout, kLogDebug, "Uploading from file finished: %s", local_path.c_str());
}


/**
 * Upload file from memory mapped file
 */
int S3Uploader::uploadFile(std::string       filename, 
			   char              *buff, 
			   unsigned long     size_of_file,
			   int               block_until_finished,
			   const callback_t  *callback,
			   MemoryMappedFile  *mmf) {
  int retme = 0, k = 0;

  s3fanout::JobInfo *info = new s3fanout::JobInfo(&keys_.at(k).first,  // access key
						  &keys_.at(k).second, // secret key
						  host_name_,
						  &bucket_body_name_,
						  filename,            // hash 
						  (unsigned char*)buff, 
						  size_of_file);
  info->request = s3fanout::JobInfo::kReqPut;
  info->origin_mem.pos = 0;
  info->callback = (void *)callback;
  info->mmf = mmf;

  if(s3fanout_mgr->Push(info) != 0) {
    // FIXME: report failure?
    LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to upload file: %s" ,
	     filename.data());
    return -1;
  }

  return retme;
}


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


/**
 * Upload file from stream, finished with FinalizeStreamedUpload method.
 */
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


/**
 * Finish uploading file from a stream.
 */
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

  LogCvmfs(kLogS3Fanout, kLogDebug, 
	   "Uploading from stream:\n"
	   "--> File:       '%s'\n"
	   "--> Hostname:   '%s'\n"
	   "--> Bucket:     '%s'\n"
	   "--> File size:  '%d'\n", 
	   final_path.c_str(),
	   full_host_name_.c_str(),
	   bucket_body_name_.c_str(),
	   mmf->size());  

  // We try few times
  const callback_t *callback = handle->commit_callback;
  int res = -1, retries = 5;
  do {
    if(retries < 5) {
      usleep(50*1000);
    }
    res = uploadFile(final_path, 
		     (char*)mmf->buffer(), 
		     (long unsigned int)mmf->size(),
		     0, callback, mmf);
  }while(retries-- > 0 && res != 0);

  if (res != 0) {
    const int cpy_errno = errno;
    LogCvmfs(kLogS3Fanout, kLogStderr, "failed to move temp file '%s' to "
	                               "final location '%s' (errno: %d)",
             local_handle->temporary_path.c_str(),
             final_path.c_str(),
             cpy_errno);
    atomic_inc32(&copy_errors_);
    Respond(handle->commit_callback, UploaderResults(cpy_errno));
    return;
  }

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
