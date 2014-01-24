/**
 * This file is part of the CernVM File System.
 */
#include "upload_s3.h"

#include "logging.h"
#include "compression.h"
#include "util.h"
#include "file_processing/char_buffer.h"

#include <sstream>

using namespace upload;

const std::string kStandardPort = "80";
static s3fanout::S3FanoutManager s3fanout_mgr;
static int s3fanout_mgr_inited_ = 0;
static UrlConstructor url_constructor;

S3Uploader::S3Uploader(const SpoolerDefinition &spooler_definition) :
  AbstractUploader(spooler_definition),
  upstream_path_(spooler_definition.spooler_configuration),
  temporary_path_(spooler_definition.temporary_path)
{
  if (! ParseSpoolerDefinition(spooler_definition)) {
    abort();
  }

  if(s3fanout_mgr_inited_ != 1) {
    s3fanout_mgr_inited_ = 1;
    s3fanout_mgr.Init(1024, &url_constructor); // max connections
    s3fanout_mgr.SetRetryParameters(3, 100, 2000);
    s3fanout_mgr.Spawn();
  }

  assert (spooler_definition.IsValid() &&
          spooler_definition.driver_type == SpoolerDefinition::S3);

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

  number_of_buckets_ = 1;
  full_host_name_    = host[0] + ":" + ((host.size() == 2) ? host[1] : kStandardPort);
  keys_.push_back(std::make_pair(config[1], config[2]));
  bucket_body_name_  = config[3];

  return true;
}

#define INVERSE_BUCKETS_AND_KEYS 1
// Key index of requested bucket
int S3Uploader::getKeyIndex(unsigned int use_bucket) {
    if(INVERSE_BUCKETS_AND_KEYS) {
      return use_bucket%keys_.size();
    }
    return use_bucket/number_of_buckets_;
  }

// Return the name of the bucket
std::string S3Uploader::getBucketName(unsigned int use_bucket) {
    int i = use_bucket%number_of_buckets_;
    if(INVERSE_BUCKETS_AND_KEYS) {
      i = use_bucket/keys_.size();
    }
    std::stringstream ss;
#if defined USE_UDS
    int k = getKeyIndex(use_bucket);
    ss <<bucket_body_name_<<k+2<<"-"<< i+1;
#else
    ss <<bucket_body_name_<<"-"<< i+1;
#endif
    return ss.str();
}

// Choose bucket according to filename
int S3Uploader::select_bucket(std::string rem_filename) {
    unsigned int use_bucket = 0;
    int number_of_buckets = number_of_buckets_*keys_.size(); // Number of available buckets
    unsigned int cutlength= 3;                               // Process filename in parts of this length
    std::string hex_filename;                                // Filename with only valid hex-symbols

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
    use_bucket = x%number_of_buckets;

    return use_bucket;
}

bool S3Uploader::WillHandle(const SpoolerDefinition &spooler_definition) {
  return spooler_definition.driver_type == SpoolerDefinition::S3;
}

unsigned int S3Uploader::GetNumberOfErrors() const {
  return atomic_read32(&copy_errors_);
}

void S3Uploader::WorkerThread() {

  s3fanout_mgr.watch_fds_ =
    static_cast<struct pollfd *>(smalloc(2 * sizeof(struct pollfd)));
  s3fanout_mgr.watch_fds_size_ = 2;
  s3fanout_mgr.watch_fds_inuse_ = 2;

  LogCvmfs(kLogS3Fanout, kLogDebug, "WorkerThread running.");

  bool running = true;
  while (running) {
    UploadJob job; 
   
    if(s3fanout_mgr.watch_fds_inuse_ < 300) {

    bool newjob = TryToAcquireNewJob(job);

    if(newjob) {
      switch (job.type) {
      case UploadJob::Upload:
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
        running = false;
        break;
      default:
        const bool unknown_job_type = false;
        assert (unknown_job_type);
        break;
      }
    }

    }

    int still_running = 0;
    int timeout = 1;
    int retval2 = poll(s3fanout_mgr.watch_fds_, s3fanout_mgr.watch_fds_inuse_,
                      timeout);
    if (retval2 < 0) {
      continue;
    }
 
    LogCvmfs(kLogS3Fanout, kLogDebug, "WorkerThread running - %d.", s3fanout_mgr.watch_fds_inuse_);

    // Activity on curl sockets
    for (unsigned i = 2; i < s3fanout_mgr.watch_fds_inuse_; ++i) {
      if (s3fanout_mgr.watch_fds_[i].revents) {
        int ev_bitmask = 0;
        if (s3fanout_mgr.watch_fds_[i].revents & (POLLIN | POLLPRI))
          ev_bitmask |= CURL_CSELECT_IN;
        if (s3fanout_mgr.watch_fds_[i].revents & (POLLOUT | POLLWRBAND))
          ev_bitmask |= CURL_CSELECT_OUT;
        if (s3fanout_mgr.watch_fds_[i].revents & (POLLERR | POLLHUP | POLLNVAL))
          ev_bitmask |= CURL_CSELECT_ERR;
        s3fanout_mgr.watch_fds_[i].revents = 0;

        int retval = curl_multi_socket_action(s3fanout_mgr.curl_multi_,
                                          s3fanout_mgr.watch_fds_[i].fd,
                                          ev_bitmask,
                                          &still_running);
        LogCvmfs(kLogS3Fanout, kLogDebug, "socket action on socket %d, returned with %d, still_running %d", 
		 s3fanout_mgr.watch_fds_[i].fd, retval, still_running);
      }
    }

    // Check if transfers are completed
    CURLMsg *curl_msg;
    int msgs_in_queue;
    while ((curl_msg = curl_multi_info_read(s3fanout_mgr.curl_multi_,
                                            &msgs_in_queue)))
      {
	if (curl_msg->msg == CURLMSG_DONE) {
	  s3fanout_mgr.statistics_->num_requests++;
	  s3fanout::JobInfo *info;
	  CURL *easy_handle = curl_msg->easy_handle;
	  int curl_error = curl_msg->data.result;
	  assert(curl_easy_getinfo(easy_handle, CURLINFO_PRIVATE, &info)==CURLE_OK);
	  LogCvmfs(kLogS3Fanout, kLogDebug, "Done message for curl_msg->msg: %s", info->object_key->c_str()); 
	  //: %s", info->origin_path->c_str());

	  curl_multi_remove_handle(s3fanout_mgr.curl_multi_, easy_handle);
	  if (s3fanout_mgr.VerifyAndFinalize(curl_error, info)) {
	    curl_multi_add_handle(s3fanout_mgr.curl_multi_, easy_handle);
	    //int retval = 
	    curl_multi_socket_action(s3fanout_mgr.curl_multi_,
					      CURL_SOCKET_TIMEOUT,
					      0,
					      &still_running);
	  } else {
	    // Return easy handle into pool and write result back
	    s3fanout_mgr.ReleaseCurlHandle(info, easy_handle);

	    /*	    if(info->mmf != ) {
	      Respond((callback_t*)info->callback, upload::UploaderResults(0, info->mmf->file_path()));
	      }else {*/
	    Respond((callback_t*)info->callback, UploaderResults(0));
	      //	    }
	    info->mmf->Unmap();
	    free(info->mmf);

	    /*
	    FinalReport r;
	    r.f=info->error_code;
	    r.s1=info->wait_at[0];
	    r.s2=info->wait_at[1];
	    WritePipe(info->wait_at[1], &r, sizeof(r));
	    */
	    // WritePipe(info->wait_at[1], &info->error_code,
	    // sizeof(info->error_code));
	    
	  }
	}
      }

    //usleep(1000*10);
    usleep(100);

  }

  LogCvmfs(kLogS3Fanout, kLogDebug, "WorkerThread exited.");
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

  // Select which bucket to upload
  unsigned int use_bucket = select_bucket(remote_path);
  int k = getKeyIndex(use_bucket);
  std::string *bucket_name = new std::string(getBucketName(use_bucket));

  //std::cerr<<"HERE"<<std::endl;
  LogCvmfs(kLogS3Fanout, kLogDebug,
	   "Uploading bucket from file:\n"
	   "--> File:       %s\n"
	   "--> Bucket num: %d\n"
	   "--> Hostname:   %s\n"
	   "--> Index:      %d\n" 
	   "--> Bucket:     %s\n"
	   "--> File size:  %d\n", 
	   remote_path.data(),
	   use_bucket, 
	   full_host_name_.c_str(),
	   k, 
	   bucket_name->c_str(),
	   mmf->size());
  // std::cerr<<"HERE-END"<<std::endl;

    // Try to upload the file
    int res = -1, retries = 5;
    do {
      if(retries < 5) {
	usleep(50*1000);
      }
      res = uploadFile(remote_path, (char*)mmf->buffer(), mmf->size(),
		       *bucket_name, full_host_name_, remote_path, 
		       use_bucket, 
		       0, callback, mmf);
    }while(retries-- > 0 && res != 0);

    // Report if job was success or failure
    /*
    if(res != 0) {
      LogCvmfs(kLogS3Fanout, kLogStderr, "Failed permanently to upload file: %s" ,
	       remote_path.data());
      atomic_inc32(&copy_errors_);
      Respond(callback, UploaderResults(res, local_path));
      return;
    }
     
    Respond(callback, UploaderResults(res, local_path));
    */

    LogCvmfs(kLogS3Fanout, kLogDebug, "Uploading from file finished: %s", local_path.c_str());
}

// Upload file to cloud
int S3Uploader::uploadFile(std::string   filename, 
			   char          *buff, 
			   unsigned long size_of_file,
			   const std::string   &bucket_name,
			   std::string   full_hostname,
			   const std::string   &hash,
			   int use_bucket,
			   int block_until_finished,
			   const callback_t  *callback,
			   MemoryMappedFile *mmf) {
    int retme = 0;
    //std::string url = "http://" + full_hostname + "/" + bucket_name + "/" + filename;

    // TODO: add upload magic
    int k = getKeyIndex(use_bucket);
    s3fanout::JobInfo *info = new s3fanout::JobInfo(&keys_.at(k).first,  // access key
						    &keys_.at(k).second, // secret key
						    &bucket_name,
						    &hash, 
						    (unsigned char*)buff, size_of_file);
    info->request = s3fanout::JobInfo::kReqPut;
    //s3fanout::Failures result = s3fanout::PrepareOrigin(info);
    info->origin_mem.pos = 0;
    info->callback = (void *)callback;
    info->mmf = mmf;
    /*if (result != s3fanout::kFailOk) {
      LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to upload file: %s" ,
	       filename.data());
	       }*/
				
    CURL *handle = s3fanout_mgr.AcquireCurlHandle();
    if(handle == NULL) {
      LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to acquire CURL handle.");
      return -1;
    }

    s3fanout::Failures init_failure = s3fanout_mgr.InitializeRequest(info, handle);
    if (init_failure != s3fanout::kFailOk) {
      // FIXME: report failure
      LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to upload file: %s" ,
	       filename.data());
    }
    //s3fanout_mgr.SetUrlOptions2(info, url_constructor.MkUrl(bucket_name, filename+"2"));
    s3fanout_mgr.SetUrlOptions(info);

    curl_multi_add_handle(s3fanout_mgr.curl_multi_, handle);
    int still_running = 0, retval = 0; 
    do {
      retval = curl_multi_socket_action(s3fanout_mgr.curl_multi_,
					CURL_SOCKET_TIMEOUT,
					0,
					&still_running);
      //usleep(1000*100);
    } while(block_until_finished == 1 && still_running == 1);

    LogCvmfs(kLogS3Fanout, kLogDebug, "curl_multi_socket_action: %d - %d", retval, still_running);

    //usleep(1000*1000*2);

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
                                          "upload of file chunk (errno: %d",
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
 * Write data to temporary disk - we upload it in the
 * final phase.
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


void S3Uploader::FinalizeStreamedUpload(UploadStreamHandle *handle,
					const shash::Any    content_hash,
					const std::string   hash_suffix) {
  int retval = 0;
  S3StreamHandle *local_handle = static_cast<S3StreamHandle*>(handle);

  /*
  struct timespec start, stop;
  double duration = 0;
  clock_gettime(CLOCK_REALTIME,&start);
  clock_gettime(CLOCK_REALTIME,&stop);
  duration= (stop.tv_sec - start.tv_sec) + (double)(stop.tv_nsec - start.tv_nsec) / 1000000000.0;
  LogCvmfs(kLogS3Fanout, kLogDebug, "Elapsed 1: %.9f", duration);
  */

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
  std::string *final_path = new std::string("data" +
					    content_hash.MakePath(1, 2) +
					    hash_suffix);

  // Select which bucket to upload
  unsigned int use_bucket = select_bucket(*final_path);
  int k = getKeyIndex(use_bucket);
  std::string *bucket_name = new std::string(getBucketName(use_bucket));

  //std::cerr<<"HERE2"<<std::endl;
  LogCvmfs(kLogS3Fanout, kLogDebug, 
	   "Uploading bucket from stream:\n"
	   "--> File:       '%s'\n"
	   "--> Bucket num: '%d'\n"
	   "--> Hostname:   '%s'\n"
	   "--> Index:      '%d'\n" 
	   "--> Bucket:     '%s'\n"
	   "--> File size:  '%d'\n", 
	   final_path->c_str(),
	   use_bucket, 
	   full_host_name_.c_str(),
	   k, 
	   bucket_name->c_str(),
	   mmf->size());  

  // We try few times
  const callback_t *callback = handle->commit_callback;
  int res = -1, retries = 5;
  do {
    if(retries < 5) {
      usleep(50*1000);
    }
    /*
    res = uploadFile(final_path->data(), (char*)mmf->buffer(), mmf->size(),
		     bucket_name.data(), full_host_name_, 
		     //("data" + content_hash.MakePath(1, 2) + hash_suffix).data(), use_bucket,
		     final_path->data(), use_bucket, 
		     //		     content_hash.ToString().data(), use_bucket, 
		     0, callback, mmf);*/
    res = uploadFile(*final_path, (char*)mmf->buffer(), (long unsigned int)mmf->size(),
		     *bucket_name, full_host_name_, 
		     *final_path, (int)use_bucket, 
		     0, callback, mmf);

  }while(retries-- > 0 && res != 0);

  if (res != 0) {
    const int cpy_errno = errno;
    LogCvmfs(kLogS3Fanout, kLogStderr, "failed to move temp file '%s' to "
	                              "final location '%s' (errno: %d)",
             local_handle->temporary_path.c_str(),
             final_path->c_str(),
             cpy_errno);
    atomic_inc32(&copy_errors_);
    Respond(handle->commit_callback, UploaderResults(cpy_errno));
    return;
  }

  LogCvmfs(kLogS3Fanout, kLogDebug, "Uploading from stream finished: %s", local_handle->temporary_path.c_str());

  // Remove the temporary file
  remove(local_handle->temporary_path.c_str());
  delete local_handle;

  //Respond(callback, UploaderResults(0));

}


bool S3Uploader::Remove(const std::string& file_to_delete) {
  if (! Peek(file_to_delete)) {
    return false;
  }

  const int retval = unlink((upstream_path_ + "/" + file_to_delete).c_str());
  return retval == 0;
}


bool S3Uploader::Peek(const std::string& path) const {
  return FileExists(upstream_path_ + "/" + path);
}


int S3Uploader::Move(const std::string &local_path,
                        const std::string &remote_path) const {

  LogCvmfs(kLogS3Fanout, kLogStderr, "Error: Move is not implemented");
  return -1;

}
