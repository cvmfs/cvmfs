/**
 * This file is part of the CernVM File System.
 *
 * Runs a thread using libcurls asynchronous I/O mode to push data to S3
 */

#include "cvmfs_config.h"
#include "s3fanout.h"
#include "upload_facility.h"
#include "util_concurrency.h"

#include <pthread.h>

using namespace std; 

namespace s3fanout {


/** 
 * S3FanoutManager static variable for the compiler to find.
 */
S3FanoutManager *S3FanoutManager::s3fm_ = NULL;
static UrlConstructor url_constructor;


/**
 * Use this to get the S3FanoutManager instance.
 */
S3FanoutManager *S3FanoutManager::Instance() {
  static pthread_once_t once_control = PTHREAD_ONCE_INIT;
  pthread_once(&once_control, &S3FanoutManager::Initialise);
  return s3fm_;
}


/**
 * S3FanoutManager initialisation as a Singleton class.
 */
void S3FanoutManager::Initialise() {
  static S3FanoutManager s;
  s.Init(1024, &url_constructor); // max connections
  s.SetRetryParameters(3, 100, 2000);
  s.Spawn();

  s3fm_ = &s;
}


/**
 * Called by curl for every HTTP header. Not called for file:// transfers.
 */
static size_t CallbackCurlHeader(void *ptr, size_t size, size_t nmemb,
                                 void *info_link) {
  const size_t num_bytes = size*nmemb;
  const string header_line(static_cast<const char *>(ptr), num_bytes);
  JobInfo *info = static_cast<JobInfo *>(info_link);

  // Check for http status code errors
  if (HasPrefix(header_line, "HTTP/1.", false)) {
    if (header_line.length() < 10)
      return 0;

    unsigned i;
    for (i = 8; (i < header_line.length()) && (header_line[i] == ' '); ++i) {}

    if (header_line[i] == '2') {
      return num_bytes;
    } else {
      LogCvmfs(kLogDownload, kLogDebug, "http status error code: %s",
               header_line.c_str());
      if (header_line.length() < i+3) {
        info->error_code = kFailOther;
        return 0;
      }
      int http_error = String2Int64(string(&header_line[i], 3));

      switch (http_error) {
        case 501:
        case 400:
          info->error_code = kFailBadRequest;
          break;
        case 403:
          info->error_code = kFailForbidden;
          break;
        case 404:
          info->error_code = kFailNotFound;
          break;
        default:
          info->error_code = kFailOther;
      }
      return 0;
    }
  }

  return num_bytes;
}


/**
 * Called by curl for every new chunk to upload.
 */
static size_t CallbackCurlData(void *ptr, size_t size, size_t nmemb,
                               void *info_link) {
  const size_t num_bytes = size*nmemb;
  JobInfo *info = static_cast<JobInfo *>(info_link);

  LogCvmfs(kLogS3Fanout, kLogDebug, "Data callback with %d bytes", num_bytes);

  if (num_bytes == 0)
    return 0;

  if (info->origin == kOriginMem) {
    const size_t avail_bytes = info->origin_mem.size - info->origin_mem.pos;
    const size_t send_size = avail_bytes < num_bytes ? avail_bytes : num_bytes;
    memcpy(ptr, info->origin_mem.data + info->origin_mem.pos, send_size);
    info->origin_mem.pos += send_size;
    LogCvmfs(kLogS3Fanout, kLogDebug, "pushed out %d bytes", send_size);
    return send_size;
  } else if (info->origin == kOriginPath) {
    size_t read_bytes = fread(ptr, 1, num_bytes, info->origin_file);
    if (read_bytes != num_bytes) {
      if (ferror(info->origin_file) != 0) {
        LogCvmfs(kLogS3Fanout, kLogDebug, "local I/O error reading %s",
                 info->origin_path->c_str());
        return CURL_READFUNC_ABORT;
      }
    }
    LogCvmfs(kLogS3Fanout, kLogDebug, "pushed out %d bytes", read_bytes);
    return read_bytes;
  }

  return CURL_READFUNC_ABORT;
}


/**
 * Called when new curl sockets arrive or existing curl sockets depart.
 */
int S3FanoutManager::CallbackCurlSocket(CURL *easy, curl_socket_t s, int action,
                                        void *userp, void *socketp) {
  LogCvmfs(kLogDownload, kLogDebug, "CallbackCurlSocket called with easy "
           "handle %p, socket %d, action %d, up %d, sp %d", easy, s, action, userp, socketp);
  S3FanoutManager *s3fanout_mgr = static_cast<S3FanoutManager *>(userp);
  if (action == CURL_POLL_NONE)
    return 0;

  // Find s in watch_fds_
  unsigned index;
  for (index = 0; index < s3fanout_mgr->watch_fds_inuse_; ++index) {
    if (s3fanout_mgr->watch_fds_[index].fd == s)
      break;
  }
  // Or create newly
  if (index == s3fanout_mgr->watch_fds_inuse_) {
    // Extend array if necessary
    if (s3fanout_mgr->watch_fds_inuse_ == s3fanout_mgr->watch_fds_size_)
    {
      s3fanout_mgr->watch_fds_size_ *= 2;
      s3fanout_mgr->watch_fds_ = static_cast<struct pollfd *>(
        srealloc(s3fanout_mgr->watch_fds_,
                 s3fanout_mgr->watch_fds_size_*sizeof(struct pollfd)));
    }
    s3fanout_mgr->watch_fds_[s3fanout_mgr->watch_fds_inuse_].fd = s;
    s3fanout_mgr->watch_fds_[s3fanout_mgr->watch_fds_inuse_].events = 0;
    s3fanout_mgr->watch_fds_[s3fanout_mgr->watch_fds_inuse_].revents = 0;
    s3fanout_mgr->watch_fds_inuse_++;
  }

  switch (action) {
    case CURL_POLL_IN:
      s3fanout_mgr->watch_fds_[index].events |= POLLIN | POLLPRI;
      break;
    case CURL_POLL_OUT:
      s3fanout_mgr->watch_fds_[index].events |= POLLOUT | POLLWRBAND;
      break;
    case CURL_POLL_INOUT:
      s3fanout_mgr->watch_fds_[index].events |=
        POLLIN | POLLPRI | POLLOUT | POLLWRBAND;
      break;
    case CURL_POLL_REMOVE:
      if (index < s3fanout_mgr->watch_fds_inuse_-1)
        s3fanout_mgr->watch_fds_[index] =
          s3fanout_mgr->watch_fds_[s3fanout_mgr->watch_fds_inuse_-1];
      s3fanout_mgr->watch_fds_inuse_--;
      // Shrink array if necessary
      if ((s3fanout_mgr->watch_fds_inuse_ > s3fanout_mgr->watch_fds_max_) &&
          (s3fanout_mgr->watch_fds_inuse_ < s3fanout_mgr->watch_fds_size_/2))
      {
        s3fanout_mgr->watch_fds_size_ /= 2;
        s3fanout_mgr->watch_fds_ = static_cast<struct pollfd *>(
          srealloc(s3fanout_mgr->watch_fds_,
                   s3fanout_mgr->watch_fds_size_*sizeof(struct pollfd)));
      }
      break;
    default:
      break;
  }

  return 0;
}


/**
 * Worker thread event loop.  Waits on new JobInfo structs on a pipe.
 */
void *S3FanoutManager::MainUpload(void *data) {
  LogCvmfs(kLogS3Fanout, kLogDebug, "Upload I/O thread started");
  S3FanoutManager *s3fanout_mgr = static_cast<S3FanoutManager *>(data);

  s3fanout_mgr->watch_fds_ =
    static_cast<struct pollfd *>(smalloc(2 * sizeof(struct pollfd)));
  s3fanout_mgr->watch_fds_size_ = 2;
  s3fanout_mgr->watch_fds_[0].fd = s3fanout_mgr->pipe_terminate_[0];
  s3fanout_mgr->watch_fds_[0].events = POLLIN | POLLPRI;
  s3fanout_mgr->watch_fds_[0].revents = 0;
  s3fanout_mgr->watch_fds_[1].fd = s3fanout_mgr->pipe_jobs_[0];
  s3fanout_mgr->watch_fds_[1].events = POLLIN | POLLPRI;
  s3fanout_mgr->watch_fds_[1].revents = 0;
  s3fanout_mgr->watch_fds_inuse_ = 2;

  int still_running = 0;
  struct timeval timeval_start, timeval_stop;
  gettimeofday(&timeval_start, NULL);
  while (true) {
    int timeout;
    if (still_running) {
      timeout = 1;
    } else {
      timeout = -1;
      gettimeofday(&timeval_stop, NULL);
      s3fanout_mgr->statistics_->transfer_time +=
        DiffTimeSeconds(timeval_start, timeval_stop);
    }
    int retval = poll(s3fanout_mgr->watch_fds_, s3fanout_mgr->watch_fds_inuse_,
                      timeout);
    if (retval < 0) {
      continue;
    }

    // Handle timeout
    if (retval == 0) {
      retval = curl_multi_socket_action(s3fanout_mgr->curl_multi_,
                                        CURL_SOCKET_TIMEOUT,
                                        0,
                                        &still_running);
    }

    // Terminate I/O thread
    if (s3fanout_mgr->watch_fds_[0].revents)
      break;

    // New job arrives
    if (s3fanout_mgr->watch_fds_[1].revents) {
      s3fanout_mgr->watch_fds_[1].revents = 0;
      JobInfo *info;
      ReadPipe(s3fanout_mgr->pipe_jobs_[0], &info, sizeof(info));
      if (!still_running) {
        gettimeofday(&timeval_start, NULL);
      }
      CURL *handle = s3fanout_mgr->AcquireCurlHandle();
      info->request = info->test_and_set ? JobInfo::kReqHead : JobInfo::kReqPut;
      Failures init_failure = s3fanout_mgr->InitializeRequest(info, handle);
      if (init_failure != kFailOk) {
        s3fanout_mgr->ReleaseCurlHandle(info, handle);
        info->error_code = init_failure;
        WritePipe(info->wait_at[1], &info->error_code,
                  sizeof(info->error_code));
      }
      s3fanout_mgr->SetUrlOptions(info);
      curl_multi_add_handle(s3fanout_mgr->curl_multi_, handle);
      retval = curl_multi_socket_action(s3fanout_mgr->curl_multi_,
                                        CURL_SOCKET_TIMEOUT,
                                        0,
                                        &still_running);
    }

    // Activity on curl sockets
    for (unsigned i = 2; i < s3fanout_mgr->watch_fds_inuse_; ++i) {
      if (s3fanout_mgr->watch_fds_[i].revents) {
        int ev_bitmask = 0;
        if (s3fanout_mgr->watch_fds_[i].revents & (POLLIN | POLLPRI))
          ev_bitmask |= CURL_CSELECT_IN;
        if (s3fanout_mgr->watch_fds_[i].revents & (POLLOUT | POLLWRBAND))
          ev_bitmask |= CURL_CSELECT_OUT;
        if (s3fanout_mgr->watch_fds_[i].revents & (POLLERR | POLLHUP | POLLNVAL))
          ev_bitmask |= CURL_CSELECT_ERR;
        s3fanout_mgr->watch_fds_[i].revents = 0;

        retval = curl_multi_socket_action(s3fanout_mgr->curl_multi_,
                                          s3fanout_mgr->watch_fds_[i].fd,
                                          ev_bitmask,
                                          &still_running);
      }
    }

    // Check if transfers are completed
    CURLMsg *curl_msg;
    int msgs_in_queue;
    while ((curl_msg = curl_multi_info_read(s3fanout_mgr->curl_multi_,
                                            &msgs_in_queue)))
    {
      if (curl_msg->msg == CURLMSG_DONE) {
        s3fanout_mgr->statistics_->num_requests++;
        JobInfo *info;
        CURL *easy_handle = curl_msg->easy_handle;
        int curl_error = curl_msg->data.result;
        curl_easy_getinfo(easy_handle, CURLINFO_PRIVATE, &info);

	LogCvmfs(kLogS3Fanout, kLogDebug, "Verify before %s", 
		 info->object_key.c_str());

        curl_multi_remove_handle(s3fanout_mgr->curl_multi_, easy_handle);
        if (s3fanout_mgr->VerifyAndFinalize(curl_error, info)) {
          curl_multi_add_handle(s3fanout_mgr->curl_multi_, easy_handle);
          retval = curl_multi_socket_action(s3fanout_mgr->curl_multi_,
                                            CURL_SOCKET_TIMEOUT,
                                            0,
                                            &still_running);
        } else {
          // Return easy handle into pool and write result back
          s3fanout_mgr->ReleaseCurlHandle(info, easy_handle);

	  FinalReport r;
	  r.f=info->error_code;
	  r.s1=info->wait_at[0];
	  r.s2=info->wait_at[1];
	  WritePipe(info->wait_at[1], &r, sizeof(r));
        }
      }
    }
  }

  for (set<CURL *>::iterator i = s3fanout_mgr->pool_handles_inuse_->begin(),
       iEnd = s3fanout_mgr->pool_handles_inuse_->end(); i != iEnd; ++i) {
    curl_multi_remove_handle(s3fanout_mgr->curl_multi_, *i);
    curl_easy_cleanup(*i);
  }
  s3fanout_mgr->pool_handles_inuse_->clear();
  free(s3fanout_mgr->watch_fds_);

  LogCvmfs(kLogS3Fanout, kLogDebug, "Upload I/O thread terminated");
  return NULL;
}


/**
 * Gets an idle CURL handle from the pool. Creates a new one and adds it to
 * the pool if necessary.
 */
CURL *S3FanoutManager::AcquireCurlHandle() {
  CURL *handle;

  if (pool_handles_idle_->empty()) {
    // Create a new handle
    handle = curl_easy_init();
    assert(handle != NULL);

    curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1);
    //curl_easy_setopt(curl_default, CURLOPT_FAILONERROR, 1);
    curl_easy_setopt(handle, CURLOPT_HEADERFUNCTION, CallbackCurlHeader);
    curl_easy_setopt(handle, CURLOPT_READFUNCTION, CallbackCurlData);
  } else {
    handle = *(pool_handles_idle_->begin());
    pool_handles_idle_->erase(pool_handles_idle_->begin());
  }

  pool_handles_inuse_->insert(handle);

  return handle;
}


void S3FanoutManager::ReleaseCurlHandle(JobInfo *info, CURL *handle) {
  if (info->http_headers) {
    curl_slist_free_all(info->http_headers);
    info->http_headers = NULL;
  }

  set<CURL *>::iterator elem = pool_handles_inuse_->find(handle);
  assert(elem != pool_handles_inuse_->end());
  
  if (pool_handles_idle_->size() > pool_max_handles_)
    curl_easy_cleanup(*elem);
  else
    pool_handles_idle_->insert(*elem);

  pool_handles_inuse_->erase(elem);

  sem_post(&available_jobs_);
}


/**
 * The Amazon authorization header according to
 * http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#ConstructingTheAuthenticationHeader
 */
string S3FanoutManager::MkAuthoritzation(const string &access_key,
                                         const string &secret_key,
                                         const string &timestamp,
                                         const string &content_type,
                                         const string &request,
                                         const string &content_md5_base64,
                                         const string &bucket,
                                         const string &object_key)
{
  string to_sign = request + "\n" +
    content_md5_base64 + "\n" +
    content_type + "\n" +
    timestamp + "\n" +
    "x-amz-acl:public-read" + "\n" + // default ACL is public read
    "/" + bucket + "/" + object_key;
  LogCvmfs(kLogS3Fanout, kLogDebug, "string to sign for: %s", object_key.c_str());

  shash::Any hmac;
  hmac.algorithm = shash::kSha1;
  shash::Hmac(secret_key,
              reinterpret_cast<const unsigned char *>(to_sign.data()),
              to_sign.length(), &hmac);

  return "Authorization: AWS " + access_key + ":" +
    Base64(string(reinterpret_cast<char *>(hmac.digest), hmac.GetDigestSize()));
}


/**
 * Request parameters set the URL and other options such as timeout and
 * proxy.
 */
Failures S3FanoutManager::InitializeRequest(JobInfo *info, CURL *handle) {
  // Initialize internal download state
  info->curl_handle = handle;
  info->error_code = kFailOk;
  info->num_retries = 0;
  info->backoff_ms = 0;
  info->http_headers = NULL;

  // HEAD or PUT
  shash::Any content_md5;
  content_md5.algorithm = shash::kMd5;
  string timestamp;
  if (info->request == JobInfo::kReqHead) {
    curl_easy_setopt(handle, CURLOPT_UPLOAD, 0);
    curl_easy_setopt(handle, CURLOPT_NOBODY, 1);
    timestamp = RfcTimestamp();
    info->http_headers =
      curl_slist_append(info->http_headers,
                        MkAuthoritzation(*(info->access_key),
                                         *(info->secret_key),
                                         timestamp, "", "HEAD", "",
                                         *(info->bucket),
                                         info->object_key).c_str());
    info->http_headers =
      curl_slist_append(info->http_headers, "Content-Length: 0");
  } else {
    curl_easy_setopt(handle, CURLOPT_NOBODY, 0);
    curl_easy_setopt(handle, CURLOPT_UPLOAD, 1);
    // MD5 content hash
    if (info->origin == kOriginMem) {
      curl_easy_setopt(handle, CURLOPT_INFILESIZE_LARGE, info->origin_mem.size);
      shash::HashMem(info->origin_mem.data, info->origin_mem.size, &content_md5);
    } else if (info->origin == kOriginPath) {
      bool retval = shash::HashFile(*(info->origin_path), &content_md5);
      if (!retval)
        return kFailLocalIO;
      int64_t file_size = GetFileSize(*(info->origin_path));
      if (file_size == -1)
        return kFailLocalIO;
      curl_easy_setopt(handle, CURLOPT_INFILESIZE_LARGE, file_size);
    }
    LogCvmfs(kLogS3Fanout, kLogDebug, "content hash: %s",
             content_md5.ToString().c_str());
    string content_md5_base64 =
      Base64(string(reinterpret_cast<char *>(content_md5.digest),
                    content_md5.GetDigestSize()));
    info->http_headers =
      curl_slist_append(info->http_headers,
                        ("Content-MD5: " + content_md5_base64).c_str());

    // Authorization
    timestamp = RfcTimestamp();
    info->http_headers =
      curl_slist_append(info->http_headers,
                        MkAuthoritzation(*(info->access_key),
                                         *(info->secret_key),
                                         timestamp, "binary/octet-stream",
                                         "PUT", content_md5_base64,
                                         *(info->bucket),
                                         (info->object_key)).c_str());

    info->http_headers =
      curl_slist_append(info->http_headers, "Content-Type: binary/octet-stream");
  }

  // Common headers
  info->http_headers =
    curl_slist_append(info->http_headers, ("Date: " + timestamp).c_str());
  std::string acl = "x-amz-acl: public-read";
  info->http_headers =
    curl_slist_append(info->http_headers, acl.c_str());
  info->http_headers =
    curl_slist_append(info->http_headers, "Connection: Keep-Alive");
  info->http_headers = curl_slist_append(info->http_headers, "Pragma:");
  // No 100-continue
  info->http_headers = curl_slist_append(info->http_headers, "Expect:");
  // Strip unnecessary header
  info->http_headers = curl_slist_append(info->http_headers, "Accept:");
  info->http_headers = curl_slist_append(info->http_headers,
                                         user_agent_->c_str());

  // Set curl parameters
  curl_easy_setopt(handle, CURLOPT_PRIVATE, static_cast<void *>(info));
  curl_easy_setopt(handle, CURLOPT_WRITEHEADER, static_cast<void *>(info));
  curl_easy_setopt(handle, CURLOPT_READDATA, static_cast<void *>(info));
  curl_easy_setopt(handle, CURLOPT_HTTPHEADER, info->http_headers);
  if (opt_ipv4_only_)
    curl_easy_setopt(handle, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);

  return kFailOk;
}


/**
 * Sets the URL specific options such as host to use and timeout.
 */
void S3FanoutManager::SetUrlOptions(JobInfo *info) {
  CURL *curl_handle = info->curl_handle;

  pthread_mutex_lock(lock_options_);
  curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, opt_timeout_);
  pthread_mutex_unlock(lock_options_);

  string url = url_constructor_->MkUrl(info->hostname, *(info->bucket), (info->object_key));
  curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str());
}


/**
 * Adds transfer time and uploaded bytes to the global counters.
 */
void S3FanoutManager::UpdateStatistics(CURL *handle) {
  double val;

  if (curl_easy_getinfo(handle, CURLINFO_SIZE_UPLOAD, &val) == CURLE_OK)
    statistics_->transferred_bytes += val;
}


/**
 * Retry if possible and if not already done too often.
 */
bool S3FanoutManager::CanRetry(const JobInfo *info) {
  pthread_mutex_lock(lock_options_);
  unsigned max_retries = opt_max_retries_;
  pthread_mutex_unlock(lock_options_);

  return (info->error_code == kFailHostConnection) &&
         (info->num_retries < max_retries);
}


/**
 * Backoff for retry to introduce a jitter into a upload sequence.
 *
 * \return true if backoff has been performed, false otherwise
 */
void S3FanoutManager::Backoff(JobInfo *info) {
  pthread_mutex_lock(lock_options_);
  unsigned backoff_init_ms = opt_backoff_init_ms_;
  unsigned backoff_max_ms = opt_backoff_max_ms_;
  pthread_mutex_unlock(lock_options_);

  info->num_retries++;
  statistics_->num_retries++;
  if (info->backoff_ms == 0) {
    info->backoff_ms = prng_.Next(backoff_init_ms + 1);  // Must be != 0
  } else {
    info->backoff_ms *= 2;
  }
  if (info->backoff_ms > backoff_max_ms)
    info->backoff_ms = backoff_max_ms;

  LogCvmfs(kLogS3Fanout, kLogDebug, "backing off for %d ms", info->backoff_ms);
  SafeSleepMs(info->backoff_ms);
}


/**
 * Checks the result of a curl download and implements the failure logic, such
 * as changing the proxy server. Takes care of cleanup.
 *
 * \return true if another download should be performed, false otherwise
 */
bool S3FanoutManager::VerifyAndFinalize(const int curl_error, JobInfo *info) {
  LogCvmfs(kLogS3Fanout, kLogDebug, "Verify uploaded/tested object %s "
           "(curl error %d, info error %d, info request %d)",
           info->object_key.c_str(), curl_error, info->error_code, info->request);
  UpdateStatistics(info->curl_handle);

  // Verification and error classification
  switch (curl_error) {
    case CURLE_OK:
      info->error_code = kFailOk;
      break;
    case CURLE_UNSUPPORTED_PROTOCOL:
    case CURLE_URL_MALFORMAT:
      info->error_code = kFailBadRequest;
      break;
    case CURLE_COULDNT_RESOLVE_HOST:
      info->error_code = kFailHostResolve;
      break;
    case CURLE_COULDNT_CONNECT:
    case CURLE_OPERATION_TIMEDOUT:
    case CURLE_SEND_ERROR:
    case CURLE_RECV_ERROR:
      info->error_code = kFailHostConnection;
      break;
    case CURLE_ABORTED_BY_CALLBACK:
    case CURLE_WRITE_ERROR:
      // Error set by callback
      break;
    default:
      LogCvmfs(kLogS3Fanout, kLogDebug | kLogSyslogErr,
               "unexpected curl error (%d) while trying to upload %s",
               curl_error, info->object_key.c_str());
      info->error_code = kFailOther;
      break;
  }

  // Transform head to put
  if ((info->error_code == kFailNotFound) &&
      (info->request == JobInfo::kReqHead))
  {
    LogCvmfs(kLogS3Fanout, kLogDebug, "not found: %s, uploading",
             info->object_key.c_str());
    info->request = JobInfo::kReqPut;
    curl_slist_free_all(info->http_headers);
    info->http_headers = NULL;
    InitializeRequest(info, info->curl_handle);
    return true;  // Again, Put
  }

  // Determination if download should be repeated
  bool try_again = false;
  if (info->error_code != kFailOk) {
    try_again = CanRetry(info);
  }

  if (try_again) {
    LogCvmfs(kLogDownload, kLogDebug, "Trying again to upload %s",
             info->object_key.c_str());
    // Reset origin
    if (info->origin == kOriginMem)
      info->origin_mem.pos = 0;
    if (info->origin == kOriginPath)
      rewind(info->origin_file);

    Backoff(info);
    return true;  // try again
  }

  // Finalize, flush destination file
  if (info->origin == kOriginPath) {
    if (fclose(info->origin_file) != 0)
      info->error_code = kFailLocalIO;
    info->origin_file = NULL;
  }

  return false;  // stop transfer and return to Push()
}


S3FanoutManager::S3FanoutManager() {
  pool_handles_idle_ = NULL;
  pool_handles_inuse_ = NULL;
  pool_max_handles_ = 0;
  curl_multi_ = NULL;
  user_agent_ = new string();

  atomic_init32(&multi_threaded_);
  pipe_terminate_[0] = pipe_terminate_[1] = -1;
  pipe_jobs_[0] = pipe_jobs_[1] = -1;
  watch_fds_ = NULL;
  watch_fds_size_ = 0;
  watch_fds_inuse_ = 0;
  watch_fds_max_ = 0;

  lock_options_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_options_, NULL);
  assert(retval == 0);

  opt_timeout_ = 0;
  opt_backoff_init_ms_ = 0;
  opt_backoff_max_ms_ = 0;

  opt_ipv4_only_ = false;

  statistics_ = NULL;
}


S3FanoutManager::~S3FanoutManager() {
  pthread_mutex_destroy(lock_options_);
  free(lock_options_);
}


void S3FanoutManager::Init(const unsigned max_pool_handles,
                           AbstractUrlConstructor *url_constructor) {
  atomic_init32(&multi_threaded_);
  int retval = curl_global_init(CURL_GLOBAL_ALL);
  assert(retval == CURLE_OK);
  pool_handles_idle_ = new set<CURL *>;
  pool_handles_inuse_ = new set<CURL *>;
  pool_max_handles_ = max_pool_handles;
  watch_fds_max_ = 4*pool_max_handles_;

  max_available_jobs_ = 4*pool_max_handles_;
  sem_init(&available_jobs_, 1, max_available_jobs_);

  opt_timeout_ = 20;
  statistics_ = new Statistics();
  *user_agent_ = "User-Agent: cvmfs " + string(VERSION);

  curl_multi_ = curl_multi_init();
  assert(curl_multi_ != NULL);
  curl_multi_setopt(curl_multi_, CURLMOPT_SOCKETFUNCTION, CallbackCurlSocket);
  curl_multi_setopt(curl_multi_, CURLMOPT_SOCKETDATA,
                    static_cast<void *>(this));
  //curl_multi_setopt(curl_multi_, CURLMOPT_PIPELINING, 1);
  //curl_multi_setopt(curl_multi_, CURLMOPT_MAX_PIPELINE_LENGTH, 64);
  curl_multi_setopt(curl_multi_, CURLMOPT_MAX_TOTAL_CONNECTIONS,
                    pool_max_handles_);

  prng_.InitLocaltime();

  // Parsing environment variables
  if ((getenv("CVMFS_IPV4_ONLY") != NULL) &&
      (strlen(getenv("CVMFS_IPV4_ONLY")) > 0))
  {
    opt_ipv4_only_ = true;
  }

  url_constructor_ = url_constructor;

  watch_fds_ =
    static_cast<struct pollfd *>(smalloc(2 * sizeof(struct pollfd)));
  watch_fds_size_ = 2;
  watch_fds_inuse_ = 2;
}


void S3FanoutManager::Fini() {
  if (atomic_xadd32(&multi_threaded_, 0) == 1) {
    // Shutdown I/O thread
    char buf = 'T';
    WritePipe(pipe_terminate_[1], &buf, 1);
    pthread_join(thread_upload_, NULL);
    // All handles are removed from the multi stack
    close(pipe_terminate_[1]);
    close(pipe_terminate_[0]);
    close(pipe_jobs_[1]);
    close(pipe_jobs_[0]);
  }

  for (set<CURL *>::iterator i = pool_handles_idle_->begin(),
       iEnd = pool_handles_idle_->end(); i != iEnd; ++i) {
    curl_easy_cleanup(*i);
  }
  delete pool_handles_idle_;
  delete pool_handles_inuse_;
  delete user_agent_;
  curl_multi_cleanup(curl_multi_);
  pool_handles_idle_ = NULL;
  pool_handles_inuse_ = NULL;
  user_agent_ = NULL;
  curl_multi_ = NULL;

  delete statistics_;
  statistics_ = NULL;

  sem_destroy(&available_jobs_);

  curl_global_cleanup();
}


/**
 * Spawns the I/O worker thread.  No way back except Fini(); Init();
 */
void S3FanoutManager::Spawn() {
  MakePipe(pipe_terminate_);
  MakePipe(pipe_jobs_);

  LogCvmfs(kLogS3Fanout, kLogDebug, "S3FanoutManager spawned");  

  int retval = pthread_create(&thread_upload_, NULL, MainUpload,
                              static_cast<void *>(this));
  assert(retval == 0);

  atomic_inc32(&multi_threaded_);  
}


/**
 * The timeout counts for all sorts of connection phases,
 * DNS, HTTP connect, etc.
 */
void S3FanoutManager::SetTimeout(const unsigned seconds) {
  pthread_mutex_lock(lock_options_);
  opt_timeout_ = seconds;
  pthread_mutex_unlock(lock_options_);
}


/**
 * Receives the currently active timeout value.
 */
void S3FanoutManager::GetTimeout(unsigned *seconds) {
  pthread_mutex_lock(lock_options_);
  *seconds = opt_timeout_;
  pthread_mutex_unlock(lock_options_);
}


const Statistics &S3FanoutManager::GetStatistics() {
  return *statistics_;
}


void S3FanoutManager::SetRetryParameters(const unsigned max_retries,
                                         const unsigned backoff_init_ms,
                                         const unsigned backoff_max_ms)
{
  pthread_mutex_lock(lock_options_);
  opt_max_retries_ = max_retries;
  opt_backoff_init_ms_ = backoff_init_ms;
  opt_backoff_max_ms_ = backoff_max_ms;
  pthread_mutex_unlock(lock_options_);
}


int S3FanoutManager::GetNumberOfActiveJobs() {
  return watch_fds_inuse_;
}


int S3FanoutManager::PollConnections() {
  int timeout = 1;
  int retval = poll(watch_fds_, watch_fds_inuse_, timeout);
  return retval;
}


int S3FanoutManager::GetCompletedJobs(std::vector<s3fanout::JobInfo*> &jobs) {
  int still_running = 0;

  // Poll the status of current jobs
  int retval2 = PollConnections();
  if (retval2 < 0) {
    // No events, we continue
    return 0;
  }

  // Activity on curl sockets
  for (unsigned i = 2; i < watch_fds_inuse_; ++i) {
    if (watch_fds_[i].revents) {
      int ev_bitmask = 0;
      if (watch_fds_[i].revents & (POLLIN | POLLPRI))
	ev_bitmask |= CURL_CSELECT_IN;
      if (watch_fds_[i].revents & (POLLOUT | POLLWRBAND))
	ev_bitmask |= CURL_CSELECT_OUT;
      if (watch_fds_[i].revents & (POLLERR | POLLHUP | POLLNVAL))
	ev_bitmask |= CURL_CSELECT_ERR;
      watch_fds_[i].revents = 0;

      int retval = curl_multi_socket_action(curl_multi_,
					    watch_fds_[i].fd,
					    ev_bitmask,
					    &still_running);
      LogCvmfs(kLogS3Fanout, kLogDebug, "socket action on socket %d, returned with %d, still_running %d", 
	       watch_fds_[i].fd, retval, still_running);
    }
  }

  // Check if transfers are completed
  CURLMsg *curl_msg;
  int msgs_in_queue;
  while ((curl_msg = curl_multi_info_read(curl_multi_, &msgs_in_queue))) {
    if (curl_msg->msg == CURLMSG_DONE) {
      statistics_->num_requests++;
      s3fanout::JobInfo *info;
      CURL *easy_handle = curl_msg->easy_handle;
      int curl_error = curl_msg->data.result;
      assert(curl_easy_getinfo(easy_handle, CURLINFO_PRIVATE, &info)==CURLE_OK);
      LogCvmfs(kLogS3Fanout, kLogDebug, "Done message for curl_msg->msg: %s", info->object_key.c_str()); 

      curl_multi_remove_handle(curl_multi_, easy_handle);
      if (VerifyAndFinalize(curl_error, info)) {
	curl_multi_add_handle(curl_multi_, easy_handle);
	curl_multi_socket_action(curl_multi_,
				 CURL_SOCKET_TIMEOUT,
				 0,
				 &still_running);
      } else {
	// Return easy handle into pool and write result back
	ReleaseCurlHandle(info, easy_handle);
	jobs.push_back(info);
      }
    }
  }

  return 0;
}


int S3FanoutManager::Push(JobInfo *info) {	 
  CURL *handle = AcquireCurlHandle();
  if(handle == NULL) {
    LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to acquire CURL handle.");
    return -1;
  }

  s3fanout::Failures init_failure = InitializeRequest(info, handle);
  if (init_failure != s3fanout::kFailOk) {
    return -1;
  }
  SetUrlOptions(info);

  curl_multi_add_handle(curl_multi_, handle);
  int still_running = 0, retval = 0; 
  retval = curl_multi_socket_action(curl_multi_,
				    CURL_SOCKET_TIMEOUT,
				    0,
				    &still_running);

  LogCvmfs(kLogS3Fanout, kLogDebug, "curl_multi_socket_action: %d - %d", retval, still_running);
  
  return 0;
}


//------------------------------------------------------------------------------


string Statistics::Print() const {
  return
  "Transferred Bytes: " + StringifyInt(uint64_t(transferred_bytes)) + "\n" +
  "Transfer duration: " + StringifyInt(uint64_t(transfer_time)) + " s\n" +
  "Number of requests: " + StringifyInt(num_requests) + "\n" +
  "Number of retries: " + StringifyInt(num_retries) + "\n";
}

}  // namespace s3fanout
