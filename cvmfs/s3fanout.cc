/**
 * This file is part of the CernVM File System.
 *
 * Runs a thread using libcurls asynchronous I/O mode to push data to S3
 */

#include <pthread.h>

#include <cerrno>
#include <utility>

#include "cvmfs_config.h"
#include "s3fanout.h"
#include "upload_facility.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace s3fanout {

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
      LogCvmfs(kLogS3Fanout, kLogDebug, "http status error code: %s",
               header_line.c_str());
      if (header_line.length() < i+3) {
        info->error_code = kFailOther;
        return 0;
      }
      int http_error = String2Int64(string(&header_line[i], 3));

      switch (http_error) {
        case 503:
          info->error_code = kFailServiceUnavailable;
          break;
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
    LogCvmfs(kLogS3Fanout, kLogDebug, "mem pushed out %d bytes", send_size);
    return send_size;
  } else if (info->origin == kOriginPath) {
    size_t read_bytes = fread(ptr, 1, num_bytes, info->origin_file);
    if (read_bytes != num_bytes) {
      if (ferror(info->origin_file) != 0) {
        LogCvmfs(kLogS3Fanout, kLogStderr, "local I/O error reading %s",
                 info->origin_path.c_str());
        return CURL_READFUNC_ABORT;
      }
    }
    LogCvmfs(kLogS3Fanout, kLogDebug, "file pushed out %d bytes", read_bytes);
    return read_bytes;
  }

  return CURL_READFUNC_ABORT;
}


/**
 * Called when new curl sockets arrive or existing curl sockets depart.
 */
int S3FanoutManager::CallbackCurlSocket(CURL *easy, curl_socket_t s, int action,
                                        void *userp, void *socketp) {
  S3FanoutManager *s3fanout_mgr = static_cast<S3FanoutManager *>(userp);
  const int ajobs = *s3fanout_mgr->available_jobs_;
  LogCvmfs(kLogS3Fanout, kLogDebug, "CallbackCurlSocket called with easy "
           "handle %p, socket %d, action %d, up %d, "
           "sp %d, fds_inuse %d, jobs %d",
           easy, s, action, userp,
           socketp, s3fanout_mgr->watch_fds_inuse_, ajobs);
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
    if (s3fanout_mgr->watch_fds_inuse_ == s3fanout_mgr->watch_fds_size_) {
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
          (s3fanout_mgr->watch_fds_inuse_ < s3fanout_mgr->watch_fds_size_/2)) {
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
 * Worker thread event loop.
 */
void *S3FanoutManager::MainUpload(void *data) {
  LogCvmfs(kLogS3Fanout, kLogDebug, "Upload I/O thread started");
  S3FanoutManager *s3fanout_mgr = static_cast<S3FanoutManager *>(data);

  while (s3fanout_mgr->thread_upload_run_) {
    JobInfo *info = NULL;
    pthread_mutex_lock(s3fanout_mgr->jobs_todo_lock_);
    if (!s3fanout_mgr->jobs_todo_.empty()) {
      info = s3fanout_mgr->jobs_todo_.back();
      s3fanout_mgr->jobs_todo_.pop_back();
    }
    pthread_mutex_unlock(s3fanout_mgr->jobs_todo_lock_);

    if (info != NULL) {
      CURL *handle = s3fanout_mgr->AcquireCurlHandle();
      if (handle == NULL) {
        LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to acquire CURL handle.");
        assert(handle != NULL);
      }

      s3fanout::Failures init_failure = s3fanout_mgr->InitializeRequest(info,
                                                                        handle);
      if (init_failure != s3fanout::kFailOk) {
        LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to initialize CURL handle "
                                           "(error: %d - %s | errno: %d)",
                 init_failure, Code2Ascii(init_failure), errno);
        abort();
      }
      s3fanout_mgr->SetUrlOptions(info);

      curl_multi_add_handle(s3fanout_mgr->curl_multi_, handle);
      int still_running = 0, retval = 0;
      retval = curl_multi_socket_action(s3fanout_mgr->curl_multi_,
                                        CURL_SOCKET_TIMEOUT,
                                        0,
                                        &still_running);

      LogCvmfs(kLogS3Fanout, kLogDebug,
               "curl_multi_socket_action: %d - %d",
               retval, still_running);
    }

    // Check events with 1ms timeout
    int timeout = 1;
    int retval = poll(s3fanout_mgr->watch_fds_, s3fanout_mgr->watch_fds_inuse_,
                      timeout);
    if (retval == 0) {
      // Handle timeout
      int still_running = 0;
      retval = curl_multi_socket_action(s3fanout_mgr->curl_multi_,
                                        CURL_SOCKET_TIMEOUT,
                                        0,
                                        &still_running);
      if (retval != CURLM_OK) {
        LogCvmfs(kLogS3Fanout, kLogStderr, "Error, timeout due to: %d", retval);
        assert(retval == CURLM_OK);
      }
    } else if (retval < 0) {
      LogCvmfs(kLogS3Fanout, kLogStderr, "Error, event poll failed: %d", errno);
      assert(retval >= 0);
    }

    // Activity on curl sockets
    for (unsigned i = 0; i < s3fanout_mgr->watch_fds_inuse_; ++i) {
      if (s3fanout_mgr->watch_fds_[i].revents) {
        int ev_bitmask = 0;
        if (s3fanout_mgr->watch_fds_[i].revents & (POLLIN | POLLPRI))
          ev_bitmask |= CURL_CSELECT_IN;
        if (s3fanout_mgr->watch_fds_[i].revents & (POLLOUT | POLLWRBAND))
          ev_bitmask |= CURL_CSELECT_OUT;
        if (s3fanout_mgr->watch_fds_[i].revents &
            (POLLERR | POLLHUP | POLLNVAL))
          ev_bitmask |= CURL_CSELECT_ERR;
        s3fanout_mgr->watch_fds_[i].revents = 0;

        int still_running = 0;
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
                                            &msgs_in_queue))) {
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
          int still_running = 0;
          retval = curl_multi_socket_action(s3fanout_mgr->curl_multi_,
                                            CURL_SOCKET_TIMEOUT,
                                            0,
                                            &still_running);
        } else {
          // Return easy handle into pool and write result back
          s3fanout_mgr->ReleaseCurlHandle(info, easy_handle);
          s3fanout_mgr->available_jobs_->Decrement();

          pthread_mutex_lock(s3fanout_mgr->jobs_completed_lock_);
          s3fanout_mgr->jobs_completed_.push_back(info);
          pthread_mutex_unlock(s3fanout_mgr->jobs_completed_lock_);
        }
      }
    }
  }

  set<CURL *>::iterator             i    =
      s3fanout_mgr->pool_handles_inuse_->begin();
  const set<CURL *>::const_iterator iEnd =
      s3fanout_mgr->pool_handles_inuse_->end();
  for (; i != iEnd; ++i) {
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
CURL *S3FanoutManager::AcquireCurlHandle() const {
  CURL *handle;

  MutexLockGuard guard(curl_handle_lock_);

  if (pool_handles_idle_->empty()) {
    CURLcode retval;

    // Create a new handle
    handle = curl_easy_init();
    assert(handle != NULL);

    // Other settings
    retval = curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1);
    assert(retval == CURLE_OK);
    retval = curl_easy_setopt(handle, CURLOPT_HEADERFUNCTION,
                              CallbackCurlHeader);
    assert(retval == CURLE_OK);
    retval = curl_easy_setopt(handle, CURLOPT_READFUNCTION, CallbackCurlData);
    assert(retval == CURLE_OK);
  } else {
    handle = *(pool_handles_idle_->begin());
    pool_handles_idle_->erase(pool_handles_idle_->begin());
  }

  pool_handles_inuse_->insert(handle);

  return handle;
}


void S3FanoutManager::ReleaseCurlHandle(JobInfo *info, CURL *handle) const {
  if (info->http_headers) {
    curl_slist_free_all(info->http_headers);
    info->http_headers = NULL;
  }

  MutexLockGuard guard(curl_handle_lock_);

  set<CURL *>::iterator elem = pool_handles_inuse_->find(handle);
  assert(elem != pool_handles_inuse_->end());

  if (pool_handles_idle_->size() > pool_max_handles_) {
    CURLcode retval = curl_easy_setopt(handle, CURLOPT_SHARE, NULL);
    assert(retval == CURLE_OK);
    curl_easy_cleanup(handle);
    std::map<CURL *, S3FanOutDnsEntry *>::size_type retitems =
        curl_sharehandles_->erase(handle);
    assert(retitems == 1);
  } else {
    pool_handles_idle_->insert(handle);
  }

  pool_handles_inuse_->erase(elem);
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
                                         const string &object_key) const {
  string to_sign = request + "\n" +
                   content_md5_base64 + "\n" +
                   content_type + "\n" +
                   timestamp + "\n" +
                   "x-amz-acl:public-read" + "\n" +  // default ACL
                   "/" + bucket + "/" + object_key;
  LogCvmfs(kLogS3Fanout, kLogDebug,
           "%s string to sign for: %s", request.c_str(), object_key.c_str());

  shash::Any hmac;
  hmac.algorithm = shash::kSha1;
  shash::Hmac(secret_key,
              reinterpret_cast<const unsigned char *>(to_sign.data()),
              to_sign.length(), &hmac);

  return "Authorization: AWS " + access_key + ":" +
      Base64(string(reinterpret_cast<char *>(hmac.digest),
                    hmac.GetDigestSize()));
}


void S3FanoutManager::InitializeDnsSettingsCurl(
  CURL *handle,
  CURLSH *sharehandle,
  curl_slist *clist) const
{
  CURLcode retval = curl_easy_setopt(handle, CURLOPT_SHARE, sharehandle);
  assert(retval == CURLE_OK);
  retval = curl_easy_setopt(handle, CURLOPT_RESOLVE, clist);
  assert(retval == CURLE_OK);
}


int S3FanoutManager::InitializeDnsSettings(
  CURL *handle,
  std::string host_with_port) const
{
  // Use existing handle
  std::map<CURL *, S3FanOutDnsEntry *>::const_iterator it =
      curl_sharehandles_->find(handle);
  if (it != curl_sharehandles_->end()) {
    InitializeDnsSettingsCurl(handle, it->second->sharehandle,
                              it->second->clist);
    return 0;
  }

  // Remove port number if such exists
  if (host_with_port.compare(0, 7, "http://") != 0)
    host_with_port = "http://" + host_with_port;
  std::string remote_host = dns::ExtractHost(host_with_port);
  std::string remote_port = dns::ExtractPort(host_with_port);

  // If we have the name already resolved, use the least used IP
  S3FanOutDnsEntry *useme = NULL;
  unsigned int usemin = UINT_MAX;
  std::set<S3FanOutDnsEntry *>::iterator its3 = sharehandles_->begin();
  for (; its3 != sharehandles_->end(); ++its3) {
    if ((*its3)->dns_name == remote_host) {
      if (usemin >= (*its3)->counter) {
        usemin = (*its3)->counter;
        useme = (*its3);
      }
    }
  }
  if (useme != NULL) {
    curl_sharehandles_->insert(std::pair<CURL *,
                              S3FanOutDnsEntry *>(handle, useme));
    useme->counter++;
    InitializeDnsSettingsCurl(handle, useme->sharehandle, useme->clist);
    return 0;
  }

  // We need to resolve the hostname
  // TODO(ssheikki): support ipv6 also...  if (opt_ipv4_only_)
  dns::Host host = resolver_->Resolve(remote_host);
  set<string> ipv4_addresses = host.ipv4_addresses();
  std::set<string>::iterator its = ipv4_addresses.begin();
  S3FanOutDnsEntry *dnse = NULL;
  for ( ; its != ipv4_addresses.end(); ++its) {
    dnse = new S3FanOutDnsEntry();
    dnse->counter = 0;
    dnse->dns_name = remote_host;
    dnse->port = remote_port.size() == 0 ? "80" : remote_port;
    dnse->ip = *its;
    dnse->clist = NULL;
    dnse->clist = curl_slist_append(dnse->clist,
                                    (dnse->dns_name+":"+
                                     dnse->port+":"+
                                     dnse->ip).c_str());
    dnse->sharehandle = curl_share_init();
    assert(dnse->sharehandle != NULL);
    CURLSHcode share_retval = curl_share_setopt(dnse->sharehandle,
                                                CURLSHOPT_SHARE,
                                                CURL_LOCK_DATA_DNS);
    assert(share_retval == CURLSHE_OK);
    sharehandles_->insert(dnse);
  }
  if (dnse == NULL) {
    LogCvmfs(kLogS3Fanout, kLogStderr | kLogSyslogErr,
             "Error: DNS resolve failed for address '%s'.",
             remote_host.c_str());
    assert(dnse != NULL);
    return -1;
  }
  curl_sharehandles_->insert(std::pair<CURL *,
                             S3FanOutDnsEntry *>(handle, dnse));
  dnse->counter++;
  InitializeDnsSettingsCurl(handle, dnse->sharehandle, dnse->clist);

  return 0;
}


/**
 * Request parameters set the URL and other options such as timeout and
 * proxy.
 */
Failures S3FanoutManager::InitializeRequest(JobInfo *info, CURL *handle) const {
  // Initialize internal download state
  info->curl_handle = handle;
  info->error_code = kFailOk;
  info->num_retries = 0;
  info->backoff_ms = 0;
  info->http_headers = NULL;

  InitializeDnsSettings(handle, info->hostname);

  // HEAD or PUT
  shash::Any content_md5;
  content_md5.algorithm = shash::kMd5;
  string timestamp;
  CURLcode retval;
  if (info->request == JobInfo::kReqHead ||
      info->request == JobInfo::kReqDelete)
  {
    retval = curl_easy_setopt(handle, CURLOPT_UPLOAD, 0);
    assert(retval == CURLE_OK);
    retval = curl_easy_setopt(handle, CURLOPT_NOBODY, 1);
    assert(retval == CURLE_OK);
    timestamp = RfcTimestamp();
    std::string req = info->request == JobInfo::kReqHead ? "HEAD" : "DELETE";
    info->http_headers =
        curl_slist_append(info->http_headers,
                          MkAuthoritzation(info->access_key,
                                           info->secret_key,
                                           timestamp, "",
                                           req.c_str(),
                                           "",
                                           info->bucket,
                                           info->object_key).c_str());
    info->http_headers =
        curl_slist_append(info->http_headers, "Content-Length: 0");

    if (info->request == JobInfo::kReqDelete) {
      retval = curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, req.c_str());
      assert(retval == CURLE_OK);
    } else {
      retval = curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, NULL);
      assert(retval == CURLE_OK);
    }
  } else {
    retval = curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, NULL);
    assert(retval == CURLE_OK);
    retval = curl_easy_setopt(handle, CURLOPT_UPLOAD, 1);
    assert(retval == CURLE_OK);
    retval = curl_easy_setopt(handle, CURLOPT_NOBODY, 0);
    assert(retval == CURLE_OK);
    // MD5 content hash
    if (info->origin == kOriginMem) {
      retval = curl_easy_setopt(handle, CURLOPT_INFILESIZE_LARGE,
                                static_cast<curl_off_t>(info->origin_mem.size));
      assert(retval == CURLE_OK);
      shash::HashMem(info->origin_mem.data,
                     info->origin_mem.size,
                     &content_md5);
    } else if (info->origin == kOriginPath) {
      bool hashretval = shash::HashFile(info->origin_path, &content_md5);
      if (!hashretval) {
        LogCvmfs(kLogS3Fanout, kLogStderr, "failed to hash file %s (errno: %d)",
                 info->origin_path.c_str(), errno);
        return kFailLocalIO;
      }
      int64_t file_size = GetFileSize(info->origin_path);
      if (file_size == -1) {
        LogCvmfs(kLogS3Fanout, kLogStderr, "failed to stat file %s (errno: %d)",
                 info->origin_path.c_str(), errno);
        return kFailLocalIO;
      }
      assert(info->origin_file == NULL);
      info->origin_file = fopen(info->origin_path.c_str(), "r");
      if (info->origin_file == NULL) {
        LogCvmfs(kLogS3Fanout, kLogStderr, "failed to open file %s (errno: %d)",
                 info->origin_path.c_str(), errno);
        return kFailLocalIO;
      }
      retval = curl_easy_setopt(handle, CURLOPT_INFILESIZE_LARGE,
                                static_cast<curl_off_t>(file_size));
      assert(retval == CURLE_OK);
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
                          MkAuthoritzation(info->access_key,
                                           info->secret_key,
                                           timestamp, "binary/octet-stream",
                                           "PUT", content_md5_base64,
                                           info->bucket,
                                           (info->object_key)).c_str());

    info->http_headers =
        curl_slist_append(info->http_headers,
                          "Content-Type: binary/octet-stream");

    if (info->request == JobInfo::kReqPutNoCache) {
      std::string cache_control = "Cache-Control: no-cache";
      info->http_headers =
          curl_slist_append(info->http_headers, cache_control.c_str());
    }
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
  retval = curl_easy_setopt(handle, CURLOPT_PRIVATE, static_cast<void *>(info));
  assert(retval == CURLE_OK);
  retval = curl_easy_setopt(handle, CURLOPT_WRITEHEADER,
                            static_cast<void *>(info));
  assert(retval == CURLE_OK);
  retval = curl_easy_setopt(handle, CURLOPT_READDATA,
                            static_cast<void *>(info));
  assert(retval == CURLE_OK);
  retval = curl_easy_setopt(handle, CURLOPT_HTTPHEADER, info->http_headers);
  assert(retval == CURLE_OK);
  if (opt_ipv4_only_) {
    retval = curl_easy_setopt(handle, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
    assert(retval == CURLE_OK);
  }
  // Follow HTTP redirects
  retval = curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 1L);
  assert(retval == CURLE_OK);

  return kFailOk;
}


/**
 * Sets the URL specific options such as host to use and timeout.
 */
void S3FanoutManager::SetUrlOptions(JobInfo *info) const {
  CURL *curl_handle = info->curl_handle;
  CURLcode retval;

  pthread_mutex_lock(lock_options_);
  retval = curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, opt_timeout_);
  assert(retval == CURLE_OK);
  pthread_mutex_unlock(lock_options_);

  string url = MkUrl(info->hostname, info->bucket, (info->object_key));
  retval = curl_easy_setopt(curl_handle, CURLOPT_URL, url.c_str());
  assert(retval == CURLE_OK);
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

  return
      (info->error_code == kFailHostConnection ||
       info->error_code == kFailHostResolve ||
       info->error_code == kFailServiceUnavailable) &&
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
 * Checks the result of a curl request and implements the failure logic
 * and takes care of cleanup.
 *
 * @return true if request should be repeated, false otherwise
 */
bool S3FanoutManager::VerifyAndFinalize(const int curl_error, JobInfo *info) {
  LogCvmfs(kLogS3Fanout, kLogDebug, "Verify uploaded/tested object %s "
           "(curl error %d, info error %d, info request %d)",
           info->object_key.c_str(),
           curl_error, info->error_code, info->request);
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
      LogCvmfs(kLogS3Fanout, kLogStderr | kLogSyslogErr,
               "unexpected curl error (%d) while trying to upload %s",
               curl_error, info->object_key.c_str());
      info->error_code = kFailOther;
      break;
  }

  // Transform HEAD to PUT request
  if ((info->error_code == kFailNotFound) &&
      (info->request == JobInfo::kReqHead)) {
    LogCvmfs(kLogS3Fanout, kLogDebug, "not found: %s, uploading",
             info->object_key.c_str());
    info->request = JobInfo::kReqPut;
    curl_slist_free_all(info->http_headers);
    info->http_headers = NULL;
    s3fanout::Failures init_failure = InitializeRequest(info,
                                                        info->curl_handle);

    if (init_failure != s3fanout::kFailOk) {
      LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to initialize CURL handle "
                                         "(error: %d - %s | errno: %d)",
               init_failure, Code2Ascii(init_failure), errno);
      abort();
    }
    SetUrlOptions(info);
    // Reset origin
    if (info->origin == kOriginMem)
      info->origin_mem.pos = 0;
    if (info->origin == kOriginPath)
      rewind(info->origin_file);
    return true;  // Again, Put
  }

  // Determination if failed request should be repeated
  bool try_again = false;
  if (info->error_code != kFailOk) {
    try_again = CanRetry(info);
  }
  if (try_again) {
    if (info->request == JobInfo::kReqPut ||
        info->request == JobInfo::kReqPutNoCache) {
      LogCvmfs(kLogS3Fanout, kLogDebug, "Trying again to upload %s",
               info->object_key.c_str());
      // Reset origin
      if (info->origin == kOriginMem)
        info->origin_mem.pos = 0;
      if (info->origin == kOriginPath) {
        assert(info->origin_file != NULL);
        rewind(info->origin_file);
      }
    }
    Backoff(info);
    return true;  // try again
  }

  // Cleanup opened resources
  if (info->origin == kOriginPath) {
    assert(info->mmf == NULL);
    if (info->origin_file != NULL) {
      if (fclose(info->origin_file) != 0)
        info->error_code = kFailLocalIO;
      info->origin_file = NULL;
    }
  } else if (info->origin == kOriginMem) {
    assert(info->origin_file == NULL);
    if (info->mmf != NULL) {
      info->mmf->Unmap();
      delete info->mmf;
      info->mmf = NULL;
    }
  }

  return false;  // stop transfer
}

S3FanoutManager::S3FanoutManager() {
  pool_handles_idle_ = NULL;
  pool_handles_inuse_ = NULL;
  sharehandles_ = NULL;
  curl_sharehandles_ = NULL;
  pool_max_handles_ = 0;
  curl_multi_ = NULL;
  user_agent_ = NULL;

  atomic_init32(&multi_threaded_);
  watch_fds_ = NULL;
  watch_fds_size_ = 0;
  watch_fds_inuse_ = 0;
  watch_fds_max_ = 0;

  lock_options_ =
      reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_options_, NULL);
  assert(retval == 0);
  jobs_completed_lock_ =
      reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  retval = pthread_mutex_init(jobs_completed_lock_, NULL);
  assert(retval == 0);
  jobs_todo_lock_ =
      reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  retval = pthread_mutex_init(jobs_todo_lock_, NULL);
  assert(retval == 0);
  curl_handle_lock_ =
      reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  retval = pthread_mutex_init(curl_handle_lock_, NULL);
  assert(retval == 0);

  opt_timeout_ = 0;
  opt_max_retries_ = 0;
  opt_backoff_init_ms_ = 0;
  opt_backoff_max_ms_ = 0;
  opt_ipv4_only_ = false;

  available_jobs_ = 0;
  max_available_jobs_ = 0;
  thread_upload_ = 0;
  thread_upload_run_ = false;
  resolver_ = NULL;
  statistics_ = NULL;
}


S3FanoutManager::~S3FanoutManager() {
  pthread_mutex_destroy(lock_options_);
  free(lock_options_);
  pthread_mutex_destroy(jobs_completed_lock_);
  free(jobs_completed_lock_);
  pthread_mutex_destroy(jobs_todo_lock_);
  free(jobs_todo_lock_);
  pthread_mutex_destroy(curl_handle_lock_);
  free(curl_handle_lock_);
}


void S3FanoutManager::Init(const unsigned int max_pool_handles) {
  atomic_init32(&multi_threaded_);
  CURLcode retval = curl_global_init(CURL_GLOBAL_ALL);
  assert(retval == CURLE_OK);
  pool_handles_idle_ = new set<CURL *>;
  pool_handles_inuse_ = new set<CURL *>;
  curl_sharehandles_ = new map<CURL *, S3FanOutDnsEntry *>;
  sharehandles_ = new set<S3FanOutDnsEntry *>;
  pool_max_handles_ = max_pool_handles;
  watch_fds_max_ = 4*pool_max_handles_;

  max_available_jobs_ = 4*pool_max_handles_;
  available_jobs_ = new Semaphore(max_available_jobs_);
  assert(NULL != available_jobs_);

  opt_timeout_ = 20;
  statistics_ = new Statistics();
  user_agent_ = new string();
  *user_agent_ = "User-Agent: cvmfs " + string(VERSION);

  curl_multi_ = curl_multi_init();
  assert(curl_multi_ != NULL);
  CURLMcode mretval;
  mretval = curl_multi_setopt(curl_multi_, CURLMOPT_SOCKETFUNCTION,
                              CallbackCurlSocket);
  assert(mretval == CURLM_OK);
  mretval = curl_multi_setopt(curl_multi_, CURLMOPT_SOCKETDATA,
                              static_cast<void *>(this));
  assert(mretval == CURLM_OK);
  mretval = curl_multi_setopt(curl_multi_, CURLMOPT_MAX_TOTAL_CONNECTIONS,
                              pool_max_handles_);
  assert(mretval == CURLM_OK);

  prng_.InitLocaltime();

  // Parsing environment variables
  if ((getenv("CVMFS_IPV4_ONLY") != NULL) &&
      (strlen(getenv("CVMFS_IPV4_ONLY")) > 0)) {
    opt_ipv4_only_ = true;
  }

  watch_fds_ =
      static_cast<struct pollfd *>(smalloc(2 * sizeof(struct pollfd)));
  watch_fds_size_ = 2;
  watch_fds_inuse_ = 0;

  SetRetryParameters(3, 100, 2000);

  resolver_ = dns::CaresResolver::Create(opt_ipv4_only_, 2, 2000);
}


void S3FanoutManager::Fini() {
  if (atomic_xadd32(&multi_threaded_, 0) == 1) {
    // Shutdown I/O thread
    thread_upload_run_ = false;
    pthread_join(thread_upload_, NULL);
  }

  set<CURL *>::iterator             i    = pool_handles_idle_->begin();
  const set<CURL *>::const_iterator iEnd = pool_handles_idle_->end();
  for (; i != iEnd; ++i) {
    curl_easy_cleanup(*i);
  }

  set<S3FanOutDnsEntry *>::iterator             is    = sharehandles_->begin();
  const set<S3FanOutDnsEntry *>::const_iterator isEnd = sharehandles_->end();
  for (; is != isEnd; ++is) {
    curl_share_cleanup((*is)->sharehandle);
    curl_slist_free_all((*is)->clist);
    delete *is;
  }
  pool_handles_idle_->clear();
  curl_sharehandles_->clear();
  sharehandles_->clear();
  delete pool_handles_idle_;
  delete pool_handles_inuse_;
  delete curl_sharehandles_;
  delete sharehandles_;
  delete user_agent_;
  curl_multi_cleanup(curl_multi_);
  pool_handles_idle_ = NULL;
  pool_handles_inuse_ = NULL;
  curl_sharehandles_ = NULL;
  sharehandles_ = NULL;
  user_agent_ = NULL;
  curl_multi_ = NULL;

  delete statistics_;
  statistics_ = NULL;

  delete available_jobs_;

  curl_global_cleanup();
}


/**
 * Spawns the I/O worker thread.  No way back except Fini(); Init();
 */
void S3FanoutManager::Spawn() {
  LogCvmfs(kLogS3Fanout, kLogDebug, "S3FanoutManager spawned");

  thread_upload_run_ = true;
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
                                         const unsigned backoff_max_ms) {
  pthread_mutex_lock(lock_options_);
  opt_max_retries_ = max_retries;
  opt_backoff_init_ms_ = backoff_init_ms;
  opt_backoff_max_ms_ = backoff_max_ms;
  pthread_mutex_unlock(lock_options_);
}

/**
 * Get completed jobs, so they can be cleaned and deleted properly.
 */
int S3FanoutManager::PopCompletedJobs(std::vector<s3fanout::JobInfo*> *jobs) {
  pthread_mutex_lock(jobs_completed_lock_);
  std::vector<JobInfo*>::iterator             it    = jobs_completed_.begin();
  const std::vector<JobInfo*>::const_iterator itend = jobs_completed_.end();
  for (; it != itend; ++it) {
    jobs->push_back(*it);
  }
  jobs_completed_.clear();
  pthread_mutex_unlock(jobs_completed_lock_);

  return 0;
}

/**
 * Push new job to be uploaded to the S3 cloud storage.
 */
int S3FanoutManager::PushNewJob(JobInfo *info) {
  available_jobs_->Increment();

  pthread_mutex_lock(jobs_todo_lock_);
  jobs_todo_.push_back(info);
  pthread_mutex_unlock(jobs_todo_lock_);

  return 0;
}


/**
 * Performs given job synchronously.
 *
 * @return true if exists, otherwise false
 */
bool S3FanoutManager::DoSingleJob(JobInfo *info) const {
  bool retme = false;

  CURL *handle = AcquireCurlHandle();
  if (handle == NULL) {
    LogCvmfs(kLogS3Fanout, kLogStderr, "Failed to acquire CURL handle.");
    assert(handle != NULL);
  }

  InitializeRequest(info, handle);
  SetUrlOptions(info);

  CURLcode resl = curl_easy_perform(handle);
  if (resl == CURLE_OK && info->error_code == kFailOk) {
    retme = true;
  }

  ReleaseCurlHandle(info, handle);

  return retme;
}

//------------------------------------------------------------------------------


string Statistics::Print() const {
  return
      "Transferred Bytes:  " +
      StringifyInt(uint64_t(transferred_bytes)) + "\n" +
      "Transfer duration:  " +
      StringifyInt(uint64_t(transfer_time)) + " s\n" +
      "Number of requests: " +
      StringifyInt(num_requests) + "\n" +
      "Number of retries:  " +
      StringifyInt(num_retries) + "\n";
}

}  // namespace s3fanout
