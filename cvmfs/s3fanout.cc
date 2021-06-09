/**
 * This file is part of the CernVM File System.
 *
 * Runs a thread using libcurls asynchronous I/O mode to push data to S3
 */

#include <pthread.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <utility>

#include "cvmfs_config.h"
#include "platform.h"
#include "s3fanout.h"
#include "upload_facility.h"
#include "util/exception.h"
#include "util/posix.h"
#include "util/string.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace s3fanout {

const char *S3FanoutManager::kCacheControlCas = "Cache-Control: max-age=259200";
const char *S3FanoutManager::kCacheControlDotCvmfs =
  "Cache-Control: max-age=61";
const unsigned S3FanoutManager::kDefault429ThrottleMs = 250;
const unsigned S3FanoutManager::kMax429ThrottleMs = 10000;
const unsigned S3FanoutManager::kThrottleReportIntervalSec = 10;
const unsigned S3FanoutManager::kDefaultHTTPPort = 80;


/**
 * Parses Retry-After and X-Retry-In headers attached to HTTP 429 responses
 */
void S3FanoutManager::DetectThrottleIndicator(
  const std::string &header,
  JobInfo *info)
{
  std::string value_str;
  if (HasPrefix(header, "retry-after:", true))
    value_str = header.substr(12);
  if (HasPrefix(header, "x-retry-in:", true))
    value_str = header.substr(11);

  value_str = Trim(value_str, true /* trim_newline */);
  if (!value_str.empty()) {
    unsigned value_numeric = String2Uint64(value_str);
    unsigned value_ms =
      HasSuffix(value_str, "ms", true /* ignore_case */) ?
        value_numeric : (value_numeric * 1000);
    if (value_ms > 0)
      info->throttle_ms = std::min(value_ms, kMax429ThrottleMs);
  }
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
      LogCvmfs(kLogS3Fanout, kLogDebug, "http status error code [info %p]: %s",
               info, header_line.c_str());
      if (header_line.length() < i+3) {
        LogCvmfs(kLogS3Fanout, kLogStderr, "S3: invalid HTTP response '%s'",
                 header_line.c_str());
        info->error_code = kFailOther;
        return 0;
      }
      info->http_error = String2Int64(string(&header_line[i], 3));

      switch (info->http_error) {
        case 429:
          info->error_code = kFailRetry;
          info->throttle_ms = S3FanoutManager::kDefault429ThrottleMs;
          info->throttle_timestamp = platform_monotonic_time();
          return num_bytes;
        case 503:
        case 502:  // Can happen if the S3 gateway-backend connection breaks
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
          return num_bytes;
        default:
          info->error_code = kFailOther;
      }
      return 0;
    }
  }

  if (info->error_code == kFailRetry) {
    S3FanoutManager::DetectThrottleIndicator(header_line, info);
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

  uint64_t read_bytes = info->origin->Read(ptr, num_bytes);

  LogCvmfs(kLogS3Fanout, kLogDebug,
           "source buffer pushed out %d bytes", read_bytes);

  return read_bytes;
}


/**
 * For the time being, ignore all received information in the HTTP body
 */
static size_t CallbackCurlBody(
  char * /*ptr*/, size_t size, size_t nmemb, void * /*userdata*/)
{
  return size * nmemb;
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
  // First 2 fds are job and terminate pipes (not curl related)
  unsigned index;
  for (index = 2; index < s3fanout_mgr->watch_fds_inuse_; ++index) {
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
      s3fanout_mgr->watch_fds_[index].events = POLLIN | POLLPRI;
      break;
    case CURL_POLL_OUT:
      s3fanout_mgr->watch_fds_[index].events = POLLOUT | POLLWRBAND;
      break;
    case CURL_POLL_INOUT:
      s3fanout_mgr->watch_fds_[index].events =
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
      PANIC(NULL);
  }

  return 0;
}


/**
 * Worker thread event loop.
 */
void *S3FanoutManager::MainUpload(void *data) {
  LogCvmfs(kLogS3Fanout, kLogDebug, "Upload I/O thread started");
  S3FanoutManager *s3fanout_mgr = static_cast<S3FanoutManager *>(data);

  s3fanout_mgr->InitPipeWatchFds();

  // Don't schedule more jobs into the multi handle than the maximum number of
  // parallel connections.  This should prevent starvation and thus a timeout
  // of the authorization header (CVM-1339).
  unsigned jobs_in_flight = 0;

  while (true) {
    // Check events with 100ms timeout
    int timeout_ms = 100;
    int retval = poll(s3fanout_mgr->watch_fds_, s3fanout_mgr->watch_fds_inuse_,
                      timeout_ms);
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
      assert(errno == EINTR);
      continue;
    }

    // Terminate I/O thread
    if (s3fanout_mgr->watch_fds_[0].revents)
      break;

    // New job incoming
    if (s3fanout_mgr->watch_fds_[1].revents) {
      s3fanout_mgr->watch_fds_[1].revents = 0;
      JobInfo *info;
      ReadPipe(s3fanout_mgr->pipe_jobs_[0], &info, sizeof(info));
      CURL *handle = s3fanout_mgr->AcquireCurlHandle();
      if (handle == NULL) {
        PANIC(kLogStderr, "Failed to acquire CURL handle.");
      }
      s3fanout::Failures init_failure =
        s3fanout_mgr->InitializeRequest(info, handle);
      if (init_failure != s3fanout::kFailOk) {
        PANIC(kLogStderr,
              "Failed to initialize CURL handle (error: %d - %s | errno: %d)",
              init_failure, Code2Ascii(init_failure), errno);
      }
      s3fanout_mgr->SetUrlOptions(info);

      curl_multi_add_handle(s3fanout_mgr->curl_multi_, handle);
      s3fanout_mgr->active_requests_->insert(info);
      jobs_in_flight++;
      int still_running = 0, retval = 0;
      retval = curl_multi_socket_action(s3fanout_mgr->curl_multi_,
                                        CURL_SOCKET_TIMEOUT,
                                        0,
                                        &still_running);

      LogCvmfs(kLogS3Fanout, kLogDebug,
               "curl_multi_socket_action: %d - %d",
               retval, still_running);
    }


    // Activity on curl sockets
    // Within this loop the curl_multi_socket_action() may cause socket(s)
    // to be removed from watch_fds_. If a socket is removed it is replaced
    // by the socket at the end of the array and the inuse count is decreased.
    // Therefore loop over the array in reverse order.
    // First 2 fds are job and terminate pipes (not curl related)
    for (int32_t i = s3fanout_mgr->watch_fds_inuse_ - 1; i >= 2; --i) {
      if (static_cast<uint32_t>(i) >= s3fanout_mgr->watch_fds_inuse_) {
        continue;
      }
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
                                            &msgs_in_queue)))
    {
      assert(curl_msg->msg == CURLMSG_DONE);

      s3fanout_mgr->statistics_->num_requests++;
      JobInfo *info;
      CURL *easy_handle = curl_msg->easy_handle;
      int curl_error = curl_msg->data.result;
      curl_easy_getinfo(easy_handle, CURLINFO_PRIVATE, &info);

      curl_multi_remove_handle(s3fanout_mgr->curl_multi_, easy_handle);
      if (s3fanout_mgr->VerifyAndFinalize(curl_error, info)) {
        curl_multi_add_handle(s3fanout_mgr->curl_multi_, easy_handle);
        int still_running = 0;
        curl_multi_socket_action(s3fanout_mgr->curl_multi_,
                                 CURL_SOCKET_TIMEOUT,
                                 0,
                                 &still_running);
      } else {
        // Return easy handle into pool and write result back
        jobs_in_flight--;
        s3fanout_mgr->active_requests_->erase(info);
        s3fanout_mgr->ReleaseCurlHandle(info, easy_handle);
        s3fanout_mgr->available_jobs_->Decrement();

        // Add to list of completed jobs
        s3fanout_mgr->PushCompletedJob(info);
      }
    }
  }

  set<CURL *>::iterator i = s3fanout_mgr->pool_handles_inuse_->begin();
  const set<CURL *>::const_iterator i_end =
    s3fanout_mgr->pool_handles_inuse_->end();
  for (; i != i_end; ++i) {
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
    retval = curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, CallbackCurlBody);
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

  if (pool_handles_idle_->size() > config_.pool_max_handles) {
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

void S3FanoutManager::InitPipeWatchFds() {
  assert(watch_fds_inuse_ == 0);
  assert(watch_fds_size_ >= 2);
  watch_fds_[0].fd = pipe_terminate_[0];
  watch_fds_[0].events = POLLIN | POLLPRI;
  watch_fds_[0].revents = 0;
  ++watch_fds_inuse_;
  watch_fds_[1].fd = pipe_jobs_[0];
  watch_fds_[1].events = POLLIN | POLLPRI;
  watch_fds_[1].revents = 0;
  ++watch_fds_inuse_;
}

/**
 * The Amazon AWS 2 authorization header according to
 * http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#ConstructingTheAuthenticationHeader
 */
bool S3FanoutManager::MkV2Authz(const JobInfo &info, vector<string> *headers)
  const
{
  string payload_hash;
  bool retval = MkPayloadHash(info, &payload_hash);
  if (!retval)
    return false;
  string content_type = GetContentType(info);
  string request = GetRequestString(info);

  string timestamp = RfcTimestamp();
  string to_sign = request + "\n" +
                   payload_hash + "\n" +
                   content_type + "\n" +
                   timestamp + "\n" +
                   "x-amz-acl:public-read" + "\n" +  // default ACL
                   "/" + config_.bucket + "/" + info.object_key;
  LogCvmfs(kLogS3Fanout, kLogDebug, "%s string to sign for: %s",
           request.c_str(), info.object_key.c_str());

  shash::Any hmac;
  hmac.algorithm = shash::kSha1;
  shash::Hmac(config_.secret_key,
              reinterpret_cast<const unsigned char *>(to_sign.data()),
              to_sign.length(), &hmac);

  headers->push_back("Authorization: AWS " + config_.access_key + ":" +
                     Base64(string(reinterpret_cast<char *>(hmac.digest),
                                   hmac.GetDigestSize())));
  headers->push_back("Date: " + timestamp);
  headers->push_back("X-Amz-Acl: public-read");
  if (!payload_hash.empty())
    headers->push_back("Content-MD5: " + payload_hash);
  if (!content_type.empty())
    headers->push_back("Content-Type: " + content_type);
  return true;
}


string S3FanoutManager::GetUriEncode(const string &val, bool encode_slash)
  const
{
  string result;
  const unsigned len = val.length();
  result.reserve(len);
  for (unsigned i = 0; i < len; ++i) {
    char c = val[i];
    if ((c >= 'A' && c <= 'Z') ||
        (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9') ||
        c == '_' || c == '-' || c == '~' || c == '.')
    {
      result.push_back(c);
    } else if (c == '/') {
      if (encode_slash) {
        result += "%2F";
      } else {
        result.push_back(c);
      }
    } else {
      result.push_back('%');
      result.push_back((c / 16) + ((c / 16 <= 9) ? '0' : 'A'-10));
      result.push_back((c % 16) + ((c % 16 <= 9) ? '0' : 'A'-10));
    }
  }
  return result;
}


string S3FanoutManager::GetAwsV4SigningKey(const string &date) const
{
  if (last_signing_key_.first == date)
    return last_signing_key_.second;

  string date_key = shash::Hmac256("AWS4" + config_.secret_key, date, true);
  string date_region_key = shash::Hmac256(date_key, config_.region, true);
  string date_region_service_key = shash::Hmac256(date_region_key, "s3", true);
  string signing_key =
    shash::Hmac256(date_region_service_key, "aws4_request", true);
  last_signing_key_.first = date;
  last_signing_key_.second = signing_key;
  return signing_key;
}


/**
 * The Amazon AWS4 authorization header according to
 * http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html
 */
bool S3FanoutManager::MkV4Authz(const JobInfo &info, vector<string> *headers)
  const
{
  string payload_hash;
  bool retval = MkPayloadHash(info, &payload_hash);
  if (!retval)
    return false;
  string content_type = GetContentType(info);
  string timestamp = IsoTimestamp();
  string date = timestamp.substr(0, 8);
  vector<string> tokens = SplitString(complete_hostname_, ':');
  assert(tokens.size() <= 2);
  string canonical_hostname = tokens[0];
  if (tokens.size() == 2 && String2Uint64(tokens[1]) != kDefaultHTTPPort)
    canonical_hostname += ":" + tokens[1];

  string signed_headers;
  string canonical_headers;
  if (!content_type.empty()) {
    signed_headers += "content-type;";
    headers->push_back("Content-Type: " + content_type);
    canonical_headers += "content-type:" + content_type + "\n";
  }
  signed_headers += "host;x-amz-acl;x-amz-content-sha256;x-amz-date";
  canonical_headers +=
    "host:" + canonical_hostname + "\n" +
    "x-amz-acl:public-read\n"
    "x-amz-content-sha256:" + payload_hash + "\n" +
    "x-amz-date:" + timestamp + "\n";

  string scope = date + "/" + config_.region + "/s3/aws4_request";
  string uri = config_.dns_buckets ?
                 (string("/") + info.object_key) :
                 (string("/") + config_.bucket + "/" + info.object_key);

  string canonical_request =
    GetRequestString(info) + "\n" +
    GetUriEncode(uri, false) + "\n" +
    "\n" +
    canonical_headers + "\n" +
    signed_headers + "\n" +
    payload_hash;

  string hash_request = shash::Sha256String(canonical_request.c_str());

  string string_to_sign =
    "AWS4-HMAC-SHA256\n" +
    timestamp + "\n" +
    scope + "\n" +
    hash_request;

  string signing_key = GetAwsV4SigningKey(date);
  string signature = shash::Hmac256(signing_key, string_to_sign);

  headers->push_back("X-Amz-Acl: public-read");
  headers->push_back("X-Amz-Content-Sha256: " + payload_hash);
  headers->push_back("X-Amz-Date: " + timestamp);
  headers->push_back(
    "Authorization: AWS4-HMAC-SHA256 "
    "Credential=" + config_.access_key + "/" + scope + ","
    "SignedHeaders=" + signed_headers + ","
    "Signature=" + signature);
  return true;
}

/**
 * The Azure Blob authorization header according to
 * https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key 
 */
bool S3FanoutManager::MkAzureAuthz(const JobInfo &info, vector<string> *headers)
  const
{
  string timestamp = RfcTimestamp();
  string canonical_headers =
    "x-ms-blob-type:BlockBlob\nx-ms-date:" +
    timestamp +
    "\nx-ms-version:2011-08-18";
  string canonical_resource =
    "/" + config_.access_key + "/" + config_.bucket + "/" + info.object_key;

  string string_to_sign;
  if ((info.request == JobInfo::kReqHeadOnly) ||
     (info.request == JobInfo::kReqHeadPut) ||
     (info.request == JobInfo::kReqDelete)) {
    string_to_sign =
      GetRequestString(info) +
      string("\n\n\n") +
      "\n\n\n\n\n\n\n\n\n" +
      canonical_headers + "\n" +
      canonical_resource;
  } else {
    string_to_sign =
      GetRequestString(info) +
      string("\n\n\n") +
      string(StringifyInt(info.origin->GetSize())) + "\n\n\n\n\n\n\n\n\n" +
      canonical_headers + "\n" +
      canonical_resource;
  }

  string signing_key;
  int retval = Debase64(config_.secret_key, &signing_key);
  if (!retval)
    return false;

  string signature = shash::Hmac256(signing_key, string_to_sign, true);

  headers->push_back("x-ms-date: " + timestamp);
  headers->push_back("x-ms-version: 2011-08-18");
  headers->push_back(
    "Authorization: SharedKey " + config_.access_key + ":" + Base64(signature));
  headers->push_back("x-ms-blob-type: BlockBlob");
  return true;
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
  if (!HasPrefix(host_with_port, "http://", false /*ignore_case*/))
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
  curl_sharehandles_->insert(
    std::pair<CURL *, S3FanOutDnsEntry *>(handle, dnse));
  dnse->counter++;
  InitializeDnsSettingsCurl(handle, dnse->sharehandle, dnse->clist);

  return 0;
}


bool S3FanoutManager::MkPayloadHash(const JobInfo &info, string *hex_hash)
  const
{
  if ((info.request == JobInfo::kReqHeadOnly) ||
      (info.request == JobInfo::kReqHeadPut) ||
      (info.request == JobInfo::kReqDelete))
  {
    switch (config_.authz_method) {
      case kAuthzAwsV2:
        hex_hash->clear();
        break;
      case kAuthzAwsV4:
        // Sha256 over empty string
        *hex_hash =
          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        break;
      case kAuthzAzure:
        // no payload hash required for Azure signature
        hex_hash->clear();
        break;
      default:
        PANIC(NULL);
    }
    return true;
  }

  // PUT, there is actually payload
  shash::Any payload_hash(shash::kMd5);

  unsigned char *data;
  unsigned int nbytes =
    info.origin->Data(reinterpret_cast<void **>(&data),
                             info.origin->GetSize(), 0);
  assert(nbytes == info.origin->GetSize());

  switch (config_.authz_method) {
    case kAuthzAwsV2:
      shash::HashMem(data, nbytes, &payload_hash);
      *hex_hash =
        Base64(string(reinterpret_cast<char *>(payload_hash.digest),
                      payload_hash.GetDigestSize()));
      return true;
    case kAuthzAwsV4:
      *hex_hash =
        shash::Sha256Mem(data, nbytes);
      return true;
    case kAuthzAzure:
      // no payload hash required for Azure signature
      hex_hash->clear();
      return true;
    default:
      PANIC(NULL);
  }
}

string S3FanoutManager::GetRequestString(const JobInfo &info) const {
  switch (info.request) {
    case JobInfo::kReqHeadOnly:
    case JobInfo::kReqHeadPut:
      return "HEAD";
    case JobInfo::kReqPutCas:
    case JobInfo::kReqPutDotCvmfs:
    case JobInfo::kReqPutHtml:
    case JobInfo::kReqPutBucket:
      return "PUT";
    case JobInfo::kReqDelete:
      return "DELETE";
    default:
      PANIC(NULL);
  }
}


string S3FanoutManager::GetContentType(const JobInfo &info) const {
  switch (info.request) {
    case JobInfo::kReqHeadOnly:
    case JobInfo::kReqHeadPut:
    case JobInfo::kReqDelete:
      return "";
    case JobInfo::kReqPutCas:
      return "application/octet-stream";
    case JobInfo::kReqPutDotCvmfs:
      return "application/x-cvmfs";
    case JobInfo::kReqPutHtml:
      return "text/html";
    case JobInfo::kReqPutBucket:
      return "text/xml";
    default:
      PANIC(NULL);
  }
}


/**
 * Request parameters set the URL and other options such as timeout and
 * proxy.
 */
Failures S3FanoutManager::InitializeRequest(JobInfo *info, CURL *handle) const {
  // Initialize internal download state
  info->curl_handle = handle;
  info->error_code = kFailOk;
  info->http_error = 0;
  info->num_retries = 0;
  info->backoff_ms = 0;
  info->throttle_ms = 0;
  info->throttle_timestamp = 0;
  info->http_headers = NULL;
  // info->payload_size is needed in S3Uploader::MainCollectResults,
  // where info->origin is already destroyed.
  info->payload_size = info->origin->GetSize();

  InitializeDnsSettings(handle, complete_hostname_);

  CURLcode retval;
  if ((info->request == JobInfo::kReqHeadOnly) ||
      (info->request == JobInfo::kReqHeadPut) ||
      (info->request == JobInfo::kReqDelete))
  {
    retval = curl_easy_setopt(handle, CURLOPT_UPLOAD, 0);
    assert(retval == CURLE_OK);
    retval = curl_easy_setopt(handle, CURLOPT_NOBODY, 1);
    assert(retval == CURLE_OK);

    if (info->request == JobInfo::kReqDelete)
    {
      retval = curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST,
                                GetRequestString(*info).c_str());
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
    retval = curl_easy_setopt(handle, CURLOPT_INFILESIZE_LARGE,
                              static_cast<curl_off_t>(info->origin->GetSize()));
    assert(retval == CURLE_OK);

    if (info->request == JobInfo::kReqPutDotCvmfs) {
      info->http_headers =
          curl_slist_append(info->http_headers, kCacheControlDotCvmfs);
    } else if (info->request == JobInfo::kReqPutCas) {
      info->http_headers =
          curl_slist_append(info->http_headers, kCacheControlCas);
    }
  }

  bool retval_b;

  // Authorization
  vector<string> authz_headers;
  switch (config_.authz_method) {
    case kAuthzAwsV2:
      retval_b = MkV2Authz(*info, &authz_headers);
      break;
    case kAuthzAwsV4:
      retval_b = MkV4Authz(*info, &authz_headers);
      break;
    case kAuthzAzure:
      retval_b = MkAzureAuthz(*info, &authz_headers);
      break;
    default:
      PANIC(NULL);
  }
  if (!retval_b)
    return kFailLocalIO;
  for (unsigned i = 0; i < authz_headers.size(); ++i) {
    info->http_headers =
      curl_slist_append(info->http_headers, authz_headers[i].c_str());
  }

  // Common headers
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
  retval = curl_easy_setopt(handle, CURLOPT_HEADERDATA,
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

  retval = curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT,
                            config_.opt_timeout_sec);
  assert(retval == CURLE_OK);
  retval = curl_easy_setopt(curl_handle, CURLOPT_LOW_SPEED_LIMIT,
                            kLowSpeedLimit);
  assert(retval == CURLE_OK);
  retval = curl_easy_setopt(curl_handle, CURLOPT_LOW_SPEED_TIME,
                            config_.opt_timeout_sec);
  assert(retval == CURLE_OK);

  if (is_curl_debug_) {
    retval = curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 1);
    assert(retval == CURLE_OK);
  }

  string url = MkUrl(info->object_key);
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
  return
      (info->error_code == kFailHostConnection ||
       info->error_code == kFailHostResolve ||
       info->error_code == kFailServiceUnavailable ||
       info->error_code == kFailRetry) &&
      (info->num_retries < config_.opt_max_retries);
}


/**
 * Backoff for retry to introduce a jitter into a upload sequence.
 *
 * \return true if backoff has been performed, false otherwise
 */
void S3FanoutManager::Backoff(JobInfo *info) {
  if (info->error_code != kFailRetry)
    info->num_retries++;
  statistics_->num_retries++;

  if (info->throttle_ms > 0) {
    LogCvmfs(kLogS3Fanout, kLogDebug, "throttling for %d ms",
             info->throttle_ms);
    uint64_t now = platform_monotonic_time();
    if ((info->throttle_timestamp + (info->throttle_ms / 1000)) >= now) {
      if ((now - timestamp_last_throttle_report_) > kThrottleReportIntervalSec)
      {
        LogCvmfs(kLogS3Fanout, kLogStdout,
                 "Warning: S3 backend throttling %ums "
                 "(total backoff time so far %ums)",
                 info->throttle_ms,
                 statistics_->ms_throttled);
        timestamp_last_throttle_report_ = now;
      }
      statistics_->ms_throttled += info->throttle_ms;
      SafeSleepMs(info->throttle_ms);
    }
  } else {
    if (info->backoff_ms == 0) {
      // Must be != 0
      info->backoff_ms = prng_.Next(config_.opt_backoff_init_ms + 1);
    } else {
      info->backoff_ms *= 2;
    }
    if (info->backoff_ms > config_.opt_backoff_max_ms)
      info->backoff_ms = config_.opt_backoff_max_ms;

    LogCvmfs(kLogS3Fanout, kLogDebug, "backing off for %d ms",
             info->backoff_ms);
    SafeSleepMs(info->backoff_ms);
  }
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
      if ((info->error_code != kFailRetry) &&
          (info->error_code != kFailNotFound))
      {
        info->error_code = kFailOk;
      }
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
      (info->request == JobInfo::kReqHeadPut))
  {
    LogCvmfs(kLogS3Fanout, kLogDebug, "not found: %s, uploading",
             info->object_key.c_str());
    info->request = JobInfo::kReqPutCas;
    curl_slist_free_all(info->http_headers);
    info->http_headers = NULL;
    s3fanout::Failures init_failure = InitializeRequest(info,
                                                        info->curl_handle);

    if (init_failure != s3fanout::kFailOk) {
      PANIC(kLogStderr,
            "Failed to initialize CURL handle "
            "(error: %d - %s | errno: %d)",
            init_failure, Code2Ascii(init_failure), errno);
    }
    SetUrlOptions(info);
    // Reset origin
    info->origin->Rewind();
    return true;  // Again, Put
  }

  // Determination if failed request should be repeated
  bool try_again = false;
  if (info->error_code != kFailOk) {
    try_again = CanRetry(info);
  }
  if (try_again) {
    if (info->request == JobInfo::kReqPutCas ||
        info->request == JobInfo::kReqPutDotCvmfs ||
        info->request == JobInfo::kReqPutHtml) {
      LogCvmfs(kLogS3Fanout, kLogDebug, "Trying again to upload %s",
               info->object_key.c_str());
      // Reset origin
      info->origin->Rewind();
    }
    Backoff(info);
    info->error_code = kFailOk;
    info->http_error = 0;
    info->throttle_ms = 0;
    info->backoff_ms = 0;
    info->throttle_timestamp = 0;
    return true;  // try again
  }

  // Cleanup opened resources
  info->origin.Destroy();

  if ((info->error_code != kFailOk) &&
      (info->http_error != 0) && (info->http_error != 404))
  {
    LogCvmfs(kLogS3Fanout, kLogStderr, "S3: HTTP failure %d", info->http_error);
  }
  return false;  // stop transfer
}

S3FanoutManager::S3FanoutManager(const S3Config &config) : config_(config) {
  atomic_init32(&multi_threaded_);
  MakePipe(pipe_terminate_);
  MakePipe(pipe_jobs_);
  MakePipe(pipe_completed_);

  int retval;
  jobs_todo_lock_ =
      reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  retval = pthread_mutex_init(jobs_todo_lock_, NULL);
  assert(retval == 0);
  curl_handle_lock_ =
      reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  retval = pthread_mutex_init(curl_handle_lock_, NULL);
  assert(retval == 0);

  active_requests_ = new set<JobInfo *>;
  pool_handles_idle_ = new set<CURL *>;
  pool_handles_inuse_ = new set<CURL *>;
  curl_sharehandles_ = new map<CURL *, S3FanOutDnsEntry *>;
  sharehandles_ = new set<S3FanOutDnsEntry *>;
  watch_fds_max_ = 4 * config_.pool_max_handles;
  max_available_jobs_ = 4 * config_.pool_max_handles;
  available_jobs_ = new Semaphore(max_available_jobs_);
  assert(NULL != available_jobs_);

  statistics_ = new Statistics();
  user_agent_ = new string();
  *user_agent_ = "User-Agent: cvmfs " + string(VERSION);
  complete_hostname_ = MkCompleteHostname();

  CURLcode cretval = curl_global_init(CURL_GLOBAL_ALL);
  assert(cretval == CURLE_OK);
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
                              config_.pool_max_handles);
  assert(mretval == CURLM_OK);

  prng_.InitLocaltime();

  thread_upload_ = 0;
  timestamp_last_throttle_report_ = 0;
  is_curl_debug_ = (getenv("_CVMFS_CURL_DEBUG") != NULL);

  // Parsing environment variables
  if ((getenv("CVMFS_IPV4_ONLY") != NULL) &&
      (strlen(getenv("CVMFS_IPV4_ONLY")) > 0)) {
    opt_ipv4_only_ = true;
  } else {
    opt_ipv4_only_ = false;
  }

  resolver_ = dns::CaresResolver::Create(opt_ipv4_only_, 2, 2000);

  watch_fds_ = static_cast<struct pollfd *>(smalloc(4 * sizeof(struct pollfd)));
  watch_fds_size_ = 4;
  watch_fds_inuse_ = 0;
}

S3FanoutManager::~S3FanoutManager() {
  pthread_mutex_destroy(jobs_todo_lock_);
  free(jobs_todo_lock_);
  pthread_mutex_destroy(curl_handle_lock_);
  free(curl_handle_lock_);

  if (atomic_xadd32(&multi_threaded_, 0) == 1) {
    // Shutdown I/O thread
    char buf = 'T';
    WritePipe(pipe_terminate_[1], &buf, 1);
    pthread_join(thread_upload_, NULL);
  }
  ClosePipe(pipe_terminate_);
  ClosePipe(pipe_jobs_);
  ClosePipe(pipe_completed_);

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
  delete active_requests_;
  delete pool_handles_idle_;
  delete pool_handles_inuse_;
  delete curl_sharehandles_;
  delete sharehandles_;
  delete user_agent_;
  curl_multi_cleanup(curl_multi_);

  delete statistics_;

  delete available_jobs_;

  curl_global_cleanup();
}

/**
 * Spawns the I/O worker thread.  No way back except ~S3FanoutManager.
 */
void S3FanoutManager::Spawn() {
  LogCvmfs(kLogS3Fanout, kLogDebug, "S3FanoutManager spawned");

  int retval = pthread_create(&thread_upload_, NULL, MainUpload,
                              static_cast<void *>(this));
  assert(retval == 0);

  atomic_inc32(&multi_threaded_);
}

const Statistics &S3FanoutManager::GetStatistics() {
  return *statistics_;
}

/**
 * Push new job to be uploaded to the S3 cloud storage.
 */
void S3FanoutManager::PushNewJob(JobInfo *info) {
  available_jobs_->Increment();
  WritePipe(pipe_jobs_[1], &info, sizeof(info));
}

/**
 * Push completed job to list of completed jobs
 */
void S3FanoutManager::PushCompletedJob(JobInfo *info) {
  WritePipe(pipe_completed_[1], &info, sizeof(info));
}

/**
 * Pop completed job
 */
JobInfo *S3FanoutManager::PopCompletedJob() {
  JobInfo *info;
  ReadPipe(pipe_completed_[0], &info, sizeof(info));
  return info;
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
