/**
 * This file is part of the CernVM File System.
 *
 * The download module provides an interface for fetching files via HTTP
 * and file.  It is internally using libcurl and the asynchronous DNS resolver
 * c-ares.  The JobInfo struct describes a single file/url to download and
 * keeps the state during the several phases of downloading.
 *
 * The module starts in single-threaded mode and can be switched to multi-
 * threaded mode by Spawn().  In multi-threaded mode, the Fetch() function still
 * blocks but there is a separate I/O thread using asynchronous I/O, which
 * maintains all concurrent connections simultaneously.  As there might be more
 * than 1024 file descriptors for the CernVM-FS process, the I/O thread uses
 * poll and the libcurl multi socket interface.
 *
 * While downloading, files can be decompressed and the secure hash can be
 * calculated on the fly.
 *
 * The module also implements failure handling.  If corrupted data has been
 * downloaded, the transfer is restarted using HTTP "no-cache" pragma.
 * A "host chain" can be configured.  When a host fails, there is automatic
 * fail-over to the next host in the chain until all hosts are probed.
 * Similarly a chain of proxy sets can be configured.  Inside a proxy set,
 * proxies are selected randomly (load-balancing set).
 */

//TODO: MS for time summing
#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "download.h"

#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>
#include <pthread.h>
#include <alloca.h>
#include <errno.h>
#include <poll.h>
#include <sys/time.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <cstdio>

#include <set>

#include "duplex_curl.h"
#include "logging.h"
#include "atomic.h"
#include "hash.h"
#include "util.h"
#include "compression.h"
#include "smalloc.h"

using namespace std;  // NOLINT

namespace download {

set<CURL *>  *pool_handles_idle_ = NULL;
set<CURL *>  *pool_handles_inuse_ = NULL;
uint32_t pool_max_handles_;
CURLM *curl_multi_ = NULL;
curl_slist *http_headers_ = NULL;
curl_slist *http_headers_nocache_ = NULL;

pthread_t thread_download_;
atomic_int32 multi_threaded_;
int pipe_terminate_[2];

int pipe_jobs_[2];
struct pollfd *watch_fds_ = NULL;
uint32_t watch_fds_size_ = 0;
uint32_t watch_fds_inuse_ = 0;
uint32_t watch_fds_max_;

pthread_mutex_t lock_options_ = PTHREAD_MUTEX_INITIALIZER;
char *opt_dns_server_ = NULL;
unsigned opt_timeout_proxy_ ;
unsigned opt_timeout_direct_;
vector<string> *opt_host_chain_ = NULL;
vector<int> *opt_host_chain_rtt_ = NULL; /**< created by SetHostChain(),
  filled by probe_hosts.  Contains time to get .cvmfschecksum in ms.
  -1 is unprobed, -2 is error */
unsigned opt_host_chain_current_;
vector< vector<string> > *opt_proxy_groups_ = NULL;
unsigned opt_proxy_groups_current_;
unsigned opt_proxy_groups_current_burned_;
unsigned opt_num_proxies_;

// Writes and reads should be atomic because reading happens in a different
// thread than writing.
double stat_transferred_bytes_;
double stat_transfer_time_;


/**
 * Escape special chars from the URL, except for ':' and '/',
 * which should keep their meaning.
 */
static string EscapeUrl(const string &url) {
  string escaped;

  for (unsigned i = 0, s = url.length(); i < s; ++i) {
    if (((url[i] >= '0') && (url[i] <= '9')) ||
        ((url[i] >= 'A') && (url[i] <= 'Z')) ||
        ((url[i] >= 'a') && (url[i] <= 'z')) ||
        (url[i] == '/') || (url[i] == ':') || (url[i] == '.') ||
        (url[i] == '+') || (url[i] == '-'))
    {
      escaped += url[i];
    } else {
      escaped += '%';
      escaped += (url[i] / 16) + ((url[i] / 16 <= 9) ? '0' : 'A'-10);
      escaped += (url[i] % 16) + ((url[i] % 16 <= 9) ? '0' : 'A'-10);
    }
  }
  LogCvmfs(kLogDownload, kLogDebug, "escaped %s to %s",
           url.c_str(), escaped.c_str());

  return escaped;
}


/**
 * Switches to the next host in the chain.  If info is set, switch only if the
 * current host is identical to the one used by info, otherwise another transfer
 * has already done the switch.
 */
void SwitchHost(JobInfo *info) {
  bool do_switch = true;

  pthread_mutex_lock(&lock_options_);
  if (!opt_host_chain_ || (opt_host_chain_->size() == 1)) {
    pthread_mutex_unlock(&lock_options_);
    return;
  }

  if (info) {
    char *effective_url;
    curl_easy_getinfo(info->curl_handle, CURLINFO_EFFECTIVE_URL,
                      &effective_url);
    if (!HasPrefix(string(effective_url),
                   (*opt_host_chain_)[opt_host_chain_current_], false))
    {
      do_switch = false;
    }
  }

  if (do_switch) {
    opt_host_chain_current_ = (opt_host_chain_current_+1) %
                              opt_host_chain_->size();
    LogCvmfs(kLogDownload, kLogDebug, "switching host to %s",
             (*opt_host_chain_)[opt_host_chain_current_].c_str());
  }
  pthread_mutex_unlock(&lock_options_);
}


void SwitchHost() {
  SwitchHost(NULL);
}


/**
 * Jumps to the next proxy in the ring of forward proxy servers.
 * Selects one randomly from a load-balancing group.
 *
 * If info is set, switch only if the current host is identical to the one used
 * by info, otherwise another transfer has already done the switch.
 */
static void SwitchProxy(JobInfo *info) {
  pthread_mutex_lock(&lock_options_);

  if (!opt_proxy_groups_) {
    pthread_mutex_unlock(&lock_options_);
    return;
  }
  if (info &&
      ((*opt_proxy_groups_)[opt_proxy_groups_current_][0] != info->proxy))
  {
    pthread_mutex_unlock(&lock_options_);
    return;
  }

  // If all proxies from the current load-balancing group are burned, switch to
  // another group
  if (opt_proxy_groups_current_burned_ ==
      (*opt_proxy_groups_)[opt_proxy_groups_current_].size())
  {
    opt_proxy_groups_current_burned_ = 0;
    if (opt_proxy_groups_->size() > 1) {
      opt_proxy_groups_current_ = (opt_proxy_groups_current_ + 1) %
                                  opt_proxy_groups_->size();
    }
  }

  vector<string> *group = &((*opt_proxy_groups_)[opt_proxy_groups_current_]);
  const unsigned group_size = group->size();

  // Move active proxy to the back
  if (opt_proxy_groups_current_burned_) {
    const string swap = (*group)[0];
    (*group)[0] = (*group)[group_size - opt_proxy_groups_current_burned_];
    (*group)[group_size - opt_proxy_groups_current_burned_] = swap;
  }
  opt_proxy_groups_current_burned_++;

  // Select new one
  if ((group_size - opt_proxy_groups_current_burned_) > 0) {
    int select = random() % (group_size - opt_proxy_groups_current_burned_ + 1);

    // Move selected proxy to front
    const string swap = (*group)[select];
    (*group)[select] = (*group)[0];
    (*group)[0] = swap;
  }

  LogCvmfs(kLogDownload, kLogDebug, "switched to proxy %s, %d remaining in "
           "the group",
           (*group)[0].c_str(), group_size - opt_proxy_groups_current_burned_);

  pthread_mutex_unlock(&lock_options_);
}


/**
 * Selects a new random proxy in the current load-balancing group.  Resets the
 * "burned" counter.
 */
void RebalanceProxies() {
  pthread_mutex_lock(&lock_options_);

  if (!opt_proxy_groups_) {
    pthread_mutex_unlock(&lock_options_);
    return;
  }

  opt_proxy_groups_current_burned_ = 0;
  vector<string> *group = &((*opt_proxy_groups_)[opt_proxy_groups_current_]);
  int select = random() % group->size();
  const string swap = (*group)[select];
  (*group)[select] = (*group)[0];
  (*group)[0] = swap;

  pthread_mutex_unlock(&lock_options_);
}


/**
 * Switches to the next load-balancing group of proxy servers.
 */
void SwitchProxyGroup() {
  pthread_mutex_lock(&lock_options_);

  if (!opt_proxy_groups_ || (opt_proxy_groups_->size() < 2)) {
    pthread_mutex_unlock(&lock_options_);
    return;
  }

  opt_proxy_groups_current_ = (opt_proxy_groups_current_ + 1) %
                              opt_proxy_groups_->size();
  opt_proxy_groups_current_burned_ = 0;

  pthread_mutex_unlock(&lock_options_);
}


/**
 * Called by curl for every HTTP header. Not called for file:// transfers.
 */
static size_t CallbackCurlHeader(void *ptr, size_t size, size_t nmemb,
                                 void *info_link)
{
  const size_t num_bytes = size*nmemb;
  const string header_line(static_cast<const char *>(ptr), num_bytes);
  JobInfo *info = static_cast<JobInfo *>(info_link);

  //LogCvmfs(kLogDownload, kLogDebug, "Header callback with line %s",
  //         header_line.c_str());

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
      info->error_code = (info->proxy == "") ? kFailHostConnection :
                                               kFailProxyConnection;
      // code dependent error heuristics?
      return 0;
    }
  }

  // Allocate memory for kDestinationMemory
  if ((info->destination == kDestinationMem) &&
      HasPrefix(header_line, "CONTENT-LENGTH:", true))
  {
    char *tmp = (char *)alloca(num_bytes+1);
    uint64_t length = 0;
    sscanf(header_line.c_str(), "%s %"PRIu64, tmp, &length);
    if (length > 0)
      info->destination_mem.data = static_cast<char *>(smalloc(length));
    else
      info->destination_mem.data = NULL;
    info->destination_mem.size = length;
  }

  return num_bytes;
}


/**
 * Called by curl for every received data chunk.
 */
static size_t CallbackCurlData(void *ptr, size_t size, size_t nmemb,
                               void *info_link)
{
  const size_t num_bytes = size*nmemb;
  JobInfo *info = static_cast<JobInfo *>(info_link);

  //LogCvmfs(kLogDownload, kLogDebug, "Data callback with %d bytes", num_bytes);

  if (num_bytes == 0)
    return 0;

  if (info->expected_hash)
    hash::Update((unsigned char *)ptr, num_bytes, info->hash_context);

  if (info->destination == kDestinationMem) {
    // Write to memory
    if (info->destination_mem.pos + num_bytes > info->destination_mem.size)
      return 0;
    memcpy(info->destination_mem.data + info->destination_mem.pos,
           ptr, num_bytes);
    info->destination_mem.pos += num_bytes;
  } else {
    // Write to file
    if (info->compressed) {
      int retval = zlib::DecompressZStream2File(&info->zstream,
                                                info->destination_file,
                                                ptr, num_bytes);
      if (retval < 0) {
        info->error_code = kFailBadData;
        return 0;
      }
    } else {
      if (fwrite(ptr, 1, num_bytes, info->destination_file) != num_bytes) {
        info->error_code = kFailLocalIO;
        return 0;
      }
    }
  }

  return num_bytes;
}


/**
 * Gets an idle CURL handle from the pool. Creates a new one and adds it to
 * the pool if necessary.
 */
static CURL *AcquireCurlHandle() {
  CURL *handle;

  if (pool_handles_idle_->empty()) {
    // Create a new handle
    handle = curl_easy_init();
    assert(handle != NULL);

    curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1);
    //curl_easy_setopt(curl_default, CURLOPT_FAILONERROR, 1);
    curl_easy_setopt(handle, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
    curl_easy_setopt(handle, CURLOPT_LOW_SPEED_LIMIT, 100);
    curl_easy_setopt(handle, CURLOPT_HEADERFUNCTION, CallbackCurlHeader);
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, CallbackCurlData);
  } else {
    handle = *(pool_handles_idle_->begin());
    pool_handles_idle_->erase(pool_handles_idle_->begin());
  }

  pool_handles_inuse_->insert(handle);

  return handle;
}


static void ReleaseCurlHandle(CURL *handle) {
  set<CURL *>::iterator elem = pool_handles_inuse_->find(handle);
  assert(elem != pool_handles_inuse_->end());

  if (pool_handles_idle_->size() > pool_max_handles_)
    curl_easy_cleanup(*elem);
  else
    pool_handles_idle_->insert(*elem);

  pool_handles_inuse_->erase(elem);
}


/**
 * Request parameters set the URL and other options such as timeout and
 * proxy.
 */
static void InitializeRequest(JobInfo *info, CURL *handle) {
  // Initialize internal download state
  info->curl_handle = handle;
  info->error_code = kFailOk;
  info->nocache = false;
  info->num_failed_proxies = 0;
  info->num_failed_hosts = 0;
  if (info->compressed) {
    zlib::DecompressInit(&(info->zstream));
  }
  if (info->expected_hash) {
    assert(info->hash_context.buffer != NULL);
    hash::Init(info->hash_context);
  }

  if ((info->destination == kDestinationMem) &&
      (HasPrefix(*(info->url), "file://", false)))
  {
    info->destination_mem.size = 64*1024;
    info->destination_mem.data = static_cast<char *>(smalloc(64*1024));
  }

  // Set curl parameters
  curl_easy_setopt(handle, CURLOPT_PRIVATE, static_cast<void *>(info));
  curl_easy_setopt(handle, CURLOPT_WRITEHEADER,
                   static_cast<void *>(info));
  curl_easy_setopt(handle, CURLOPT_WRITEDATA, static_cast<void *>(info));
  curl_easy_setopt(handle, CURLOPT_HTTPHEADER, http_headers_);
}


/**
 * Sets the URL specific options such as host to use and timeout.
 */
static void SetUrlOptions(JobInfo *info) {
  CURL *curl_handle = info->curl_handle;
  string url_prefix;

  pthread_mutex_lock(&lock_options_);
  if (!opt_proxy_groups_ ||
      ((*opt_proxy_groups_)[opt_proxy_groups_current_][0] == "DIRECT"))
  {
    info->proxy = "";
  } else {
    info->proxy = (*opt_proxy_groups_)[opt_proxy_groups_current_][0];
  }
  curl_easy_setopt(info->curl_handle, CURLOPT_PROXY, info->proxy.c_str());
  if (info->proxy != "") {
    curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, opt_timeout_proxy_);
    curl_easy_setopt(curl_handle, CURLOPT_LOW_SPEED_TIME, opt_timeout_proxy_);
  } else {
    curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, opt_timeout_direct_);
    curl_easy_setopt(curl_handle, CURLOPT_LOW_SPEED_TIME, opt_timeout_direct_);
  }
  if (opt_dns_server_)
    curl_easy_setopt(curl_handle, CURLOPT_DNS_SERVERS, opt_dns_server_);

  if (info->probe_hosts && opt_host_chain_)
    url_prefix = (*opt_host_chain_)[opt_host_chain_current_];
  pthread_mutex_unlock(&lock_options_);

  curl_easy_setopt(curl_handle, CURLOPT_URL,
                   EscapeUrl((url_prefix + *(info->url))).c_str());
  //LogCvmfs(kLogDownload, kLogDebug, "set url %s for info %p / curl handle %p",
  //         EscapeUrl((url_prefix + *(info->url))).c_str(), info, curl_handle);
}


/**
 * Adds transfer time and downloaded bytes to the global counters.
 */
static void UpdateStatistics(CURL *handle) {
  double val;

  if (curl_easy_getinfo(handle, CURLINFO_SIZE_DOWNLOAD, &val) == CURLE_OK)
    stat_transferred_bytes_ += val;
}


/**
 * Checks the result of a curl download and implements the failure logic, such
 * as changing the proxy server.  Takes care of cleanup.
 *
 * \return true if another download should be performed, false otherwise
 */
static bool VerifyAndFinalize(const int curl_error, JobInfo *info) {
  //LogCvmfs(kLogDownload, kLogDebug, "Verify Download (curl error %d)",
  //         curl_error);
  UpdateStatistics(info->curl_handle);

  // Verification and error classification
  switch (curl_error) {
    case CURLE_OK:
      // Verify content hash
      if (info->expected_hash) {
        hash::Any match_hash;
        hash::Final(info->hash_context, &match_hash);
        if (match_hash != *(info->expected_hash)) {
          // TODO(jakob): Logging
          info->error_code = kFailBadData;
          break;
        }
      }

      // Decompress memory in a single run
      if ((info->destination == kDestinationMem) && info->compressed) {
        void *buf;
        int64_t size;
        int retval = zlib::DecompressMem2Mem(info->destination_mem.data,
                                             info->destination_mem.size,
                                             &buf, &size);
        if (retval == 0) {
          free(info->destination_mem.data);
          info->destination_mem.data = static_cast<char *>(buf);
          info->destination_mem.size = size;
        } else {
          info->error_code = kFailBadData;
          break;
        }
      }

      info->error_code = kFailOk;
      break;
    case CURLE_UNSUPPORTED_PROTOCOL:
    case CURLE_URL_MALFORMAT:
      info->error_code = kFailBadUrl;
      break;
    case CURLE_COULDNT_RESOLVE_PROXY:
      info->error_code = kFailProxyConnection;
      break;
    case CURLE_COULDNT_RESOLVE_HOST:
      info->error_code = kFailHostConnection;
      break;
    case CURLE_COULDNT_CONNECT:
    case CURLE_OPERATION_TIMEDOUT:
      if (info->proxy != "")
        info->error_code = kFailProxyConnection;
      else
        info->error_code = kFailHostConnection;
      break;
    case CURLE_ABORTED_BY_CALLBACK:
    case CURLE_WRITE_ERROR:
      // Error set by callback
      break;
    default:
      LogCvmfs(kLogDownload, kLogSyslog, "unexpected curl error (%d) while "
               "trying to fetch %s", curl_error, info->url->c_str());
      info->error_code = kFailOther;
      break;
  }

  // Determination if download should be repeated
  bool try_again = false;
  if (info->error_code != kFailOk) {
    pthread_mutex_lock(&lock_options_);
    if ((info->error_code) == kFailBadData && !info->nocache)
      try_again = true;
    if ((info->error_code == kFailHostConnection) &&
        info->probe_hosts &&
        opt_host_chain_ && (info->num_failed_hosts < opt_host_chain_->size()))
    {
      try_again = true;
    }
    if ((info->error_code == kFailProxyConnection) &&
        (info->num_failed_proxies < opt_num_proxies_))
    {
      try_again = true;
    }
    pthread_mutex_unlock(&lock_options_);
  }

  if (try_again) {
    // Reset internal state and destination
    if ((info->destination == kDestinationMem) && info->destination_mem.data) {
      free(info->destination_mem.data);
      info->destination_mem.data = NULL;
      info->destination_mem.size = 0;
      info->destination_mem.pos = 0;
    }
    if ((info->destination == kDestinationFile) ||
        (info->destination == kDestinationPath))
    {
      if ((fflush(info->destination_file) != 0) ||
          (ftruncate(fileno(info->destination_file), 0) != 0))
      {
        info->error_code = kFailLocalIO;
        goto verify_and_finalize_stop;
      }
    }

    // Failure handling
    if (info->error_code == kFailBadData) {
      curl_easy_setopt(info->curl_handle, CURLOPT_HTTPHEADER,
                       http_headers_nocache_);
      info->nocache = true;
    } else if (info->error_code == kFailHostConnection) {
      SwitchHost(info);
      info->num_failed_hosts++;
      SetUrlOptions(info);
    } else if (info->error_code == kFailProxyConnection) {
      SwitchProxy(info);
      info->num_failed_proxies++;
      SetUrlOptions(info);
    }

    return true;  // try again
  }

 verify_and_finalize_stop:
  // Finalize, flush destination file
  if ((info->destination == kDestinationFile) &&
      fflush(info->destination_file) != 0)
  {
    info->error_code = kFailLocalIO;
  } else if (info->destination == kDestinationPath) {
    if (fclose(info->destination_file) != 0)
      info->error_code = kFailLocalIO;
    info->destination_file = NULL;
  }

  if (info->compressed)
    zlib::DecompressFini(&info->zstream);

  return false;  // stop transfer and return to Fetch()
}


static Failures PrepareDownloadDestination(JobInfo *info) {
  info->destination_mem.size = 0;
  info->destination_mem.pos = 0;
  info->destination_mem.data = NULL;

  if (info->destination == kDestinationFile)
    assert(info->destination_file != NULL);

  if (info->destination == kDestinationPath) {
    assert(info->destination_path != NULL);
    info->destination_file = fopen(info->destination_path->c_str(), "w");
    if (info->destination_file == NULL)
      return kFailLocalIO;
  }

  return kFailOk;
}


/**
 * Downloads data from an unsecure outside channel (currently HTTP or file).
 */
Failures Fetch(JobInfo *info) {
  assert(info != NULL);
  assert(info->url != NULL);

  Failures result;
  result = PrepareDownloadDestination(info);
  if (result != kFailOk)
    return result;

  if (info->expected_hash) {
    const hash::Algorithms algorithm = info->expected_hash->algorithm;
    info->hash_context.algorithm = algorithm;
    info->hash_context.size = hash::GetContextSize(algorithm);
    info->hash_context.buffer = alloca(info->hash_context.size);
  }

  if (atomic_xadd32(&multi_threaded_, 0) == 1) {
    if (info->wait_at[0] == -1) {
      MakePipe(info->wait_at);
    }

    //LogCvmfs(kLogDownload, kLogDebug, "send job to thread, pipe %d %d",
    //         info->wait_at[0], info->wait_at[1]);
    WritePipe(pipe_jobs_[1], &info, sizeof(info));
    ReadPipe(info->wait_at[0], &result, sizeof(result));
    //LogCvmfs(kLogDownload, kLogDebug, "got result %d", result);
  } else {
    CURL *handle = AcquireCurlHandle();
    InitializeRequest(info, handle);
    SetUrlOptions(info);
    //curl_easy_setopt(handle, CURLOPT_VERBOSE, 1);
    int retval;
    do {
      retval = curl_easy_perform(handle);
      double elapsed;
      if (curl_easy_getinfo(handle, CURLINFO_TOTAL_TIME, &elapsed) == CURLE_OK)
        stat_transfer_time_ += elapsed;
    } while (VerifyAndFinalize(retval, info));
    result = info->error_code;
    ReleaseCurlHandle(info->curl_handle);
  }

  if ((info->destination == kDestinationPath) && (result != kFailOk))
    unlink(info->destination_path->c_str());
  if ((info->destination_mem.data) && (result != kFailOk)) {
    free(info->destination_mem.data);
    info->destination_mem.data = NULL;
    info->destination_mem.size = 0;
  }

  if (result != kFailOk) {
    LogCvmfs(kLogDownload, kLogDebug, "download failed (error %d)", result);
  }

  return result;
}


/**
 * Called when new curl sockets arrive or existing curl sockets departure.
 */
static int CallbackCurlSocket(CURL *easy, curl_socket_t s, int action,
                              void *userp, void *socketp)
{
  //LogCvmfs(kLogDownload, kLogDebug, "CallbackCurlSocket called with easy "
  //         "handle %p, socket %d, action %d", easy, s, action);
  if (action == CURL_POLL_NONE)
    return 0;

  // Find s in watch_fds_
  unsigned index;
  for (index = 0; index < watch_fds_inuse_; ++index) {
    if (watch_fds_[index].fd == s)
      break;
  }
  // Or create newly
  if (index == watch_fds_inuse_) {
    // Extend array if necessary
    if (watch_fds_inuse_ == watch_fds_size_) {
      watch_fds_size_ *= 2;
      //LogCvmfs(kLogDownload, kLogDebug, "extending watch_fds_ (%d)", watch_fds_size_);
      watch_fds_ = static_cast<struct pollfd *>(
                   srealloc(watch_fds_, watch_fds_size_*sizeof(struct pollfd)));
      //LogCvmfs(kLogDownload, kLogDebug, "extending watch_fds_ done (%d)", watch_fds_size_);
    }
    watch_fds_[watch_fds_inuse_].fd = s;
    watch_fds_[watch_fds_inuse_].events = 0;
    watch_fds_[watch_fds_inuse_].revents = 0;
    watch_fds_inuse_++;
  }

  switch (action) {
    case CURL_POLL_IN:
      watch_fds_[index].events |= POLLIN | POLLPRI;
      break;
    case CURL_POLL_OUT:
      watch_fds_[index].events |= POLLOUT | POLLWRBAND;
      break;
    case CURL_POLL_INOUT:
      watch_fds_[index].events |= POLLIN | POLLPRI | POLLOUT | POLLWRBAND;
      break;
    case CURL_POLL_REMOVE:
      if (index < watch_fds_inuse_-1)
        watch_fds_[index] = watch_fds_[watch_fds_inuse_-1];
      watch_fds_inuse_--;
      // Shrink array if necessary
      if ((watch_fds_inuse_ > watch_fds_max_) &&
          (watch_fds_inuse_ < watch_fds_size_/2))
      {
        watch_fds_size_ /= 2;
        //LogCvmfs(kLogDownload, kLogDebug, "shrinking watch_fds_ (%d)", watch_fds_size_);
        watch_fds_ = static_cast<struct pollfd *>(
                   srealloc(watch_fds_, watch_fds_size_*sizeof(struct pollfd)));
        //LogCvmfs(kLogDownload, kLogDebug, "shrinking watch_fds_ done", watch_fds_size_);
      }
      break;
    default:
      break;
  }

  return 0;
}


/**
 * Worker thread event loop. Waits on new JobInfo structs on a pipe.
 */
static void *MainDownload(void *data __attribute__((unused))) {
  LogCvmfs(kLogDownload, kLogDebug, "download I/O thread started");

  watch_fds_ = static_cast<struct pollfd *>(smalloc(2 * sizeof(struct pollfd)));
  watch_fds_size_ = 2;
  watch_fds_[0].fd = pipe_terminate_[0];
  watch_fds_[0].events = POLLIN | POLLPRI;
  watch_fds_[0].revents = 0;
  watch_fds_[1].fd = pipe_jobs_[0];
  watch_fds_[1].events = POLLIN | POLLPRI;
  watch_fds_[1].revents = 0;
  watch_fds_inuse_ = 2;

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
      stat_transfer_time_ += DiffTimeSeconds(timeval_start, timeval_stop);
    }
    int retval = poll(watch_fds_, watch_fds_inuse_, timeout);
    if (errno == -1) {
      continue;
    }

    // Handle timeout
    if (retval == 0) {
      retval = curl_multi_socket_action(curl_multi_, CURL_SOCKET_TIMEOUT, 0,
                                        &still_running);
    }

    // Terminate I/O thread
    if (watch_fds_[0].revents)
      break;

    // New job arrives
    if (watch_fds_[1].revents) {
      watch_fds_[1].revents = 0;
      JobInfo *info;
      ReadPipe(pipe_jobs_[0], &info, sizeof(info));
      //LogCvmfs(kLogDownload, kLogDebug, "IO thread, got job: url %s, compressed %d, nocache %d, destination %d, file %p, expected hash %p, wait at %d", info->url->c_str(), info->compressed, info->nocache,
      //         info->destination, info->destination_file, info->expected_hash, info->wait_at[1]);

      if (!still_running)
        gettimeofday(&timeval_start, NULL);
      CURL *handle = AcquireCurlHandle();
      InitializeRequest(info, handle);
      SetUrlOptions(info);
      curl_multi_add_handle(curl_multi_, handle);
      retval = curl_multi_socket_action(curl_multi_, CURL_SOCKET_TIMEOUT, 0,
                                        &still_running);
      //LogCvmfs(kLogDownload, kLogDebug, "socket action returned with %d, still_running %d", retval, still_running);
    }

    // Activity on curl sockets
    for (unsigned i = 2; i < watch_fds_inuse_; ++i) {
      if (watch_fds_[i].revents) {
        int ev_bitmask = 0;
        if (watch_fds_[i].revents & (POLLIN | POLLPRI))
          ev_bitmask |= CURL_CSELECT_IN;
        if (watch_fds_[i].revents & (POLLOUT | POLLWRBAND))
          ev_bitmask |= CURL_CSELECT_IN;
        if (watch_fds_[i].revents & (POLLERR | POLLHUP | POLLNVAL))
          ev_bitmask |= CURL_CSELECT_ERR;
        watch_fds_[i].revents = 0;

        retval = curl_multi_socket_action(curl_multi_, watch_fds_[i].fd,
                                          ev_bitmask, &still_running);
        //LogCvmfs(kLogDownload, kLogDebug, "socket action on socket %d, returned with %d, still_running %d", watch_fds_[i].fd, retval, still_running);
      }
    }

    // Check if transfers are completed
    CURLMsg *curl_msg;
    int msgs_in_queue;
    while ((curl_msg = curl_multi_info_read(curl_multi_, &msgs_in_queue))) {
      if (curl_msg->msg == CURLMSG_DONE) {
        JobInfo *info;
        CURL *easy_handle = curl_msg->easy_handle;
        int curl_error = curl_msg->data.result;
        curl_easy_getinfo(easy_handle, CURLINFO_PRIVATE, &info);
        //LogCvmfs(kLogDownload, kLogDebug, "Done message for %s", info->url->c_str());

        curl_multi_remove_handle(curl_multi_, easy_handle);
        if (VerifyAndFinalize(curl_error, info)) {
          curl_multi_add_handle(curl_multi_, easy_handle);
          retval = curl_multi_socket_action(curl_multi_, CURL_SOCKET_TIMEOUT, 0,
                                            &still_running);
        } else {
          // Return easy handle into pool and write result back
          ReleaseCurlHandle(easy_handle);

          WritePipe(info->wait_at[1], &info->error_code,
                    sizeof(info->error_code));
        }
      }
    }
  }

  for (set<CURL *>::iterator i = pool_handles_inuse_->begin(),
       iEnd = pool_handles_inuse_->end(); i != iEnd; ++i)
  {
    curl_multi_remove_handle(curl_multi_, *i);
    curl_multi_cleanup(*i);
  }
  pool_handles_inuse_->clear();

  LogCvmfs(kLogDownload, kLogDebug, "download I/O thread terminated");
  return NULL;
}


void Init(const unsigned max_pool_handles) {
  atomic_init32(&multi_threaded_);
  int retval = curl_global_init(CURL_GLOBAL_ALL);
  assert(retval == CURLE_OK);
  pool_handles_idle_ = new set<CURL *>;
  pool_handles_inuse_ = new set<CURL *>;
  pool_max_handles_ = max_pool_handles;
  watch_fds_max_ = 4*pool_max_handles_;

  opt_timeout_proxy_ = 5;
  opt_timeout_direct_ = 10;
  opt_proxy_groups_current_ = 0;
  opt_proxy_groups_current_burned_ = 0;
  opt_num_proxies_ = 0;
  opt_host_chain_current_ = 0;

  stat_transferred_bytes_ = 0.0;
  stat_transfer_time_ = 0.0;

  // Prepare HTTP headers
  string custom_header;
  if (getenv("CERNVM_UUID") != NULL) {
    custom_header = "X-CVMFS2 " + string(VERSION) + " " +
    string(getenv("CERNVM_UUID"));
  } else {
    custom_header = "X-CVMFS2 " + string(VERSION) + " anonymous";
  }
  http_headers_ = curl_slist_append(http_headers_, "Connection: Keep-Alive");
  http_headers_ = curl_slist_append(http_headers_, "Pragma:");
  http_headers_ = curl_slist_append(http_headers_, custom_header.c_str());
  http_headers_nocache_ = curl_slist_append(http_headers_nocache_,
                                            "Pragma: no-cache");
  http_headers_nocache_ = curl_slist_append(http_headers_nocache_,
                                            "Cache-Control: no-cache");
  http_headers_nocache_ = curl_slist_append(http_headers_nocache_,
                                            custom_header.c_str());

  curl_multi_ = curl_multi_init();
  assert(curl_multi_ != NULL);
  curl_multi_setopt(curl_multi_, CURLMOPT_SOCKETFUNCTION, CallbackCurlSocket);
  curl_multi_setopt(curl_multi_, CURLMOPT_MAXCONNECTS, watch_fds_max_);
  //curl_multi_setopt(curl_multi_, CURLMOPT_PIPELINING, 1);

  // Initialize random number engine with system time
  struct timeval tv_now;
  gettimeofday(&tv_now, NULL);
  srandom(tv_now.tv_usec);
}


void Fini() {
  if (atomic_xadd32(&multi_threaded_, 0) == 1) {
    // Shutdown I/O thread
    char buf = 'T';
    WritePipe(pipe_terminate_[1], &buf, 1);
    pthread_join(thread_download_, NULL);
    // All handles are removed from the multi stack
    close(pipe_terminate_[1]);
    close(pipe_terminate_[0]);
  }

  for (set<CURL *>::iterator i = pool_handles_idle_->begin(),
       iEnd = pool_handles_idle_->end(); i != iEnd; ++i)
  {
    curl_easy_cleanup(*i);
  }
  delete pool_handles_idle_;
  delete pool_handles_inuse_;
  curl_slist_free_all(http_headers_);
  curl_slist_free_all(http_headers_nocache_);
  curl_multi_cleanup(curl_multi_);
  pool_handles_idle_ = NULL;
  pool_handles_inuse_ = NULL;
  http_headers_ = NULL;
  http_headers_nocache_ = NULL;
  curl_multi_ = NULL;

  delete opt_host_chain_;
  delete opt_host_chain_rtt_;
  delete opt_proxy_groups_;
  opt_host_chain_ = NULL;
  opt_host_chain_rtt_ = NULL;
  opt_proxy_groups_ = NULL;

  curl_global_cleanup();
}


/**
 * Spawns the I/O worker thread and switches the module in multi-threaded mode.
 * No way back except Fini(); Init();
 */
void Spawn() {
  MakePipe(pipe_terminate_);
  MakePipe(pipe_jobs_);

  int retval = pthread_create(&thread_download_, NULL, MainDownload, NULL);
  assert(retval == 0);

  atomic_inc32(&multi_threaded_);
}


/**
 * Sets a DNS server.  Only for testing as it cannot be reverted to the system
 * default.
 */
void SetDnsServer(const string &address) {
  pthread_mutex_lock(&lock_options_);
  if (opt_dns_server_)
    free(opt_dns_server_);
  if (address != "") {
    opt_dns_server_ = strdup(address.c_str());
    assert(opt_dns_server_);
  }
  pthread_mutex_unlock(&lock_options_);
}


/**
 * Sets two timeout values for proxied and for direct conections, respectively.
 * The timeout counts for all sorts of connection phases,
 * DNS, HTTP connect, etc.
 */
void SetTimeout(const unsigned seconds_proxy, const unsigned seconds_direct) {
  pthread_mutex_lock(&lock_options_);
  opt_timeout_proxy_ = seconds_proxy;
  opt_timeout_direct_ = seconds_direct;
  pthread_mutex_unlock(&lock_options_);
}


/**
 * Receives the currently active timeout values.
 */
void GetTimeout(unsigned *seconds_proxy, unsigned *seconds_direct) {
  pthread_mutex_lock(&lock_options_);
  *seconds_proxy = opt_timeout_proxy_;
  *seconds_direct = opt_timeout_direct_;
  pthread_mutex_unlock(&lock_options_);
}


/**
 * Overall number of bytes received through downloads.
 */
uint64_t GetTransferredBytes() {
  return uint64_t(stat_transferred_bytes_);
}


/**
 * Overall time spend in receiving data.
 */
uint64_t GetTransferTime() {
  LogCvmfs(kLogDownload, kLogDebug, "Transfer time %lf", stat_transfer_time_);
  return uint64_t(stat_transfer_time_);
}


/**
 * Parses a list of ';'-separated hosts for the host chain.  The empty string
 * removes the host list.
 */
void SetHostChain(const string &host_list) {
  pthread_mutex_lock(&lock_options_);
  delete opt_host_chain_;
  delete opt_host_chain_rtt_;
  opt_host_chain_current_ = 0;

  if (host_list == "") {
    opt_host_chain_ = NULL;
    opt_host_chain_rtt_ = NULL;
    pthread_mutex_unlock(&lock_options_);
    return;
  }

  opt_host_chain_ = new vector<string>(SplitString(host_list, ';'));
  opt_host_chain_rtt_ = new vector<int>();
  for (unsigned i = 0, s = opt_host_chain_->size(); i < s; ++i)
    opt_host_chain_rtt_->push_back(-1);
  pthread_mutex_unlock(&lock_options_);
}


/**
 * Retrieves the currently set chain of hosts, their round trip times, and the
 * currently used host.
 */
void GetHostInfo(std::vector<std::string> *host_chain,
                 std::vector<int> *rtt, unsigned *current_host)
{
  pthread_mutex_lock(&lock_options_);
  if (opt_host_chain_) {
    *current_host = opt_host_chain_current_;
    *host_chain = *opt_host_chain_;
    *rtt = *opt_host_chain_rtt_;
  }
  pthread_mutex_unlock(&lock_options_);
}


/**
 * Parses a list of ';'- and '|'-separated proxy servers for the proxy groups.
 * The empty string removes the proxy chain.
 */
void SetProxyChain(const std::string &proxy_list) {
  pthread_mutex_lock(&lock_options_);

  delete opt_proxy_groups_;
  if (proxy_list == "") {
    opt_proxy_groups_ = NULL;
    opt_proxy_groups_current_ = 0;
    opt_proxy_groups_current_burned_ = 0;
    opt_num_proxies_ = 0;
    pthread_mutex_unlock(&lock_options_);
    return;
  }

  vector<string> proxy_groups = SplitString(proxy_list, ';');
  opt_proxy_groups_ = new vector< vector<string> >();
  opt_num_proxies_ = 0;
  for (unsigned i = 0; i < proxy_groups.size(); ++i) {
    opt_proxy_groups_->push_back(SplitString(proxy_groups[i], '|'));
    opt_num_proxies_ += (*opt_proxy_groups_)[i].size();
  }
  opt_proxy_groups_current_ = 0;
  opt_proxy_groups_current_burned_ = 0;

  /* Select random start proxy from the first group */
  if ((*opt_proxy_groups_)[0].size() > 1) {
    int random_index = random() % (*opt_proxy_groups_)[0].size();
    string tmp = (*opt_proxy_groups_)[0][0];
    (*opt_proxy_groups_)[0][0] = (*opt_proxy_groups_)[0][random_index];
    (*opt_proxy_groups_)[0][random_index] = tmp;
  }
  pthread_mutex_unlock(&lock_options_);
}


/**
 * Retrieves the proxy chain and the currently active load-balancing group.
 */
void GetProxyInfo(vector< vector<string> > *proxy_chain,
                  unsigned *current_group)
{
  pthread_mutex_lock(&lock_options_);

  if (!opt_proxy_groups_) {
    pthread_mutex_unlock(&lock_options_);
    proxy_chain = NULL;
    current_group = NULL;
    return;
  }

  *proxy_chain = *opt_proxy_groups_;
  *current_group = opt_proxy_groups_current_;

  pthread_mutex_unlock(&lock_options_);
}


/**
 * Orders the hostlist according to RTT of downloading .cvmfschecksum.
 * Sets the current host to the best-responsive host.
 * If you change the host list in between by SetHostChain(), it will be
 * overwritten by this function.
 */
void ProbeHosts() {
  vector<string> host_chain;
  vector<int> host_rtt;
  unsigned current_host;

  GetHostInfo(&host_chain, &host_rtt, &current_host);

  // Stopwatch, two times to fill caches first
  unsigned i, retries;
  string url;
  JobInfo info(&url, false, false, NULL);
  for (retries = 0; retries < 2; ++retries) {
    for (i = 0; i < host_chain.size(); ++i) {
      url = host_chain[i] + "/.cvmfspublished";

      struct timeval tv_start, tv_end;
      gettimeofday(&tv_start, NULL);
      Failures result = Fetch(&info);
      gettimeofday(&tv_end, NULL);
      if (info.destination_mem.data)
        free(info.destination_mem.data);
      if (result == kFailOk) {
        host_rtt[i] = int(DiffTimeSeconds(tv_start, tv_end));
        LogCvmfs(kLogDownload, kLogDebug, "probing host %s had %dms rtt",
                 url.c_str(), host_rtt[i]);
      } else {
        LogCvmfs(kLogDownload, kLogDebug, "error while probing host %s: %d",
                 url.c_str(), result);
        host_rtt[i] = INT_MAX;
      }
    }
  }

  // Sort entries, insertion sort on both, rtt and hosts
  for (i = 1; i < host_chain.size(); ++i) {
    int val_rtt = host_rtt[i];
    string val_host = host_chain[i];
    int pos;
    for (pos = i-1; (pos >= 0) && (host_rtt[pos] > val_rtt); --pos) {
      host_rtt[pos+1] = host_rtt[pos];
      host_chain[pos+1] = host_chain[pos];
    }
    host_rtt[pos+1] = val_rtt;
    host_chain[pos+1] = val_host;
  }
  for (i = 0; i < host_chain.size(); ++i) {
    if (host_rtt[i] == INT_MAX) host_rtt[i] = -2;
  }

  pthread_mutex_lock(&lock_options_);
  delete opt_host_chain_;
  delete opt_host_chain_rtt_;
  opt_host_chain_ = new vector<string>(host_chain);
  opt_host_chain_rtt_ = new vector<int>(host_rtt);
  opt_host_chain_current_ = 0;
  pthread_mutex_unlock(&lock_options_);
}


void RestartNetwork() {
  // TODO: transfer special job
}

}  // namespace download
