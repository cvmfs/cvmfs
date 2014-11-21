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

#include <alloca.h>
#include <errno.h>
#include <inttypes.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <set>

#include "atomic.h"
#include "compression.h"
#include "duplex_curl.h"
#include "hash.h"
#include "logging.h"
#include "prng.h"
#include "sanitizer.h"
#include "smalloc.h"
#include "util.h"

using namespace std;  // NOLINT

namespace download {


static inline bool EscapeUrlChar(char input, char output[3]) {
  if (((input >= '0') && (input <= '9')) ||
      ((input >= 'A') && (input <= 'Z')) ||
      ((input >= 'a') && (input <= 'z')) ||
      (input == '/') || (input == ':') || (input == '.') ||
      (input == '+') || (input == '-') ||
      (input == '_') || (input == '~') ||
      (input == '[') || (input == ']'))
  {
    output[0] = input;
    return false;
  }

  output[0] = '%';
  output[1] = (input / 16) + ((input / 16 <= 9) ? '0' : 'A'-10);
  output[2] = (input % 16) + ((input % 16 <= 9) ? '0' : 'A'-10);
  return true;
}


/**
 * Escape special chars from the URL, except for ':' and '/',
 * which should keep their meaning.
 */
static string EscapeUrl(const string &url) {
  string escaped;
  escaped.reserve(url.length());

  char escaped_char[3];
  for (unsigned i = 0, s = url.length(); i < s; ++i) {
    if (EscapeUrlChar(url[i], escaped_char))
      escaped.append(escaped_char, 3);
    else
      escaped.push_back(escaped_char[0]);
  }
  LogCvmfs(kLogDownload, kLogDebug, "escaped %s to %s",
           url.c_str(), escaped.c_str());

  return escaped;
}


/**
 * escaped array needs to be sufficiently large.  It's size is calculated by
 * passing NULL to EscapeHeader.
 */
static unsigned EscapeHeader(const string &header,
                             char *escaped_buf,
                             size_t buf_size)
{
  unsigned esc_pos = 0;
  char escaped_char[3];
  for (unsigned i = 0, s = header.size(); i < s; ++i) {
    if (EscapeUrlChar(header[i], escaped_char)) {
      for (unsigned j = 0; j < 3; ++j) {
        if (escaped_buf) {
          if (esc_pos >= buf_size)
            return esc_pos;
          escaped_buf[esc_pos] = escaped_char[j];
        }
        esc_pos++;
      }
    } else {
      if (escaped_buf) {
        if (esc_pos >= buf_size)
          return esc_pos;
        escaped_buf[esc_pos] = escaped_char[0];
      }
      esc_pos++;
    }
  }

  return esc_pos;
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
 * Called by curl for every HTTP header. Not called for file:// transfers.
 */
static size_t CallbackCurlHeader(void *ptr, size_t size, size_t nmemb,
                                 void *info_link)
{
  const size_t num_bytes = size*nmemb;
  const string header_line(static_cast<const char *>(ptr), num_bytes);
  JobInfo *info = static_cast<JobInfo *>(info_link);

  //LogCvmfs(kLogDownload, kLogDebug, "REMOVE-ME: Header callback with line %s",
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
      if (header_line[i] == '5') {
        // 5XX returned by host
        info->error_code = kFailHostHttp;
      } else if ((header_line.length() > i+2) && (header_line[i] == '4') &&
                 (header_line[i+1] == '0') && (header_line[i+2] == '4'))
      {
        // 404: the stratum 1 does not have the newest files
        info->error_code = kFailHostHttp;
      } else {
        info->error_code = (info->proxy == "") ? kFailHostHttp :
                                                 kFailProxyHttp;
      }
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
    if (length > 0) {
      if (length > DownloadManager::kMaxMemSize) {
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslogErr,
                 "resource %s too large to store in memory (%"PRIu64")",
                 info->url->c_str(), length);
        info->error_code = kFailTooBig;
        return 0;
      }
      info->destination_mem.data = static_cast<char *>(smalloc(length));
    } else {
      // Empty resource
      info->destination_mem.data = NULL;
    }
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
    shash::Update((unsigned char *)ptr, num_bytes, info->hash_context);

  if (info->destination == kDestinationMem) {
    // Write to memory
    if (info->destination_mem.pos + num_bytes > info->destination_mem.size) {
      info->error_code = kFailBadData;
      return 0;
    }
    memcpy(info->destination_mem.data + info->destination_mem.pos,
           ptr, num_bytes);
    info->destination_mem.pos += num_bytes;
  } else {
    // Write to file
    if (info->compressed) {
      //LogCvmfs(kLogDownload, kLogDebug, "REMOVE-ME: writing %d bytes for %s",
      //         num_bytes, info->url->c_str());
      zlib::StreamStates retval =
        zlib::DecompressZStream2File(&info->zstream,
                                     info->destination_file,
                                     ptr, num_bytes);
      if (retval == zlib::kStreamDataError) {
        LogCvmfs(kLogDownload, kLogDebug, "failed to decompress %s",
                 info->url->c_str());
        info->error_code = kFailBadData;
        return 0;
      } else if (retval == zlib::kStreamIOError) {
        LogCvmfs(kLogDownload, kLogSyslogErr, "decompressing %s, local IO error",
                 info->url->c_str());
        info->error_code = kFailLocalIO;
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
 * Called when new curl sockets arrive or existing curl sockets depart.
 */
int DownloadManager::CallbackCurlSocket(CURL *easy,
                                        curl_socket_t s,
                                        int action,
                                        void *userp,
                                        void *socketp)
{
  //LogCvmfs(kLogDownload, kLogDebug, "CallbackCurlSocket called with easy "
  //         "handle %p, socket %d, action %d", easy, s, action);
  DownloadManager *download_mgr = static_cast<DownloadManager *>(userp);
  if (action == CURL_POLL_NONE)
    return 0;

  // Find s in watch_fds_
  unsigned index;
  for (index = 0; index < download_mgr->watch_fds_inuse_; ++index) {
    if (download_mgr->watch_fds_[index].fd == s)
      break;
  }
  // Or create newly
  if (index == download_mgr->watch_fds_inuse_) {
    // Extend array if necessary
    if (download_mgr->watch_fds_inuse_ == download_mgr->watch_fds_size_)
    {
      download_mgr->watch_fds_size_ *= 2;
      download_mgr->watch_fds_ = static_cast<struct pollfd *>(
        srealloc(download_mgr->watch_fds_,
                 download_mgr->watch_fds_size_*sizeof(struct pollfd)));
    }
    download_mgr->watch_fds_[download_mgr->watch_fds_inuse_].fd = s;
    download_mgr->watch_fds_[download_mgr->watch_fds_inuse_].events = 0;
    download_mgr->watch_fds_[download_mgr->watch_fds_inuse_].revents = 0;
    download_mgr->watch_fds_inuse_++;
  }

  switch (action) {
    case CURL_POLL_IN:
      download_mgr->watch_fds_[index].events |= POLLIN | POLLPRI;
      break;
    case CURL_POLL_OUT:
      download_mgr->watch_fds_[index].events |= POLLOUT | POLLWRBAND;
      break;
    case CURL_POLL_INOUT:
      download_mgr->watch_fds_[index].events |=
        POLLIN | POLLPRI | POLLOUT | POLLWRBAND;
      break;
    case CURL_POLL_REMOVE:
      if (index < download_mgr->watch_fds_inuse_-1)
        download_mgr->watch_fds_[index] =
          download_mgr->watch_fds_[download_mgr->watch_fds_inuse_-1];
      download_mgr->watch_fds_inuse_--;
      // Shrink array if necessary
      if ((download_mgr->watch_fds_inuse_ > download_mgr->watch_fds_max_) &&
          (download_mgr->watch_fds_inuse_ < download_mgr->watch_fds_size_/2))
      {
        download_mgr->watch_fds_size_ /= 2;
        //LogCvmfs(kLogDownload, kLogDebug, "shrinking watch_fds_ (%d)",
        //         watch_fds_size_);
        download_mgr->watch_fds_ = static_cast<struct pollfd *>(
          srealloc(download_mgr->watch_fds_,
                   download_mgr->watch_fds_size_*sizeof(struct pollfd)));
        //LogCvmfs(kLogDownload, kLogDebug, "shrinking watch_fds_ done",
        //         watch_fds_size_);
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
void *DownloadManager::MainDownload(void *data) {
  LogCvmfs(kLogDownload, kLogDebug, "download I/O thread started");
  DownloadManager *download_mgr = static_cast<DownloadManager *>(data);

  download_mgr->watch_fds_ =
    static_cast<struct pollfd *>(smalloc(2 * sizeof(struct pollfd)));
  download_mgr->watch_fds_size_ = 2;
  download_mgr->watch_fds_[0].fd = download_mgr->pipe_terminate_[0];
  download_mgr->watch_fds_[0].events = POLLIN | POLLPRI;
  download_mgr->watch_fds_[0].revents = 0;
  download_mgr->watch_fds_[1].fd = download_mgr->pipe_jobs_[0];
  download_mgr->watch_fds_[1].events = POLLIN | POLLPRI;
  download_mgr->watch_fds_[1].revents = 0;
  download_mgr->watch_fds_inuse_ = 2;

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
      download_mgr->statistics_->transfer_time +=
        DiffTimeSeconds(timeval_start, timeval_stop);
    }
    int retval = poll(download_mgr->watch_fds_, download_mgr->watch_fds_inuse_,
                      timeout);
    if (retval < 0) {
      continue;
    }

    // Handle timeout
    if (retval == 0) {
      retval = curl_multi_socket_action(download_mgr->curl_multi_,
                                        CURL_SOCKET_TIMEOUT,
                                        0,
                                        &still_running);
    }

    // Terminate I/O thread
    if (download_mgr->watch_fds_[0].revents)
      break;

    // New job arrives
    if (download_mgr->watch_fds_[1].revents) {
      download_mgr->watch_fds_[1].revents = 0;
      JobInfo *info;
      ReadPipe(download_mgr->pipe_jobs_[0], &info, sizeof(info));
      //LogCvmfs(kLogDownload, kLogDebug, "IO thread, got job: url %s, compressed %d, nocache %d, destination %d, file %p, expected hash %p, wait at %d", info->url->c_str(), info->compressed, info->nocache,
      //         info->destination, info->destination_file, info->expected_hash, info->wait_at[1]);

      if (!still_running)
        gettimeofday(&timeval_start, NULL);
      CURL *handle = download_mgr->AcquireCurlHandle();
      download_mgr->InitializeRequest(info, handle);
      download_mgr->SetUrlOptions(info);
      curl_multi_add_handle(download_mgr->curl_multi_, handle);
      retval = curl_multi_socket_action(download_mgr->curl_multi_,
                                        CURL_SOCKET_TIMEOUT,
                                        0,
                                        &still_running);
      //LogCvmfs(kLogDownload, kLogDebug, "socket action returned with %d, still_running %d", retval, still_running);
    }

    // Activity on curl sockets
    for (unsigned i = 2; i < download_mgr->watch_fds_inuse_; ++i) {
      if (download_mgr->watch_fds_[i].revents) {
        int ev_bitmask = 0;
        if (download_mgr->watch_fds_[i].revents & (POLLIN | POLLPRI))
          ev_bitmask |= CURL_CSELECT_IN;
        if (download_mgr->watch_fds_[i].revents & (POLLOUT | POLLWRBAND))
          ev_bitmask |= CURL_CSELECT_IN;
        if (download_mgr->watch_fds_[i].revents & (POLLERR | POLLHUP | POLLNVAL))
          ev_bitmask |= CURL_CSELECT_ERR;
        download_mgr->watch_fds_[i].revents = 0;

        retval = curl_multi_socket_action(download_mgr->curl_multi_,
                                          download_mgr->watch_fds_[i].fd,
                                          ev_bitmask,
                                          &still_running);
        //LogCvmfs(kLogDownload, kLogDebug, "socket action on socket %d, returned with %d, still_running %d", watch_fds_[i].fd, retval, still_running);
      }
    }

    // Check if transfers are completed
    CURLMsg *curl_msg;
    int msgs_in_queue;
    while ((curl_msg = curl_multi_info_read(download_mgr->curl_multi_,
                                            &msgs_in_queue)))
    {
      if (curl_msg->msg == CURLMSG_DONE) {
        download_mgr->statistics_->num_requests++;
        JobInfo *info;
        CURL *easy_handle = curl_msg->easy_handle;
        int curl_error = curl_msg->data.result;
        curl_easy_getinfo(easy_handle, CURLINFO_PRIVATE, &info);
        //LogCvmfs(kLogDownload, kLogDebug, "Done message for %s", info->url->c_str());

        curl_multi_remove_handle(download_mgr->curl_multi_, easy_handle);
        if (download_mgr->VerifyAndFinalize(curl_error, info)) {
          curl_multi_add_handle(download_mgr->curl_multi_, easy_handle);
          retval = curl_multi_socket_action(download_mgr->curl_multi_,
                                            CURL_SOCKET_TIMEOUT,
                                            0,
                                            &still_running);
        } else {
          // Return easy handle into pool and write result back
          download_mgr->ReleaseCurlHandle(easy_handle);

          WritePipe(info->wait_at[1], &info->error_code,
                    sizeof(info->error_code));
        }
      }
    }
  }

  for (set<CURL *>::iterator i = download_mgr->pool_handles_inuse_->begin(),
       iEnd = download_mgr->pool_handles_inuse_->end(); i != iEnd; ++i)
  {
    curl_multi_remove_handle(download_mgr->curl_multi_, *i);
    curl_easy_cleanup(*i);
  }
  download_mgr->pool_handles_inuse_->clear();
  free(download_mgr->watch_fds_);

  LogCvmfs(kLogDownload, kLogDebug, "download I/O thread terminated");
  return NULL;
}


//------------------------------------------------------------------------------


HeaderLists::~HeaderLists() {
  for (unsigned i = 0; i < blocks_.size(); ++i) {
    delete[] blocks_[i];
  }
  blocks_.clear();
}


curl_slist *HeaderLists::GetList(const char *header) {
  return Get(header);
}


curl_slist *HeaderLists::DuplicateList(curl_slist *slist) {
  assert(slist);
  curl_slist *copy = GetList(slist->data);
  copy->next = slist->next;
  curl_slist *prev = copy;
  slist = slist->next;
  while (slist) {
    curl_slist *new_link = Get(slist->data);
    new_link->next = slist->next;
    prev->next = new_link;
    prev = new_link;
    slist = slist->next;
  }
  return copy;
}


void HeaderLists::AppendHeader(curl_slist *slist, const char *header) {
  assert(slist);
  curl_slist *new_link = Get(header);
  new_link->next = NULL;

  while (slist->next)
    slist = slist->next;
  slist->next = new_link;
}


void HeaderLists::PutList(curl_slist *slist) {
  while (slist) {
    curl_slist *next = slist->next;
    Put(slist);
    slist = next;
  }
}


string HeaderLists::Print(curl_slist *slist) {
  string verbose;
  while (slist) {
    verbose += string(slist->data) + "\n";
    slist = slist->next;
  }
  return verbose;
}


curl_slist *HeaderLists::Get(const char *header) {
  for (unsigned i = 0; i < blocks_.size(); ++i) {
    for (unsigned j = 0; j < kBlockSize; ++j) {
      if (!IsUsed(&(blocks_[i][j]))) {
        blocks_[i][j].data = const_cast<char *>(header);
        return &(blocks_[i][j]);
      }
    }
  }

  // All used, new block
  AddBlock();
  blocks_[blocks_.size()-1][0].data = const_cast<char *>(header);
  return &(blocks_[blocks_.size()-1][0]);
}


void HeaderLists::Put(curl_slist *slist) {
  slist->data = NULL;
  slist->next = NULL;
}


void HeaderLists::AddBlock(){
  curl_slist *new_block = new curl_slist[kBlockSize];
  for (unsigned i = 0; i < kBlockSize; ++i) {
    Put(&new_block[i]);
  }
  blocks_.push_back(new_block);
}


//------------------------------------------------------------------------------


string DownloadManager::ProxyInfo::Print() {
  if (url == "DIRECT")
    return url;
  
  string result = url;
  int remaining =
    static_cast<int>(host.deadline()) - static_cast<int>(time(NULL));
  string expinfo = (remaining >= 0) ? "+" : "";
  if (abs(remaining) >= 3600) {
    expinfo += StringifyInt(remaining/3600) + "h";
  } else if (abs(remaining) >= 60) {
    expinfo += StringifyInt(remaining/60) + "m";
  } else {
    expinfo += StringifyInt(remaining) + "s";
  }
  if (host.status() == dns::kFailOk) {
    result += " (" + host.name() + ", " + expinfo + ")";
  } else {
    result += " (:unresolved:, " + expinfo + ")";
  }
  return result;
}


/**
 * Gets an idle CURL handle from the pool. Creates a new one and adds it to
 * the pool if necessary.
 */
CURL *DownloadManager::AcquireCurlHandle() {
  CURL *handle;

  if (pool_handles_idle_->empty()) {
    // Create a new handle
    handle = curl_easy_init();
    assert(handle != NULL);

    curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1);
    //curl_easy_setopt(curl_default, CURLOPT_FAILONERROR, 1);
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


void DownloadManager::ReleaseCurlHandle(CURL *handle) {
  set<CURL *>::iterator elem = pool_handles_inuse_->find(handle);
  assert(elem != pool_handles_inuse_->end());

  if (pool_handles_idle_->size() > pool_max_handles_)
    curl_easy_cleanup(*elem);
  else
    pool_handles_idle_->insert(*elem);

  pool_handles_inuse_->erase(elem);
}


/**
 * HTTP request options: set the URL and other options such as timeout and
 * proxy.
 */
void DownloadManager::InitializeRequest(JobInfo *info, CURL *handle) {
  // Initialize internal download state
  info->curl_handle = handle;
  info->error_code = kFailOk;
  info->nocache = false;
  info->num_used_proxies = 1;
  info->num_used_hosts = 1;
  info->num_retries = 0;
  info->backoff_ms = 0;
  info->headers = header_lists_->DuplicateList(default_headers_);
  if (info->info_header) {
    header_lists_->AppendHeader(info->headers, info->info_header);
  }
  if (info->compressed) {
    zlib::DecompressInit(&(info->zstream));
  }
  if (info->expected_hash) {
    assert(info->hash_context.buffer != NULL);
    shash::Init(info->hash_context);
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
  curl_easy_setopt(handle, CURLOPT_HTTPHEADER, info->headers);
  if (info->head_request)
    curl_easy_setopt(handle, CURLOPT_NOBODY, 1);
  else
    curl_easy_setopt(handle, CURLOPT_HTTPGET, 1);
  if (opt_ipv4_only_)
    curl_easy_setopt(handle, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
}


/**
 * Sets the URL specific options such as host to use and timeout.  It might also
 * set an error code, in which case the further processing should react on.
 */
void DownloadManager::SetUrlOptions(JobInfo *info) {
  CURL *curl_handle = info->curl_handle;
  string url_prefix;

  pthread_mutex_lock(lock_options_);
  // Check if proxy group needs to be reset from backup to primary
  if (opt_timestamp_backup_proxies_ > 0) {
    const time_t now = time(NULL);
    if (static_cast<int64_t>(now) >
        static_cast<int64_t>(opt_timestamp_backup_proxies_ +
                             opt_proxy_groups_reset_after_))
    {
      string old_proxy;
      if (opt_proxy_groups_)
        old_proxy = (*opt_proxy_groups_)[opt_proxy_groups_current_][0].url;

      opt_proxy_groups_current_ = 0;
      RebalanceProxiesUnlocked();
      opt_timestamp_backup_proxies_ = 0;

      if (opt_proxy_groups_) {
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
                 "switching proxy from %s to %s (reset proxy group)",
                 old_proxy.c_str(), (*opt_proxy_groups_)[0][0].url.c_str());
      }
    }
  }
  // Check if load-balanced proxies within the group need to be reset
  if (opt_timestamp_failover_proxies_ > 0) {
    const time_t now = time(NULL);
    if (static_cast<int64_t>(now) >
        static_cast<int64_t>(opt_timestamp_failover_proxies_ +
                             opt_proxy_groups_reset_after_))
    {
      string old_proxy;
      if (opt_proxy_groups_)
        old_proxy = (*opt_proxy_groups_)[opt_proxy_groups_current_][0].url;
      RebalanceProxiesUnlocked();
      if (opt_proxy_groups_ && (old_proxy != (*opt_proxy_groups_)[0][0].url)) {
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
                 "switching proxy from %s to %s (reset load-balanced proxies)",
                 old_proxy.c_str(), (*opt_proxy_groups_)[0][0].url.c_str());
      }
    }
  }
  // Check if host needs to be reset
  if (opt_timestamp_backup_host_ > 0) {
    const time_t now = time(NULL);
    if (static_cast<int64_t>(now) >
        static_cast<int64_t>(opt_timestamp_backup_host_ +
                             opt_host_reset_after_))
    {
      LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
               "switching host from %s to %s (reset host)",
               (*opt_host_chain_)[opt_host_chain_current_].c_str(),
               (*opt_host_chain_)[0].c_str());
      opt_host_chain_current_ = 0;
      opt_timestamp_backup_host_ = 0;
    }
  }

  if (!opt_proxy_groups_ ||
      ((*opt_proxy_groups_)[opt_proxy_groups_current_][0].url == "DIRECT"))
  {
    info->proxy = "";
    curl_easy_setopt(info->curl_handle, CURLOPT_PROXY, info->proxy.c_str());
  } else {
    ProxyInfo *proxy = &((*opt_proxy_groups_)[opt_proxy_groups_current_][0]);
    ValidateProxyIpsUnlocked(proxy->url, proxy->host);
    info->proxy = proxy->url;
    if (proxy->host.status() == dns::kFailOk) {
      curl_easy_setopt(info->curl_handle, CURLOPT_PROXY, info->proxy.c_str());
    } else {
      // We know it can't work, don't even try to download: TODO
      curl_easy_setopt(info->curl_handle, CURLOPT_PROXY, info->proxy.c_str());
      //curl_easy_setopt(info->curl_handle, CURLOPT_PROXY, "http://$.");
    }
  }
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
  pthread_mutex_unlock(lock_options_);

  curl_easy_setopt(curl_handle, CURLOPT_URL,
                   EscapeUrl((url_prefix + *(info->url))).c_str());
  //LogCvmfs(kLogDownload, kLogDebug, "set url %s for info %p / curl handle %p",
  //         EscapeUrl((url_prefix + *(info->url))).c_str(), info, curl_handle);
}


/**
 * Checks if the name resolving information is still up to date.  The host
 * object should be one from the current load-balance group.  If the information
 * changed, gather new set of resolved IPs and, if different, exchange them in
 * the load-balance group on the fly.  In the latter case, also rebalance the
 * proxies.  The options mutex needs to be open.
 */
void DownloadManager::ValidateProxyIpsUnlocked(
  const string &url,
  const dns::Host &host)
{
  if (!host.IsExpired())
    return;
  LogCvmfs(kLogDownload, kLogDebug, "validate DNS entry for %s",
           host.name().c_str());

  unsigned group_idx = opt_proxy_groups_current_;
  dns::Host new_host = resolver->Resolve(host.name());

  bool update_only = true;  // No changes to the list of IP addresses.
  if (new_host.status() != dns::kFailOk) {
    // Try again later when resolving fails.
    LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
             "failed to resolve IP addresses for %s (%d - %s)",
             host.name().c_str(), new_host.status(),
             dns::Code2Ascii(new_host.status()));
    new_host = dns::Host::ExtendDeadline(host, dns::Resolver::kMinTtl);
  } else if (!host.IsEquivalent(new_host)) {
    update_only = false;
  }

  if (update_only) {
    for (unsigned i = 0; i < (*opt_proxy_groups_)[group_idx].size(); ++i) {
      if ((*opt_proxy_groups_)[group_idx][i].host.id() == host.id())
        (*opt_proxy_groups_)[group_idx][i].host = new_host;
    }
    return;
  }

  assert(new_host.status() == dns::kFailOk);

  // Remove old host objects, insert new objects, and rebalance.
  LogCvmfs(kLogDownload, kLogDebug | kLogSyslog,
           "DNS entries for proxy %s changed, adjusting", host.name().c_str());
  vector<ProxyInfo> *group = &((*opt_proxy_groups_)[opt_proxy_groups_current_]);
  for (unsigned i = 0; i < group->size(); ) {
    if ((*group)[i].host.id() == host.id()) {
      group->erase(group->begin() + i);
    } else {
      i++;
    }
  }
  vector<ProxyInfo> new_infos;
  // IPv6 addresses have precedence
  set<string>::const_iterator iter_ips;
  if (new_host.HasIpv6()) {
    iter_ips = new_host.ipv6_addresses().begin();
    for (; iter_ips != new_host.ipv6_addresses().end(); ++iter_ips) {
      string url_ip = dns::RewriteUrl(url, *iter_ips);
      new_infos.push_back(ProxyInfo(new_host, url_ip));
    }
  } else {
    // IPv4
    iter_ips = new_host.ipv4_addresses().begin();
    for (; iter_ips != new_host.ipv4_addresses().end(); ++iter_ips) {
      string url_ip = dns::RewriteUrl(url, *iter_ips);
      new_infos.push_back(ProxyInfo(new_host, url_ip));
    }
  }
  group->insert(group->end(), new_infos.begin(), new_infos.end());

  RebalanceProxiesUnlocked();
}


/**
 * Adds transfer time and downloaded bytes to the global counters.
 */
void DownloadManager::UpdateStatistics(CURL *handle) {
  double val;

  if (curl_easy_getinfo(handle, CURLINFO_SIZE_DOWNLOAD, &val) == CURLE_OK)
    statistics_->transferred_bytes += val;
}


/**
 * Retry if possible if not on no-cache and if not already done too often.
 */
bool DownloadManager::CanRetry(const JobInfo *info) {
  pthread_mutex_lock(lock_options_);
  unsigned max_retries = opt_max_retries_;
  pthread_mutex_unlock(lock_options_);

  return !info->nocache && (info->num_retries < max_retries) &&
    ((info->error_code == kFailProxyConnection) ||
     (info->error_code == kFailHostConnection));
}


/**
 * Backoff for retry to introduce a jitter into a cluster of requesting
 * cvmfs nodes.
 * Retry only when HTTP caching is on.
 *
 * \return true if backoff has been performed, false otherwise
 */
void DownloadManager::Backoff(JobInfo *info) {
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

  LogCvmfs(kLogDownload, kLogDebug, "backing off for %d ms", info->backoff_ms);
  SafeSleepMs(info->backoff_ms);
}


/**
 * Checks the result of a curl download and implements the failure logic, such
 * as changing the proxy server.  Takes care of cleanup.
 *
 * \return true if another download should be performed, false otherwise
 */
bool DownloadManager::VerifyAndFinalize(const int curl_error, JobInfo *info) {
  LogCvmfs(kLogDownload, kLogDebug, "Verify downloaded url %s (curl error %d)",
           info->url->c_str(), curl_error);
  UpdateStatistics(info->curl_handle);

  // Verification and error classification
  switch (curl_error) {
    case CURLE_OK:
      // Verify content hash
      if (info->expected_hash) {
        shash::Any match_hash;
        shash::Final(info->hash_context, &match_hash);
        if (match_hash != *(info->expected_hash)) {
          LogCvmfs(kLogDownload, kLogDebug,
                   "hash verification of %s failed (expected %s, got %s)",
                   info->url->c_str(), info->expected_hash->ToString().c_str(),
                   match_hash.ToString().c_str());
          info->error_code = kFailBadData;
          break;
        }
      }

      // Decompress memory in a single run
      if ((info->destination == kDestinationMem) && info->compressed) {
        void *buf;
        uint64_t size;
        bool retval = zlib::DecompressMem2Mem(info->destination_mem.data,
                                              info->destination_mem.size,
                                              &buf, &size);
        if (retval) {
          free(info->destination_mem.data);
          info->destination_mem.data = static_cast<char *>(buf);
          info->destination_mem.size = size;
        } else {
          LogCvmfs(kLogDownload, kLogDebug,
                   "decompression (memory) of url %s failed",
                   info->url->c_str());
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
      info->error_code = kFailProxyResolve;
      break;
    case CURLE_COULDNT_RESOLVE_HOST:
      info->error_code = kFailHostResolve;
      break;
    case CURLE_COULDNT_CONNECT:
    case CURLE_OPERATION_TIMEDOUT:
    case CURLE_PARTIAL_FILE:
      if (info->proxy != "")
        // This is a guess.  Fail-over can still change to switching host
        info->error_code = kFailProxyConnection;
      else
        info->error_code = kFailHostConnection;
      break;
    case CURLE_ABORTED_BY_CALLBACK:
    case CURLE_WRITE_ERROR:
      // Error set by callback
      break;
    default:
      LogCvmfs(kLogDownload, kLogSyslogErr, "unexpected curl error (%d) while "
               "trying to fetch %s", curl_error, info->url->c_str());
      info->error_code = kFailOther;
      break;
  }

  // Determination if download should be repeated
  bool try_again = false;
  bool same_url_retry = CanRetry(info);
  if (info->error_code != kFailOk) {
    pthread_mutex_lock(lock_options_);
    if ((info->error_code) == kFailBadData && !info->nocache)
      try_again = true;
    if ( same_url_retry || (
         ( (info->error_code == kFailHostResolve) ||
           (info->error_code == kFailHostConnection) ||
           (info->error_code == kFailHostHttp)) &&
         info->probe_hosts &&
         opt_host_chain_ && (info->num_used_hosts < opt_host_chain_->size()))
       )
    {
      try_again = true;
    }
    if ( same_url_retry || (
         ( (info->error_code == kFailProxyResolve) ||
           (info->error_code == kFailProxyConnection) ||
           (info->error_code == kFailProxyHttp)) )
       )
    {
      try_again = true;
      // If all proxies failed, do a next round with the next host
      if (!same_url_retry && (info->num_used_proxies >= opt_num_proxies_)) {
        // Check if this can be made a host fail-over
        if (info->probe_hosts &&
            opt_host_chain_ &&
            (info->num_used_hosts < opt_host_chain_->size()))
        {
          // reset proxy group if not already performed by other handle
          if (opt_proxy_groups_) {
            if ((opt_proxy_groups_current_ > 0) ||
                (opt_proxy_groups_current_burned_ > 1))
            {
              string old_proxy;
              old_proxy =
                (*opt_proxy_groups_)[opt_proxy_groups_current_][0].url;
              opt_proxy_groups_current_ = 0;
              RebalanceProxiesUnlocked();
              opt_timestamp_backup_proxies_ = 0;
              if (opt_proxy_groups_) {
                LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
                         "switching proxy from %s to %s "
                         "(reset proxies for host failover)",
                         old_proxy.c_str(),
                         (*opt_proxy_groups_)[0][0].url.c_str());
              }
            }
          }

          // Make it a host failure
          info->num_used_proxies = 1;
          info->error_code = kFailHostAfterProxy;
        } else {
          try_again = false;
        }
      }  // Make a proxy failure a host failure
    }  // Proxy failure assumed
    pthread_mutex_unlock(lock_options_);
  }

  if (try_again) {
    LogCvmfs(kLogDownload, kLogDebug, "Trying again on same curl handle, "
             "same url: %d", same_url_retry);
    // Reset internal state and destination
    if ((info->destination == kDestinationMem) && info->destination_mem.data) {
      if (info->destination_mem.data)
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
      rewind(info->destination_file);
    }
    if (info->expected_hash)
      shash::Init(info->hash_context);
    if (info->compressed)
      zlib::DecompressInit(&info->zstream);

    // Failure handling
    bool switch_proxy = false;
    bool switch_host = false;
    switch (info->error_code) {
      case kFailBadData:
        header_lists_->AppendHeader(info->headers, "Pragma: no-cache");
        header_lists_->AppendHeader(info->headers, "Cache-Control: no-cache");
        curl_easy_setopt(info->curl_handle, CURLOPT_HTTPHEADER, info->headers);
        info->nocache = true;
        break;
      case kFailProxyResolve:
      case kFailProxyHttp:
        switch_proxy = true;
        break;
      case kFailHostResolve:
      case kFailHostHttp:
      case kFailHostAfterProxy:
        switch_host = true;
        break;
      case kFailProxyConnection:
        if (same_url_retry)
          Backoff(info);
        else
          switch_proxy = true;
        break;
      case kFailHostConnection:
        if (same_url_retry)
          Backoff(info);
        else
          switch_host = true;
        break;
      default:
        // No other errors expected when retrying
        abort();
    }
    if (switch_proxy) {
      SwitchProxy(info);
      info->num_used_proxies++;
      SetUrlOptions(info);
    }
    if (switch_host) {
      SwitchHost(info);
      info->num_used_hosts++;
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

  if (info->headers) {
    header_lists_->PutList(info->headers);
    info->headers = NULL;
  }

  return false;  // stop transfer and return to Fetch()
}


DownloadManager::DownloadManager() {
  pool_handles_idle_ = NULL;
  pool_handles_inuse_ = NULL;
  pool_max_handles_ = 0;
  curl_multi_ = NULL;
  default_headers_ = NULL;

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
  lock_synchronous_mode_ =
  reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  retval = pthread_mutex_init(lock_synchronous_mode_, NULL);
  assert(retval == 0);

  opt_dns_server_ = NULL;
  opt_timeout_proxy_ = 0;
  opt_timeout_direct_ = 0;
  opt_host_chain_ = NULL;
  opt_host_chain_rtt_ = NULL;
  opt_host_chain_current_ = 0;
  opt_proxy_groups_ = NULL;
  opt_proxy_groups_current_ = 0;
  opt_proxy_groups_current_burned_ = 0;
  opt_num_proxies_ = 0;
  opt_max_retries_ = 0;
  opt_backoff_init_ms_ = 0;
  opt_backoff_max_ms_ = 0;
  enable_info_header_ = false;
  opt_ipv4_only_ = false;

  resolver = NULL;

  opt_timestamp_backup_proxies_ = 0;
  opt_timestamp_failover_proxies_ = 0;
  opt_proxy_groups_reset_after_ = 0;
  opt_timestamp_backup_host_ = 0;
  opt_host_reset_after_ = 0;

  statistics_ = NULL;
}


DownloadManager::~DownloadManager() {
  pthread_mutex_destroy(lock_options_);
  pthread_mutex_destroy(lock_synchronous_mode_);
  free(lock_options_);
  free(lock_synchronous_mode_);
}

void DownloadManager::InitHeaders() {
  // User-Agent
  string cernvm_id = "User-Agent: cvmfs ";
#ifdef CVMFS_LIBCVMFS
  cernvm_id += "libcvmfs ";
#else
  cernvm_id += "Fuse ";
#endif
  cernvm_id += string(VERSION);
  if (getenv("CERNVM_UUID") != NULL) {
    cernvm_id += " " +
    sanitizer::InputSanitizer("az AZ 09 -").Filter(getenv("CERNVM_UUID"));
  }
  user_agent_ = strdup(cernvm_id.c_str());

  header_lists_ = new HeaderLists();

  default_headers_ = header_lists_->GetList("Connection: Keep-Alive");
  header_lists_->AppendHeader(default_headers_, "Pragma:");
  header_lists_->AppendHeader(default_headers_, user_agent_);
}


void DownloadManager::FiniHeaders() {
  delete header_lists_;
  header_lists_ = NULL;
  default_headers_ = NULL;
}


void DownloadManager::Init(const unsigned max_pool_handles,
                           const bool use_system_proxy)
{
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

  statistics_ = new Statistics();

  user_agent_ = NULL;
  InitHeaders();

  curl_multi_ = curl_multi_init();
  assert(curl_multi_ != NULL);
  curl_multi_setopt(curl_multi_, CURLMOPT_SOCKETFUNCTION, CallbackCurlSocket);
  curl_multi_setopt(curl_multi_, CURLMOPT_SOCKETDATA,
                    static_cast<void *>(this));
  curl_multi_setopt(curl_multi_, CURLMOPT_MAXCONNECTS, watch_fds_max_);
  curl_multi_setopt(curl_multi_, CURLMOPT_MAX_TOTAL_CONNECTIONS,
                    pool_max_handles_);
  //curl_multi_setopt(curl_multi_, CURLMOPT_PIPELINING, 1);

  prng_.InitLocaltime();

  // Name resolving
  if ((getenv("CVMFS_IPV4_ONLY") != NULL) &&
      (strlen(getenv("CVMFS_IPV4_ONLY")) > 0))
  {
    opt_ipv4_only_ = true;
  }
  resolver = dns::NormalResolver::Create(opt_ipv4_only_,
                                         3 /* retries */, 3000 /* timeout */);
  assert(resolver);

  // Parsing environment variables
  if (use_system_proxy) {
    if (getenv("http_proxy") == NULL) {
      SetProxyChain("");
    } else {
      SetProxyChain(string(getenv("http_proxy")));
    }
  }
}


void DownloadManager::Fini() {
  if (atomic_xadd32(&multi_threaded_, 0) == 1) {
    // Shutdown I/O thread
    char buf = 'T';
    WritePipe(pipe_terminate_[1], &buf, 1);
    pthread_join(thread_download_, NULL);
    // All handles are removed from the multi stack
    close(pipe_terminate_[1]);
    close(pipe_terminate_[0]);
    close(pipe_jobs_[1]);
    close(pipe_jobs_[0]);
  }

  for (set<CURL *>::iterator i = pool_handles_idle_->begin(),
       iEnd = pool_handles_idle_->end(); i != iEnd; ++i)
  {
    curl_easy_cleanup(*i);
  }
  delete pool_handles_idle_;
  delete pool_handles_inuse_;
  curl_multi_cleanup(curl_multi_);
  pool_handles_idle_ = NULL;
  pool_handles_inuse_ = NULL;
  curl_multi_ = NULL;

  FiniHeaders();
  if (user_agent_)
    free(user_agent_);
  user_agent_ = NULL;

  delete statistics_;
  statistics_ = NULL;

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
void DownloadManager::Spawn() {
  MakePipe(pipe_terminate_);
  MakePipe(pipe_jobs_);

  int retval = pthread_create(&thread_download_, NULL, MainDownload,
                              static_cast<void *>(this));
  assert(retval == 0);

  atomic_inc32(&multi_threaded_);
}


/**
 * Downloads data from an unsecure outside channel (currently HTTP or file).
 */
Failures DownloadManager::Fetch(JobInfo *info) {
  assert(info != NULL);
  assert(info->url != NULL);

  Failures result;
  result = PrepareDownloadDestination(info);
  if (result != kFailOk)
    return result;

  if (info->expected_hash) {
    const shash::Algorithms algorithm = info->expected_hash->algorithm;
    info->hash_context.algorithm = algorithm;
    info->hash_context.size = shash::GetContextSize(algorithm);
    info->hash_context.buffer = alloca(info->hash_context.size);
  }

  // Prepare cvmfs-info: header, allocate string on the stack
  info->info_header = NULL;
  if (enable_info_header_ && info->extra_info) {
    const char *header_name = "cvmfs-info: ";
    const size_t header_name_len = strlen(header_name);
    const unsigned header_size = 1 + header_name_len +
      EscapeHeader(*(info->extra_info), NULL, 0);
    info->info_header = static_cast<char *>(alloca(header_size));
    memcpy(info->info_header, header_name, header_name_len);
    EscapeHeader(*(info->extra_info), info->info_header + header_name_len,
                 header_size - header_name_len);
    info->info_header[header_size-1] = '\0';
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
    pthread_mutex_lock(lock_synchronous_mode_);
    CURL *handle = AcquireCurlHandle();
    InitializeRequest(info, handle);
    SetUrlOptions(info);
    //curl_easy_setopt(handle, CURLOPT_VERBOSE, 1);
    int retval;
    do {
      retval = curl_easy_perform(handle);
      statistics_->num_requests++;
      double elapsed;
      if (curl_easy_getinfo(handle, CURLINFO_TOTAL_TIME, &elapsed) == CURLE_OK)
        statistics_->transfer_time += elapsed;
    } while (VerifyAndFinalize(retval, info));
    result = info->error_code;
    ReleaseCurlHandle(info->curl_handle);
    pthread_mutex_unlock(lock_synchronous_mode_);
  }

  if (result != kFailOk) {
    LogCvmfs(kLogDownload, kLogDebug, "download failed (error %d - %s)", result,
             Code2Ascii(result));

    if (info->destination == kDestinationPath)
      unlink(info->destination_path->c_str());

    if (info->destination_mem.data) {
      free(info->destination_mem.data);
      info->destination_mem.data = NULL;
      info->destination_mem.size = 0;
    }
  }

  return result;
}


/**
 * Sets a DNS server.  Only for testing as it cannot be reverted to the system
 * default.
 */
void DownloadManager::SetDnsServer(const string &address) {
  pthread_mutex_lock(lock_options_);
  if (opt_dns_server_)
    free(opt_dns_server_);
  if (address != "") {
    opt_dns_server_ = strdup(address.c_str());
    assert(opt_dns_server_);

    vector<string> servers;
    servers.push_back(address);
    bool retval = resolver->SetResolvers(servers);
    assert(retval);
  }
  pthread_mutex_unlock(lock_options_);
  LogCvmfs(kLogDownload, kLogSyslog, "set nameserver to %s", address.c_str());
}


/**
 * Sets the DNS query timeout parameters.
 */
void DownloadManager::SetDnsParameters(
  const unsigned retries,
  const unsigned timeout_sec)
{
  pthread_mutex_lock(lock_options_);
  delete resolver;
  resolver = NULL;
  resolver =
    dns::NormalResolver::Create(opt_ipv4_only_, retries, timeout_sec*1000);
  assert(resolver);
  pthread_mutex_unlock(lock_options_);
}


/**
 * Sets two timeout values for proxied and for direct conections, respectively.
 * The timeout counts for all sorts of connection phases,
 * DNS, HTTP connect, etc.
 */
void DownloadManager::SetTimeout(const unsigned seconds_proxy,
                                 const unsigned seconds_direct)
{
  pthread_mutex_lock(lock_options_);
  opt_timeout_proxy_ = seconds_proxy;
  opt_timeout_direct_ = seconds_direct;
  pthread_mutex_unlock(lock_options_);
}


/**
 * Receives the currently active timeout values.
 */
void DownloadManager::GetTimeout(unsigned *seconds_proxy,
                                 unsigned *seconds_direct)
{
  pthread_mutex_lock(lock_options_);
  *seconds_proxy = opt_timeout_proxy_;
  *seconds_direct = opt_timeout_direct_;
  pthread_mutex_unlock(lock_options_);
}


const Statistics &DownloadManager::GetStatistics() {
  return *statistics_;
}


/**
 * Parses a list of ';'-separated hosts for the host chain.  The empty string
 * removes the host list.
 */
void DownloadManager::SetHostChain(const string &host_list) {
  pthread_mutex_lock(lock_options_);
  opt_timestamp_backup_host_ = 0;
  delete opt_host_chain_;
  delete opt_host_chain_rtt_;
  opt_host_chain_current_ = 0;

  if (host_list == "") {
    opt_host_chain_ = NULL;
    opt_host_chain_rtt_ = NULL;
    pthread_mutex_unlock(lock_options_);
    return;
  }

  opt_host_chain_ = new vector<string>(SplitString(host_list, ';'));
  opt_host_chain_rtt_ = new vector<int>();
  //LogCvmfs(kLogDownload, kLogSyslog, "using host %s",
  //         (*opt_host_chain_)[0].c_str());
  for (unsigned i = 0, s = opt_host_chain_->size(); i < s; ++i)
    opt_host_chain_rtt_->push_back(-1);
  pthread_mutex_unlock(lock_options_);
}


/**
 * Retrieves the currently set chain of hosts, their round trip times, and the
 * currently used host.
 */
void DownloadManager::GetHostInfo(vector<string> *host_chain, vector<int> *rtt,
                                  unsigned *current_host)
{
  pthread_mutex_lock(lock_options_);
  if (opt_host_chain_) {
    *current_host = opt_host_chain_current_;
    *host_chain = *opt_host_chain_;
    *rtt = *opt_host_chain_rtt_;
  }
  pthread_mutex_unlock(lock_options_);
}


/**
 * Jumps to the next proxy in the ring of forward proxy servers.
 * Selects one randomly from a load-balancing group.
 *
 * If info is set, switch only if the current proxy is identical to the one used
 * by info, otherwise another transfer has already done the switch.
 */
void DownloadManager::SwitchProxy(JobInfo *info) {
  pthread_mutex_lock(lock_options_);

  if (!opt_proxy_groups_) {
    pthread_mutex_unlock(lock_options_);
    return;
  }
  if (info &&
      ((*opt_proxy_groups_)[opt_proxy_groups_current_][0].url != info->proxy))
  {
    pthread_mutex_unlock(lock_options_);
    return;
  }

  statistics_->num_proxy_failover++;
  string old_proxy = (*opt_proxy_groups_)[opt_proxy_groups_current_][0].url;

  // If all proxies from the current load-balancing group are burned, switch to
  // another group
  if (opt_proxy_groups_current_burned_ ==
      (*opt_proxy_groups_)[opt_proxy_groups_current_].size())
  {
    opt_proxy_groups_current_burned_ = 0;
    if (opt_proxy_groups_->size() > 1) {
      opt_proxy_groups_current_ = (opt_proxy_groups_current_ + 1) %
      opt_proxy_groups_->size();
      // Remeber the timestamp of switching to backup proxies
      if (opt_proxy_groups_reset_after_ > 0) {
        if (opt_proxy_groups_current_ > 0) {
          if (opt_timestamp_backup_proxies_ == 0)
            opt_timestamp_backup_proxies_ = time(NULL);
          //LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
          //         "switched to (another) backup proxy group");
        } else {
          opt_timestamp_backup_proxies_ = 0;
          //LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
          //         "switched back to primary proxy group");
        }
        opt_timestamp_failover_proxies_ = 0;
      }
    }
  } else {
    // failover within the same group
    if (opt_proxy_groups_reset_after_ > 0) {
      if (opt_timestamp_failover_proxies_ == 0)
        opt_timestamp_failover_proxies_ = time(NULL);
    }
  }

  vector<ProxyInfo> *group = &((*opt_proxy_groups_)[opt_proxy_groups_current_]);
  const unsigned group_size = group->size();

  // Move active proxy to the back
  if (opt_proxy_groups_current_burned_) {
    const ProxyInfo swap = (*group)[0];
    (*group)[0] = (*group)[group_size - opt_proxy_groups_current_burned_];
    (*group)[group_size - opt_proxy_groups_current_burned_] = swap;
  }
  opt_proxy_groups_current_burned_++;

  // Select new one
  if ((group_size - opt_proxy_groups_current_burned_) > 0) {
    int select = prng_.Next(group_size - opt_proxy_groups_current_burned_ + 1);

    // Move selected proxy to front
    const ProxyInfo swap = (*group)[select];
    (*group)[select] = (*group)[0];
    (*group)[0] = swap;
  }

  LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
           "switching proxy from %s to %s",
           old_proxy.c_str(), (*group)[0].url.c_str());
  LogCvmfs(kLogDownload, kLogDebug, "%d proxies remain in group",
           group_size - opt_proxy_groups_current_burned_);

  pthread_mutex_unlock(lock_options_);
}


/**
 * Switches to the next host in the chain.  If info is set, switch only if the
 * current host is identical to the one used by info, otherwise another transfer
 * has already done the switch.
 */
void DownloadManager::SwitchHost(JobInfo *info) {
  bool do_switch = true;

  pthread_mutex_lock(lock_options_);
  if (!opt_host_chain_ || (opt_host_chain_->size() == 1)) {
    pthread_mutex_unlock(lock_options_);
    return;
  }

  if (info) {
    char *effective_url;
    curl_easy_getinfo(info->curl_handle, CURLINFO_EFFECTIVE_URL,
                      &effective_url);
    if (!HasPrefix(string(effective_url) + "/",
                   (*opt_host_chain_)[opt_host_chain_current_] + "/",
                   true))
    {
      do_switch = false;
      LogCvmfs(kLogDownload, kLogDebug, "don't switch host, "
               "effective url: %s, current host: %s", effective_url,
               (*opt_host_chain_)[opt_host_chain_current_].c_str());
    }
  }

  if (do_switch) {
    string old_host = (*opt_host_chain_)[opt_host_chain_current_];
    opt_host_chain_current_ = (opt_host_chain_current_+1) %
    opt_host_chain_->size();
    statistics_->num_host_failover++;
    LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
             "switching host from %s to %s", old_host.c_str(),
             (*opt_host_chain_)[opt_host_chain_current_].c_str());

    // Remeber the timestamp of switching to backup host
    if (opt_host_reset_after_ > 0) {
      if (opt_host_chain_current_ != 0) {
        if (opt_timestamp_backup_host_ == 0)
          opt_timestamp_backup_host_ = time(NULL);
      } else {
        opt_timestamp_backup_host_ = 0;
      }
    }
  }
  pthread_mutex_unlock(lock_options_);
}


void DownloadManager::SwitchHost() {
  SwitchHost(NULL);
}


/**
 * Orders the hostlist according to RTT of downloading .cvmfschecksum.
 * Sets the current host to the best-responsive host.
 * If you change the host list in between by SetHostChain(), it will be
 * overwritten by this function.
 */
void DownloadManager::ProbeHosts() {
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
        host_rtt[i] = int(DiffTimeSeconds(tv_start, tv_end) * 1000);
        LogCvmfs(kLogDownload, kLogDebug, "probing host %s had %dms rtt",
                 url.c_str(), host_rtt[i]);
      } else {
        LogCvmfs(kLogDownload, kLogDebug, "error while probing host %s: %d - %s",
                 url.c_str(), result, Code2Ascii(result));
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

  pthread_mutex_lock(lock_options_);
  delete opt_host_chain_;
  delete opt_host_chain_rtt_;
  opt_host_chain_ = new vector<string>(host_chain);
  opt_host_chain_rtt_ = new vector<int>(host_rtt);
  opt_host_chain_current_ = 0;
  pthread_mutex_unlock(lock_options_);
}


/**
 * Parses a list of ';'- and '|'-separated proxy servers for the proxy groups.
 * The empty string removes the proxy chain.
 */
void DownloadManager::SetProxyChain(const string &proxy_list) {
  pthread_mutex_lock(lock_options_);

  opt_timestamp_backup_proxies_ = 0;
  opt_timestamp_failover_proxies_ = 0;
  delete opt_proxy_groups_;
  if (proxy_list == "") {
    opt_proxy_groups_ = NULL;
    opt_proxy_groups_current_ = 0;
    opt_proxy_groups_current_burned_ = 0;
    opt_num_proxies_ = 0;
    pthread_mutex_unlock(lock_options_);
    return;
  }

  // Resolve server names in provided urls, skip DIRECT
  vector<string> urls;  // All encountered URLs
  vector<string> proxy_groups = SplitString(proxy_list, ';');
  for (unsigned i = 0; i < proxy_groups.size(); ++i) {
    vector<string> this_group = SplitString(proxy_groups[i], '|');
    for (unsigned j = 0; j < this_group.size(); ++j) {
      // Note: DIRECT strings will be "extracted" to an empty string and not
      // resolved.
      urls.push_back(dns::ExtractHost(this_group[j]));
    }
  }
  vector<dns::Host> hosts;
  LogCvmfs(kLogDownload, kLogDebug, "resolving %u proxy addresses",
           urls.size());
  resolver->ResolveMany(urls, &hosts);

  // Construct opt_proxy_groups_: traverse proxy list in same order and expand
  // names to resolved IP addresses.
  opt_proxy_groups_ = new vector< vector<ProxyInfo> >();
  opt_num_proxies_ = 0;
  unsigned num_proxy = 0;  // Combined i, j counter
  for (unsigned i = 0; i < proxy_groups.size(); ++i) {
    vector<string> this_group = SplitString(proxy_groups[i], '|');
    vector<ProxyInfo> infos;
    for (unsigned j = 0; j < this_group.size(); ++j, ++num_proxy) {
      if (this_group[j] == "DIRECT") {
        infos.push_back(ProxyInfo("DIRECT"));
        continue;
      }

      if (hosts[num_proxy].status() != dns::kFailOk) {
        infos.push_back(ProxyInfo(hosts[num_proxy], this_group[j]));
        continue;
      }

      // IPv6 addresses have precedence
      set<string>::const_iterator iter_ips;
      if (hosts[num_proxy].HasIpv6()) {
        // IPv6
        iter_ips = hosts[num_proxy].ipv6_addresses().begin();
        for (; iter_ips != hosts[num_proxy].ipv6_addresses().end();
             ++iter_ips)
        {
          string url_ip = dns::RewriteUrl(this_group[j], *iter_ips);
          infos.push_back(ProxyInfo(hosts[num_proxy], url_ip));
        }
      } else {
        // IPv4
        iter_ips = hosts[num_proxy].ipv4_addresses().begin();
        for (; iter_ips != hosts[num_proxy].ipv4_addresses().end();
             ++iter_ips)
        {
          string url_ip = dns::RewriteUrl(this_group[j], *iter_ips);
          infos.push_back(ProxyInfo(hosts[num_proxy], url_ip));
        }
      }
    }
    opt_proxy_groups_->push_back(infos);
    opt_num_proxies_ += (*opt_proxy_groups_)[i].size();
  }
  LogCvmfs(kLogDownload, kLogDebug,
           "installed %u proxies in %u load-balance groups",
           opt_num_proxies_, opt_proxy_groups_->size());
  opt_proxy_groups_current_ = 0;
  opt_proxy_groups_current_burned_ = 1;

  // Select random start proxy from the first group.
  if ((*opt_proxy_groups_)[0].size() > 1) {
    int random_index = prng_.Next((*opt_proxy_groups_)[0].size());
    swap((*opt_proxy_groups_)[0][0], (*opt_proxy_groups_)[0][random_index]);
  }
  //LogCvmfs(kLogDownload, kLogSyslog, "using proxy %s",
  //         (*opt_proxy_groups_)[0][0].c_str());
  pthread_mutex_unlock(lock_options_);
}


/**
 * Retrieves the proxy chain and the currently active load-balancing group.
 */
void DownloadManager::GetProxyInfo(vector< vector<ProxyInfo> > *proxy_chain,
                                   unsigned *current_group)
{
  pthread_mutex_lock(lock_options_);

  if (!opt_proxy_groups_) {
    pthread_mutex_unlock(lock_options_);
    proxy_chain = NULL;
    current_group = NULL;
    return;
  }

  *proxy_chain = *opt_proxy_groups_;
  *current_group = opt_proxy_groups_current_;

  pthread_mutex_unlock(lock_options_);
}


/**
 * Selects a new random proxy in the current load-balancing group.  Resets the
 * "burned" counter.
 */
void DownloadManager::RebalanceProxiesUnlocked() {
  if (!opt_proxy_groups_)
    return;

  opt_timestamp_failover_proxies_ = 0;
  opt_proxy_groups_current_burned_ = 1;
  vector<ProxyInfo> *group = &((*opt_proxy_groups_)[opt_proxy_groups_current_]);
  int select = prng_.Next(group->size());
  swap((*group)[select], (*group)[0]);
  //LogCvmfs(kLogDownload, kLogDebug | kLogSyslog,
  //         "switching proxy from %s to %s (rebalance)",
  //         (*group)[select].c_str(), swap.c_str());
}


void DownloadManager::RebalanceProxies() {
  pthread_mutex_lock(lock_options_);
  RebalanceProxiesUnlocked();
  pthread_mutex_unlock(lock_options_);
}


/**
 * Switches to the next load-balancing group of proxy servers.
 */
void DownloadManager::SwitchProxyGroup() {
  pthread_mutex_lock(lock_options_);

  if (!opt_proxy_groups_ || (opt_proxy_groups_->size() < 2)) {
    pthread_mutex_unlock(lock_options_);
    return;
  }

  //string old_proxy = (*opt_proxy_groups_)[opt_proxy_groups_current_][0];
  opt_proxy_groups_current_ = (opt_proxy_groups_current_ + 1) %
  opt_proxy_groups_->size();
  opt_proxy_groups_current_burned_ = 1;
  opt_timestamp_backup_proxies_ = time(NULL);
  opt_timestamp_failover_proxies_ = 0;
  //LogCvmfs(kLogDownload, kLogDebug | kLogSyslog,
  //         "switching proxy from %s to %s (manual group change)",
  //         old_proxy.c_str(),
  //         (*opt_proxy_groups_)[opt_proxy_groups_current_][0].c_str());

  pthread_mutex_unlock(lock_options_);
}


void DownloadManager::SetProxyGroupResetDelay(const unsigned seconds) {
  pthread_mutex_lock(lock_options_);
  opt_proxy_groups_reset_after_ = seconds;
  if (opt_proxy_groups_reset_after_ == 0) {
    opt_timestamp_backup_proxies_ = 0;
    opt_timestamp_failover_proxies_ = 0;
  }
  pthread_mutex_unlock(lock_options_);
}


void DownloadManager::GetProxyBackupInfo(unsigned *reset_delay,
                                         time_t *timestamp_failover)
{
  pthread_mutex_lock(lock_options_);
  *reset_delay = opt_proxy_groups_reset_after_;
  *timestamp_failover = opt_timestamp_backup_proxies_;
  pthread_mutex_unlock(lock_options_);
}


void DownloadManager::SetHostResetDelay(const unsigned seconds)
{
  pthread_mutex_lock(lock_options_);
  opt_host_reset_after_ = seconds;
  if (opt_host_reset_after_ == 0)
    opt_timestamp_backup_host_ = 0;
  pthread_mutex_unlock(lock_options_);
}


void DownloadManager::GetHostBackupInfo(unsigned *reset_delay,
                                        time_t *timestamp_failover)
{
  pthread_mutex_lock(lock_options_);
  *reset_delay = opt_host_reset_after_;
  *timestamp_failover = opt_timestamp_backup_host_;
  pthread_mutex_unlock(lock_options_);
}


void DownloadManager::SetRetryParameters(const unsigned max_retries,
                                         const unsigned backoff_init_ms,
                                         const unsigned backoff_max_ms)
{
  pthread_mutex_lock(lock_options_);
  opt_max_retries_ = max_retries;
  opt_backoff_init_ms_ = backoff_init_ms;
  opt_backoff_max_ms_ = backoff_max_ms;
  pthread_mutex_unlock(lock_options_);
}


void DownloadManager::EnableInfoHeader() {
  enable_info_header_ = true;
}


void DownloadManager::EnablePipelining() {
  curl_multi_setopt(curl_multi_, CURLMOPT_PIPELINING, 1);
}


//------------------------------------------------------------------------------


string Statistics::Print() const {
  return
  "Transferred Bytes: " + StringifyInt(uint64_t(transferred_bytes)) + "\n" +
  "Transfer duration: " + StringifyInt(uint64_t(transfer_time)) + " s\n" +
  "Number of requests: " + StringifyInt(num_requests) + "\n" +
  "Number of retries: " + StringifyInt(num_retries) + "\n" +
  "Number of proxy failovers: " + StringifyInt(num_proxy_failover) + "\n" +
  "Number of host failovers: " + StringifyInt(num_host_failover) + "\n";
}

}  // namespace download
