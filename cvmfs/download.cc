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

// TODO(jblomer): MS for time summing
#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "download.h"

#include <alloca.h>
#include <errno.h>
#include <inttypes.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <set>

#include "atomic.h"
#include "compression.h"
#include "duplex_curl.h"
#include "hash.h"
#include "logging.h"
#include "prng.h"
#include "sanitizer.h"
#include "smalloc.h"
#include "util/algorithm.h"
#include "util/exception.h"
#include "util/posix.h"
#include "util/string.h"
#include "util_concurrency.h"

using namespace std;  // NOLINT

namespace download {

static inline bool EscapeUrlChar(char input, char output[3]) {
  if (((input >= '0') && (input <= '9')) ||
      ((input >= 'A') && (input <= 'Z')) ||
      ((input >= 'a') && (input <= 'z')) ||
      (input == '/') || (input == ':') || (input == '.') ||
      (input == '@') ||
      (input == '+') || (input == '-') ||
      (input == '_') || (input == '~') ||
      (input == '[') || (input == ']') || (input == ','))
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
    if (info->destination_file == NULL) {
      LogCvmfs(kLogDownload, kLogDebug, "Failed to open path %s: %s"
               " (errno=%d).",
               info->destination_path->c_str(), strerror(errno), errno);
      return kFailLocalIO;
    }
  }

  if (info->destination == kDestinationSink)
    assert(info->destination_sink != NULL);

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

  // LogCvmfs(kLogDownload, kLogDebug, "REMOVE-ME: Header callback with %s",
  //          header_line.c_str());

  // Check http status codes
  if (HasPrefix(header_line, "HTTP/1.", false)) {
    if (header_line.length() < 10)
      return 0;

    unsigned i;
    for (i = 8; (i < header_line.length()) && (header_line[i] == ' '); ++i) {}

    // Code is initialized to -1
    if (header_line.length() > i+2) {
      info->http_code = DownloadManager::ParseHttpCode(&header_line[i]);
    }

    if ((info->http_code / 100) == 2) {
      return num_bytes;
    } else if ((info->http_code == 301) ||
               (info->http_code == 302) ||
               (info->http_code == 303) ||
               (info->http_code == 307))
    {
      if (!info->follow_redirects) {
        LogCvmfs(kLogDownload, kLogDebug, "redirect support not enabled: %s",
                 header_line.c_str());
        info->error_code = kFailHostHttp;
        return 0;
      }
      LogCvmfs(kLogDownload, kLogDebug, "http redirect: %s",
               header_line.c_str());
      // libcurl will handle this because of CURLOPT_FOLLOWLOCATION
      return num_bytes;
    } else {
      LogCvmfs(kLogDownload, kLogDebug, "http status error code: %s",
               header_line.c_str());
      if ((info->http_code / 100) == 5) {
        // 5XX returned by host
        info->error_code = kFailHostHttp;
      } else if ((info->http_code == 400) || (info->http_code == 404)) {
        // 400: error from the GeoAPI module
        // 404: the stratum 1 does not have the newest files
        info->error_code = kFailHostHttp;
      } else if (info->http_code == 429) {
        // 429: rate throttling (we ignore the backoff hint for the time being)
        info->error_code = kFailHostConnection;
      } else {
        info->error_code = (info->proxy == "DIRECT") ? kFailHostHttp :
                                                       kFailProxyHttp;
      }
      return 0;
    }
  }

  // Allocate memory for kDestinationMemory
  if ((info->destination == kDestinationMem) &&
      HasPrefix(header_line, "CONTENT-LENGTH:", true))
  {
    char *tmp = reinterpret_cast<char *>(alloca(num_bytes+1));
    uint64_t length = 0;
    sscanf(header_line.c_str(), "%s %" PRIu64, tmp, &length);
    if (length > 0) {
      if (length > DownloadManager::kMaxMemSize) {
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslogErr,
                 "resource %s too large to store in memory (%" PRIu64 ")",
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
  } else if (HasPrefix(header_line, "LOCATION:", true)) {
    // This comes along with redirects
    LogCvmfs(kLogDownload, kLogDebug, "%s", header_line.c_str());
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

  // LogCvmfs(kLogDownload, kLogDebug, "Data callback,  %d bytes", num_bytes);

  if (num_bytes == 0)
    return 0;

  if (info->expected_hash)
    shash::Update((unsigned char *)ptr, num_bytes, info->hash_context);

  if (info->destination == kDestinationSink) {
    if (info->compressed) {
      zlib::StreamStates retval =
        zlib::DecompressZStream2Sink(ptr, num_bytes,
                                     &info->zstream, info->destination_sink);
      if (retval == zlib::kStreamDataError) {
        LogCvmfs(kLogDownload, kLogSyslogErr, "failed to decompress %s",
                 info->url->c_str());
        info->error_code = kFailBadData;
        return 0;
      } else if (retval == zlib::kStreamIOError) {
        LogCvmfs(kLogDownload, kLogSyslogErr,
                 "decompressing %s, local IO error", info->url->c_str());
        info->error_code = kFailLocalIO;
        return 0;
      }
    } else {
      int64_t written = info->destination_sink->Write(ptr, num_bytes);
      if ((written < 0) || (static_cast<uint64_t>(written) != num_bytes)) {
        LogCvmfs(kLogDownload, kLogDebug, "Failed to perform write on %s (%"
                 PRId64 ")", info->url->c_str(), written);
        info->error_code = kFailLocalIO;
        return 0;
      }
    }
  } else if (info->destination == kDestinationMem) {
    // Write to memory
    if (info->destination_mem.pos + num_bytes > info->destination_mem.size) {
      if (info->destination_mem.size == 0) {
        LogCvmfs(kLogDownload, kLogDebug,
                 "Content-Length was missing or zero, but %zu bytes received",
                 info->destination_mem.pos + num_bytes);
      } else {
        LogCvmfs(kLogDownload, kLogDebug, "Callback had too much data: "
                 "start %zu, bytes %zu, expected %zu",
                 info->destination_mem.pos,
                 num_bytes,
                 info->destination_mem.size);
      }
      info->error_code = kFailBadData;
      return 0;
    }
    memcpy(info->destination_mem.data + info->destination_mem.pos,
           ptr, num_bytes);
    info->destination_mem.pos += num_bytes;
  } else {
    // Write to file
    if (info->compressed) {
      // LogCvmfs(kLogDownload, kLogDebug, "REMOVE-ME: writing %d bytes for %s",
      //          num_bytes, info->url->c_str());
      zlib::StreamStates retval =
        zlib::DecompressZStream2File(ptr, num_bytes,
                                     &info->zstream, info->destination_file);
      if (retval == zlib::kStreamDataError) {
        LogCvmfs(kLogDownload, kLogSyslogErr, "failed to decompress %s",
                 info->url->c_str());
        info->error_code = kFailBadData;
        return 0;
      } else if (retval == zlib::kStreamIOError) {
        LogCvmfs(kLogDownload, kLogSyslogErr,
                 "decompressing %s, local IO error", info->url->c_str());
        info->error_code = kFailLocalIO;
        return 0;
      }
    } else {
      if (fwrite(ptr, 1, num_bytes, info->destination_file) != num_bytes) {
       LogCvmfs(kLogDownload, kLogSyslogErr,
                 "downloading %s, IO failure: %s (errno=%d)",
                 info->url->c_str(), strerror(errno), errno);
        info->error_code = kFailLocalIO;
        return 0;
      }
    }
  }

  return num_bytes;
}


//------------------------------------------------------------------------------


bool JobInfo::IsFileNotFound() {
  if (HasPrefix(*url, "file://", true /* ignore_case */))
    return error_code == kFailHostConnection;

  return http_code == 404;
}


//------------------------------------------------------------------------------


const int DownloadManager::kProbeUnprobed = -1;
const int DownloadManager::kProbeDown     = -2;
const int DownloadManager::kProbeGeo      = -3;
const unsigned DownloadManager::kMaxMemSize = 1024*1024;


/**
 * -1 of digits is not a valid Http return code
 */
int DownloadManager::ParseHttpCode(const char digits[3]) {
  int result = 0;
  int factor = 100;
  for (int i = 0; i < 3; ++i) {
    if ((digits[i] < '0') || (digits[i] > '9'))
      return -1;
    result += (digits[i] - '0') * factor;
    factor /= 10;
  }
  return result;
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
  // LogCvmfs(kLogDownload, kLogDebug, "CallbackCurlSocket called with easy "
  //          "handle %p, socket %d, action %d", easy, s, action);
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
      download_mgr->watch_fds_[index].events = POLLIN | POLLPRI;
      break;
    case CURL_POLL_OUT:
      download_mgr->watch_fds_[index].events = POLLOUT | POLLWRBAND;
      break;
    case CURL_POLL_INOUT:
      download_mgr->watch_fds_[index].events =
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
        // LogCvmfs(kLogDownload, kLogDebug, "shrinking watch_fds_ (%d)",
        //          watch_fds_size_);
        download_mgr->watch_fds_ = static_cast<struct pollfd *>(
          srealloc(download_mgr->watch_fds_,
                   download_mgr->watch_fds_size_*sizeof(struct pollfd)));
        // LogCvmfs(kLogDownload, kLogDebug, "shrinking watch_fds_ done",
        //          watch_fds_size_);
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
      /* NOTE: The following might degrade the performance for many small files
       * use case. TODO(jblomer): look into it.
      // Specify a timeout for polling in ms; this allows us to return
      // to libcurl once a second so it can look for internal operations
      // which timed out.  libcurl has a more elaborate mechanism
      // (CURLMOPT_TIMERFUNCTION) that would inform us of the next potential
      // timeout.  TODO(bbockelm) we should switch to that in the future.
      timeout = 100;
      */
      timeout = 1;
    } else {
      timeout = -1;
      gettimeofday(&timeval_stop, NULL);
      int64_t delta = static_cast<int64_t>(
        1000 * DiffTimeSeconds(timeval_start, timeval_stop));
      perf::Xadd(download_mgr->counters_->sz_transfer_time, delta);
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
    }

    // Activity on curl sockets
    // Within this loop the curl_multi_socket_action() may cause socket(s)
    // to be removed from watch_fds_. If a socket is removed it is replaced
    // by the socket at the end of the array and the inuse count is decreased.
    // Therefore loop over the array in reverse order.
    for (int64_t i = download_mgr->watch_fds_inuse_-1; i >= 2; --i) {
      if (i >= download_mgr->watch_fds_inuse_) {
        continue;
      }
      if (download_mgr->watch_fds_[i].revents) {
        int ev_bitmask = 0;
        if (download_mgr->watch_fds_[i].revents & (POLLIN | POLLPRI))
          ev_bitmask |= CURL_CSELECT_IN;
        if (download_mgr->watch_fds_[i].revents & (POLLOUT | POLLWRBAND))
          ev_bitmask |= CURL_CSELECT_OUT;
        if (download_mgr->watch_fds_[i].revents &
            (POLLERR | POLLHUP | POLLNVAL))
        {
          ev_bitmask |= CURL_CSELECT_ERR;
        }
        download_mgr->watch_fds_[i].revents = 0;

        retval = curl_multi_socket_action(download_mgr->curl_multi_,
                                          download_mgr->watch_fds_[i].fd,
                                          ev_bitmask,
                                          &still_running);
      }
    }

    // Check if transfers are completed
    CURLMsg *curl_msg;
    int msgs_in_queue;
    while ((curl_msg = curl_multi_info_read(download_mgr->curl_multi_,
                                            &msgs_in_queue)))
    {
      if (curl_msg->msg == CURLMSG_DONE) {
        perf::Inc(download_mgr->counters_->n_requests);
        JobInfo *info;
        CURL *easy_handle = curl_msg->easy_handle;
        int curl_error = curl_msg->data.result;
        curl_easy_getinfo(easy_handle, CURLINFO_PRIVATE, &info);

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


/**
 * Ensures that a certain header string is _not_ part of slist on return.
 * Note that if the first header element matches, the returned slist points
 * to a different value.
 */
void HeaderLists::CutHeader(const char *header, curl_slist **slist) {
  assert(slist);
  curl_slist head;
  head.next = *slist;
  curl_slist *prev = &head;
  curl_slist *rover = *slist;
  while (rover) {
    if (strcmp(rover->data, header) == 0) {
      prev->next = rover->next;
      Put(rover);
      rover = prev;
    }
    prev = rover;
    rover = rover->next;
  }
  *slist = head.next;
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


void HeaderLists::AddBlock() {
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
    // curl_easy_setopt(curl_default, CURLOPT_FAILONERROR, 1);
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
  info->http_code = -1;
  info->follow_redirects = follow_redirects_;
  info->num_used_proxies = 1;
  info->num_used_hosts = 1;
  info->num_retries = 0;
  info->backoff_ms = 0;
  info->headers = header_lists_->DuplicateList(default_headers_);
  if (info->info_header) {
    header_lists_->AppendHeader(info->headers, info->info_header);
  }
  if (info->force_nocache) {
    SetNocache(info);
  } else {
    info->nocache = false;
  }
  if (info->compressed) {
    zlib::DecompressInit(&(info->zstream));
  }
  if (info->expected_hash) {
    assert(info->hash_context.buffer != NULL);
    shash::Init(info->hash_context);
  }

  if ((info->range_offset != -1) && (info->range_size)) {
    char byte_range_array[100];
    const int64_t range_lower = static_cast<int64_t>(info->range_offset);
    const int64_t range_upper = static_cast<int64_t>(
      info->range_offset + info->range_size - 1);
    if (snprintf(byte_range_array, sizeof(byte_range_array),
                 "%" PRId64 "-%" PRId64,
                 range_lower, range_upper) == 100)
    {
      PANIC(NULL);  // Should be impossible given limits on offset size.
    }
    curl_easy_setopt(handle, CURLOPT_RANGE, byte_range_array);
  } else {
    curl_easy_setopt(handle, CURLOPT_RANGE, NULL);
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
  if (follow_redirects_) {
    curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 1);
    curl_easy_setopt(handle, CURLOPT_MAXREDIRS, 4);
  }
}


/**
 * Sets the URL specific options such as host to use and timeout.  It might also
 * set an error code, in which case the further processing should react on.
 */
void DownloadManager::SetUrlOptions(JobInfo *info) {
  CURL *curl_handle = info->curl_handle;
  string url_prefix;

  MutexLockGuard m(lock_options_);
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
    info->proxy = "DIRECT";
    curl_easy_setopt(info->curl_handle, CURLOPT_PROXY, "");
  } else {
    ProxyInfo proxy = (*opt_proxy_groups_)[opt_proxy_groups_current_][0];
    ValidateProxyIpsUnlocked(proxy.url, proxy.host);
    ProxyInfo *proxy_ptr =
      &((*opt_proxy_groups_)[opt_proxy_groups_current_][0]);
    info->proxy = proxy_ptr->url;
    if (proxy_ptr->host.status() == dns::kFailOk) {
      curl_easy_setopt(info->curl_handle, CURLOPT_PROXY, info->proxy.c_str());
    } else {
      // We know it can't work, don't even try to download
      curl_easy_setopt(info->curl_handle, CURLOPT_PROXY, "0.0.0.0");
    }
  }
  curl_easy_setopt(curl_handle, CURLOPT_LOW_SPEED_LIMIT, opt_low_speed_limit_);
  if (info->proxy != "DIRECT") {
    curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, opt_timeout_proxy_);
    curl_easy_setopt(curl_handle, CURLOPT_LOW_SPEED_TIME, opt_timeout_proxy_);
  } else {
    curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, opt_timeout_direct_);
    curl_easy_setopt(curl_handle, CURLOPT_LOW_SPEED_TIME, opt_timeout_direct_);
  }
  if (!opt_dns_server_.empty())
    curl_easy_setopt(curl_handle, CURLOPT_DNS_SERVERS, opt_dns_server_.c_str());

  if (info->probe_hosts && opt_host_chain_) {
    url_prefix = (*opt_host_chain_)[opt_host_chain_current_];
    info->current_host_chain_index = opt_host_chain_current_;
  }

  string url = url_prefix + *(info->url);

  curl_easy_setopt(curl_handle, CURLOPT_SSL_VERIFYPEER, 1L);
  if (url.substr(0, 5) == "https") {
    const char *cadir = getenv("X509_CERT_DIR");
    if (!cadir || !*cadir) {cadir = "/etc/grid-security/certificates";}
    curl_easy_setopt(curl_handle, CURLOPT_CAPATH, cadir);
    const char *cabundle = getenv("X509_CERT_BUNDLE");
    if (cabundle && *cabundle) {
      curl_easy_setopt(curl_handle, CURLOPT_CAINFO, cabundle);
    }
    if (info->pid != -1) {
      if (credentials_attachment_ == NULL) {
        LogCvmfs(kLogDownload, kLogDebug,
                 "uses secure downloads but no credentials attachment set");
      } else {
        bool retval = credentials_attachment_->ConfigureCurlHandle(
          curl_handle, info->pid, &info->cred_data);
        if (!retval) {
          LogCvmfs(kLogDownload, kLogDebug, "failed attaching credentials");
        }
      }
    }
    // The download manager disables signal handling in the curl library;
    // as OpenSSL's implementation of TLS will generate a sigpipe in some
    // error paths, we must explicitly disable SIGPIPE here.
    // TODO(jblomer): it should be enough to do this once
    signal(SIGPIPE, SIG_IGN);
  }

  if (url.find("@proxy@") != string::npos) {
    // This is used in Geo-API requests (only), to replace a portion of the
    // URL with the current proxy name for the sake of caching the result.
    // Replace the @proxy@ either with a passed in "forced" template (which
    // is set from $CVMFS_PROXY_TEMPLATE) if there is one, or a "direct"
    // template (which is the uuid) if there's no proxy, or the name of the
    // proxy.
    string replacement;
    if (proxy_template_forced_ != "") {
      replacement = proxy_template_forced_;
    } else if (info->proxy == "DIRECT") {
      replacement = proxy_template_direct_;
    } else {
      if (opt_proxy_groups_current_ >= opt_proxy_groups_fallback_) {
        // It doesn't make sense to use the fallback proxies in Geo-API requests
        // since the fallback proxies are supposed to get sorted, too.
        info->proxy = "DIRECT";
        curl_easy_setopt(info->curl_handle, CURLOPT_PROXY, "");
        replacement = proxy_template_direct_;
      } else {
        replacement =
          (*opt_proxy_groups_)[opt_proxy_groups_current_][0].host.name();
      }
    }
    replacement = (replacement == "") ? proxy_template_direct_ : replacement;
    LogCvmfs(kLogDownload, kLogDebug, "replacing @proxy@ by %s",
             replacement.c_str());
    url = ReplaceAll(url, "@proxy@", replacement);
  }

  if ((info->destination == kDestinationMem) &&
      (info->destination_mem.size == 0) &&
      HasPrefix(url, "file://", false))
  {
    info->destination_mem.size = 64*1024;
    info->destination_mem.data = static_cast<char *>(smalloc(64*1024));
  }

  curl_easy_setopt(curl_handle, CURLOPT_URL, EscapeUrl(url).c_str());
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
  dns::Host new_host = resolver_->Resolve(host.name());

  bool update_only = true;  // No changes to the list of IP addresses.
  if (new_host.status() != dns::kFailOk) {
    // Try again later in case resolving fails.
    LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
             "failed to resolve IP addresses for %s (%d - %s)",
             host.name().c_str(), new_host.status(),
             dns::Code2Ascii(new_host.status()));
    new_host = dns::Host::ExtendDeadline(host, resolver_->min_ttl());
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
  opt_num_proxies_ -= group->size();
  for (unsigned i = 0; i < group->size(); ) {
    if ((*group)[i].host.id() == host.id()) {
      group->erase(group->begin() + i);
    } else {
      i++;
    }
  }
  vector<ProxyInfo> new_infos;
  set<string> best_addresses = new_host.ViewBestAddresses(opt_ip_preference_);
  set<string>::const_iterator iter_ips = best_addresses.begin();
  for (; iter_ips != best_addresses.end(); ++iter_ips) {
    string url_ip = dns::RewriteUrl(url, *iter_ips);
    new_infos.push_back(ProxyInfo(new_host, url_ip));
  }
  group->insert(group->end(), new_infos.begin(), new_infos.end());
  opt_num_proxies_ += new_infos.size();

  RebalanceProxiesUnlocked();
}


/**
 * Adds transfer time and downloaded bytes to the global counters.
 */
void DownloadManager::UpdateStatistics(CURL *handle) {
  double val;
  int retval;
  int64_t sum = 0;

  retval = curl_easy_getinfo(handle, CURLINFO_SIZE_DOWNLOAD, &val);
  assert(retval == CURLE_OK);
  sum += static_cast<int64_t>(val);
  /*retval = curl_easy_getinfo(handle, CURLINFO_HEADER_SIZE, &val);
  assert(retval == CURLE_OK);
  sum += static_cast<int64_t>(val);*/
  perf::Xadd(counters_->sz_transferred_bytes, sum);
}


/**
 * Retry if possible if not on no-cache and if not already done too often.
 */
bool DownloadManager::CanRetry(const JobInfo *info) {
  MutexLockGuard m(lock_options_);
  unsigned max_retries = opt_max_retries_;

  return !info->nocache && (info->num_retries < max_retries) &&
         (IsProxyTransferError(info->error_code) ||
          IsHostTransferError(info->error_code));
}

/**
 * Backoff for retry to introduce a jitter into a cluster of requesting
 * cvmfs nodes.
 * Retry only when HTTP caching is on.
 *
 * \return true if backoff has been performed, false otherwise
 */
void DownloadManager::Backoff(JobInfo *info) {
  unsigned backoff_init_ms = 0;
  unsigned backoff_max_ms = 0;
  {
    MutexLockGuard m(lock_options_);
    backoff_init_ms = opt_backoff_init_ms_;
    backoff_max_ms = opt_backoff_max_ms_;
  }

  info->num_retries++;
  perf::Inc(counters_->n_retries);
  if (info->backoff_ms == 0) {
    info->backoff_ms = prng_.Next(backoff_init_ms + 1);  // Must be != 0
  } else {
    info->backoff_ms *= 2;
  }
  if (info->backoff_ms > backoff_max_ms) info->backoff_ms = backoff_max_ms;

  LogCvmfs(kLogDownload, kLogDebug, "backing off for %d ms", info->backoff_ms);
  SafeSleepMs(info->backoff_ms);
}

void DownloadManager::SetNocache(JobInfo *info) {
  if (info->nocache)
    return;
  header_lists_->AppendHeader(info->headers, "Pragma: no-cache");
  header_lists_->AppendHeader(info->headers, "Cache-Control: no-cache");
  curl_easy_setopt(info->curl_handle, CURLOPT_HTTPHEADER, info->headers);
  info->nocache = true;
}


/**
 * Reverse operation of SetNocache. Makes sure that "no-cache" header
 * disappears from the list of headers to let proxies work normally.
 */
void DownloadManager::SetRegularCache(JobInfo *info) {
  if (info->nocache == false)
    return;
  header_lists_->CutHeader("Pragma: no-cache", &(info->headers));
  header_lists_->CutHeader("Cache-Control: no-cache", &(info->headers));
  curl_easy_setopt(info->curl_handle, CURLOPT_HTTPHEADER, info->headers);
  info->nocache = false;
}


/**
 * Frees the storage associated with the authz attachment from the job
 */
void DownloadManager::ReleaseCredential(JobInfo *info) {
  if (info->cred_data) {
    assert(credentials_attachment_ != NULL);  // Someone must have set it
    credentials_attachment_->ReleaseCurlHandle(info->curl_handle,
                                               info->cred_data);
    info->cred_data = NULL;
  }
}


/**
 * Checks the result of a curl download and implements the failure logic, such
 * as changing the proxy server.  Takes care of cleanup.
 *
 * \return true if another download should be performed, false otherwise
 */
bool DownloadManager::VerifyAndFinalize(const int curl_error, JobInfo *info) {
  LogCvmfs(kLogDownload, kLogDebug,
           "Verify downloaded url %s, proxy %s (curl error %d)",
           info->url->c_str(), info->proxy.c_str(), curl_error);
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
                                              info->destination_mem.pos,
                                              &buf, &size);
        if (retval) {
          free(info->destination_mem.data);
          info->destination_mem.data = static_cast<char *>(buf);
          info->destination_mem.pos = info->destination_mem.size = size;
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
      info->error_code = kFailUnsupportedProtocol;
      break;
    case CURLE_URL_MALFORMAT:
      info->error_code = kFailBadUrl;
      break;
    case CURLE_COULDNT_RESOLVE_PROXY:
      info->error_code = kFailProxyResolve;
      break;
    case CURLE_COULDNT_RESOLVE_HOST:
      info->error_code = kFailHostResolve;
      break;
    case CURLE_OPERATION_TIMEDOUT:
      info->error_code = (info->proxy == "DIRECT") ?
                         kFailHostTooSlow : kFailProxyTooSlow;
      break;
    case CURLE_PARTIAL_FILE:
    case CURLE_GOT_NOTHING:
    case CURLE_RECV_ERROR:
      info->error_code = (info->proxy == "DIRECT") ?
                         kFailHostShortTransfer : kFailProxyShortTransfer;
      break;
    case CURLE_FILE_COULDNT_READ_FILE:
    case CURLE_COULDNT_CONNECT:
      if (info->proxy != "DIRECT")
        // This is a guess.  Fail-over can still change to switching host
        info->error_code = kFailProxyConnection;
      else
        info->error_code = kFailHostConnection;
      break;
    case CURLE_TOO_MANY_REDIRECTS:
      info->error_code = kFailHostConnection;
      break;
    case CURLE_SSL_CACERT_BADFILE:
      LogCvmfs(kLogDownload, kLogDebug | kLogSyslogErr,
               "Failed to load certificate bundle. "
               "X509_CERT_BUNDLE might point to the wrong location.");
      info->error_code = kFailHostConnection;
      break;
    // As of curl 7.62.0, CURLE_SSL_CACERT is the same as
    // CURLE_PEER_FAILED_VERIFICATION
    case CURLE_PEER_FAILED_VERIFICATION:
      LogCvmfs(kLogDownload, kLogDebug | kLogSyslogErr,
               "invalid SSL certificate of remote host. "
               "X509_CERT_DIR and/or X509_CERT_BUNDLE might point to the wrong "
               "location.");
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

  std::vector<std::string> *host_chain = opt_host_chain_;

  // Determination if download should be repeated
  bool try_again = false;
  bool same_url_retry = CanRetry(info);
  if (info->error_code != kFailOk) {
    MutexLockGuard m(lock_options_);
    if (info->error_code == kFailBadData) {
      if (!info->nocache) {
        try_again = true;
      } else {
        // Make it a host failure
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
                 "data corruption with no-cache header, try another host");

        info->error_code = kFailHostHttp;
      }
    }
    if ( same_url_retry || (
         ( (info->error_code == kFailHostResolve) ||
           IsHostTransferError(info->error_code) ||
           (info->error_code == kFailHostHttp)) &&
         info->probe_hosts &&
         host_chain && (info->num_used_hosts < host_chain->size()))
       )
    {
      try_again = true;
    }
    if ( same_url_retry || (
         ( (info->error_code == kFailProxyResolve) ||
           IsProxyTransferError(info->error_code) ||
           (info->error_code == kFailProxyHttp)) )
       )
    {
      try_again = true;
      // If all proxies failed, do a next round with the next host
      if (!same_url_retry && (info->num_used_proxies >= opt_num_proxies_)) {
        // Check if this can be made a host fail-over
        if (info->probe_hosts &&
            host_chain &&
            (info->num_used_hosts < host_chain->size()))
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
          LogCvmfs(kLogDownload, kLogDebug, "make it a host failure");
          info->num_used_proxies = 1;
          info->error_code = kFailHostAfterProxy;
        } else {
          try_again = false;
        }
      }  // Make a proxy failure a host failure
    }  // Proxy failure assumed
  }

  if (try_again) {
    LogCvmfs(kLogDownload, kLogDebug, "Trying again on same curl handle, "
             "same url: %d, error code %d", same_url_retry, info->error_code);
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
      rewind(info->destination_file);
    }
    if (info->destination == kDestinationSink) {
      if (info->destination_sink->Reset() != 0) {
        info->error_code = kFailLocalIO;
        goto verify_and_finalize_stop;
      }
    }
    if (info->expected_hash)
      shash::Init(info->hash_context);
    if (info->compressed)
      zlib::DecompressInit(&info->zstream);
    SetRegularCache(info);

    // Failure handling
    bool switch_proxy = false;
    bool switch_host = false;
    switch (info->error_code) {
      case kFailBadData:
        SetNocache(info);
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
      default:
        if (IsProxyTransferError(info->error_code)) {
          if (same_url_retry)
            Backoff(info);
          else
            switch_proxy = true;
        } else if (IsHostTransferError(info->error_code)) {
          if (same_url_retry)
            Backoff(info);
          else
            switch_host = true;
        } else {
          // No other errors expected when retrying
          PANIC(NULL);
        }
    }
    if (switch_proxy) {
      ReleaseCredential(info);
      SwitchProxy(info);
      info->num_used_proxies++;
      SetUrlOptions(info);
    }
    if (switch_host) {
      ReleaseCredential(info);
      SwitchHost(info);
      info->num_used_hosts++;
      SetUrlOptions(info);
    }

    return true;  // try again
  }

 verify_and_finalize_stop:
  // Finalize, flush destination file
  ReleaseCredential(info);
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

  opt_dns_server_ = "";
  opt_ip_preference_ = dns::kIpPreferSystem;
  opt_timeout_proxy_ = 0;
  opt_timeout_direct_ = 0;
  opt_low_speed_limit_ = 0;
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
  follow_redirects_ = false;
  use_system_proxy_ = false;

  resolver_ = NULL;

  opt_timestamp_backup_proxies_ = 0;
  opt_timestamp_failover_proxies_ = 0;
  opt_proxy_groups_reset_after_ = 0;
  opt_timestamp_backup_host_ = 0;
  opt_host_reset_after_ = 0;

  credentials_attachment_ = NULL;

  counters_ = NULL;
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
                           const bool use_system_proxy,
                           perf::StatisticsTemplate statistics)
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
  opt_low_speed_limit_ = 1024;
  opt_proxy_groups_current_ = 0;
  opt_proxy_groups_current_burned_ = 0;
  opt_num_proxies_ = 0;
  opt_host_chain_current_ = 0;
  opt_ip_preference_ = dns::kIpPreferSystem;

  counters_ = new Counters(statistics);

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

  prng_.InitLocaltime();

  // Name resolving
  if ((getenv("CVMFS_IPV4_ONLY") != NULL) &&
      (strlen(getenv("CVMFS_IPV4_ONLY")) > 0))
  {
    opt_ipv4_only_ = true;
  }
  resolver_ = dns::NormalResolver::Create(opt_ipv4_only_,
    kDnsDefaultRetries, kDnsDefaultTimeoutMs);
  assert(resolver_);

  // Parsing environment variables
  if (use_system_proxy) {
    use_system_proxy_ = true;
    if (getenv("http_proxy") == NULL) {
      SetProxyChain("", "", kSetProxyRegular);
    } else {
      SetProxyChain(string(getenv("http_proxy")), "", kSetProxyRegular);
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

  delete counters_;
  counters_ = NULL;

  delete opt_host_chain_;
  delete opt_host_chain_rtt_;
  delete opt_proxy_groups_;
  opt_host_chain_ = NULL;
  opt_host_chain_rtt_ = NULL;
  opt_proxy_groups_ = NULL;

  curl_global_cleanup();

  delete resolver_;
  resolver_ = NULL;
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

    // LogCvmfs(kLogDownload, kLogDebug, "send job to thread, pipe %d %d",
    //          info->wait_at[0], info->wait_at[1]);
    WritePipe(pipe_jobs_[1], &info, sizeof(info));
    ReadPipe(info->wait_at[0], &result, sizeof(result));
    // LogCvmfs(kLogDownload, kLogDebug, "got result %d", result);
  } else {
    MutexLockGuard l(lock_synchronous_mode_);
    CURL *handle = AcquireCurlHandle();
    InitializeRequest(info, handle);
    SetUrlOptions(info);
    // curl_easy_setopt(handle, CURLOPT_VERBOSE, 1);
    int retval;
    do {
      retval = curl_easy_perform(handle);
      perf::Inc(counters_->n_requests);
      double elapsed;
      if (curl_easy_getinfo(handle, CURLINFO_TOTAL_TIME, &elapsed) == CURLE_OK)
        perf::Xadd(counters_->sz_transfer_time, (int64_t)(elapsed * 1000));
    } while (VerifyAndFinalize(retval, info));
    result = info->error_code;
    ReleaseCurlHandle(info->curl_handle);
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
 * Used by the client to connect the authz session manager to the download
 * manager.
 */
void DownloadManager::SetCredentialsAttachment(CredentialsAttachment *ca) {
  MutexLockGuard m(lock_options_);
  credentials_attachment_ = ca;
}

/**
 * Gets the DNS sever.
 */
std::string DownloadManager::GetDnsServer() const {
  return opt_dns_server_;
}

/**
 * Sets a DNS server.  Only for testing as it cannot be reverted to the system
 * default.
 */
void DownloadManager::SetDnsServer(const string &address) {
  if (!address.empty()) {
    MutexLockGuard m(lock_options_);
    opt_dns_server_ = address;
    assert(!opt_dns_server_.empty());

    vector<string> servers;
    servers.push_back(address);
    bool retval = resolver_->SetResolvers(servers);
    assert(retval);
  }
  LogCvmfs(kLogDownload, kLogSyslog, "set nameserver to %s", address.c_str());
}


/**
 * Sets the DNS query timeout parameters.
 */
void DownloadManager::SetDnsParameters(
  const unsigned retries,
  const unsigned timeout_ms)
{
  MutexLockGuard m(lock_options_);
  if ((resolver_->retries() == retries) &&
      (resolver_->timeout_ms() == timeout_ms))
  {
    return;
  }
  delete resolver_;
  resolver_ = NULL;
  resolver_ =
    dns::NormalResolver::Create(opt_ipv4_only_, retries, timeout_ms);
  assert(resolver_);
}


void DownloadManager::SetDnsTtlLimits(
  const unsigned min_seconds,
  const unsigned max_seconds)
{
  MutexLockGuard m(lock_options_);
  resolver_->set_min_ttl(min_seconds);
  resolver_->set_max_ttl(max_seconds);
}


void DownloadManager::SetIpPreference(dns::IpPreference preference) {
  MutexLockGuard m(lock_options_);
  opt_ip_preference_ = preference;
}


/**
 * Sets two timeout values for proxied and for direct conections, respectively.
 * The timeout counts for all sorts of connection phases,
 * DNS, HTTP connect, etc.
 */
void DownloadManager::SetTimeout(const unsigned seconds_proxy,
                                 const unsigned seconds_direct)
{
  MutexLockGuard m(lock_options_);
  opt_timeout_proxy_ = seconds_proxy;
  opt_timeout_direct_ = seconds_direct;
}


/**
 * Sets contains the average transfer speed in bytes per second that the
 * transfer should be below during CURLOPT_LOW_SPEED_TIME seconds for libcurl to
 * consider it to be too slow and abort.  Only effective for new connections.
 */
void DownloadManager::SetLowSpeedLimit(const unsigned low_speed_limit) {
  MutexLockGuard m(lock_options_);
  opt_low_speed_limit_ = low_speed_limit;
}


/**
 * Receives the currently active timeout values.
 */
void DownloadManager::GetTimeout(unsigned *seconds_proxy,
                                 unsigned *seconds_direct)
{
  MutexLockGuard m(lock_options_);
  *seconds_proxy = opt_timeout_proxy_;
  *seconds_direct = opt_timeout_direct_;
}


/**
 * Parses a list of ';'-separated hosts for the host chain.  The empty string
 * removes the host list.
 */
void DownloadManager::SetHostChain(const string &host_list) {
  SetHostChain(SplitString(host_list, ';'));
}


void DownloadManager::SetHostChain(const std::vector<std::string> &host_list) {
  MutexLockGuard m(lock_options_);
  opt_timestamp_backup_host_ = 0;
  delete opt_host_chain_;
  delete opt_host_chain_rtt_;
  opt_host_chain_current_ = 0;

  if (host_list.empty()) {
    opt_host_chain_ = NULL;
    opt_host_chain_rtt_ = NULL;
    return;
  }

  opt_host_chain_ = new vector<string>(host_list);
  opt_host_chain_rtt_ =
    new vector<int>(opt_host_chain_->size(), kProbeUnprobed);
  // LogCvmfs(kLogDownload, kLogSyslog, "using host %s",
  //          (*opt_host_chain_)[0].c_str());
}



/**
 * Retrieves the currently set chain of hosts, their round trip times, and the
 * currently used host.
 */
void DownloadManager::GetHostInfo(vector<string> *host_chain, vector<int> *rtt,
                                  unsigned *current_host)
{
  MutexLockGuard m(lock_options_);
  if (opt_host_chain_) {
    if (current_host) {*current_host = opt_host_chain_current_;}
    if (host_chain) {*host_chain = *opt_host_chain_;}
    if (rtt) {*rtt = *opt_host_chain_rtt_;}
  }
}


/**
 * Jumps to the next proxy in the ring of forward proxy servers.
 * Selects one randomly from a load-balancing group.
 *
 * If info is set, switch only if the current proxy is identical to the one used
 * by info, otherwise another transfer has already done the switch.
 */
void DownloadManager::SwitchProxy(JobInfo *info) {
  MutexLockGuard m(lock_options_);

  if (!opt_proxy_groups_) {
    return;
  }
  if (info &&
      ((*opt_proxy_groups_)[opt_proxy_groups_current_][0].url != info->proxy))
  {
    return;
  }

  perf::Inc(counters_->n_proxy_failover);
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
          // LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
          //          "switched to (another) backup proxy group");
        } else {
          opt_timestamp_backup_proxies_ = 0;
          // LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
          //          "switched back to primary proxy group");
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
}


/**
 * Switches to the next host in the chain.  If info is set, switch only if the
 * current host is identical to the one used by info, otherwise another transfer
 * has already done the switch.
 */
void DownloadManager::SwitchHost(JobInfo *info) {
  MutexLockGuard m(lock_options_);

  if (!opt_host_chain_ || (opt_host_chain_->size() == 1)) {
    return;
  }

  if (info && (info->current_host_chain_index != opt_host_chain_current_)) {
    LogCvmfs(kLogDownload, kLogDebug,
             "don't switch host, "
             "last used host: %s, current host: %s",
             (*opt_host_chain_)[info->current_host_chain_index].c_str(),
             (*opt_host_chain_)[opt_host_chain_current_].c_str());
    return;
  }

  string reason = "manually triggered";
  if (info) {
    reason = download::Code2Ascii(info->error_code);
  }

  string old_host = (*opt_host_chain_)[opt_host_chain_current_];
  opt_host_chain_current_ =
      (opt_host_chain_current_ + 1) % opt_host_chain_->size();
  perf::Inc(counters_->n_host_failover);
  LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
           "switching host from %s to %s (%s)", old_host.c_str(),
           (*opt_host_chain_)[opt_host_chain_current_].c_str(),
           reason.c_str());

  // Remember the timestamp of switching to backup host
  if (opt_host_reset_after_ > 0) {
    if (opt_host_chain_current_ != 0) {
      if (opt_timestamp_backup_host_ == 0)
        opt_timestamp_backup_host_ = time(NULL);
    } else {
      opt_timestamp_backup_host_ = 0;
    }
  }
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
        host_rtt[i] = static_cast<int>(
          DiffTimeSeconds(tv_start, tv_end) * 1000);
        LogCvmfs(kLogDownload, kLogDebug, "probing host %s had %dms rtt",
                 url.c_str(), host_rtt[i]);
      } else {
        LogCvmfs(kLogDownload, kLogDebug, "error while probing host %s: %d %s",
                 url.c_str(), result, Code2Ascii(result));
        host_rtt[i] = INT_MAX;
      }
    }
  }

  SortTeam(&host_rtt, &host_chain);
  for (i = 0; i < host_chain.size(); ++i) {
    if (host_rtt[i] == INT_MAX) host_rtt[i] = kProbeDown;
  }

  MutexLockGuard m(lock_options_);
  delete opt_host_chain_;
  delete opt_host_chain_rtt_;
  opt_host_chain_ = new vector<string>(host_chain);
  opt_host_chain_rtt_ = new vector<int>(host_rtt);
  opt_host_chain_current_ = 0;
}

bool DownloadManager::GeoSortServers(std::vector<std::string> *servers,
                    std::vector<uint64_t> *output_order) {
  if (!servers) {return false;}
  if (servers->size() == 1) {
    if (output_order) {
      output_order->clear();
      output_order->push_back(0);
    }
    return true;
  }

  std::vector<std::string> host_chain;
  GetHostInfo(&host_chain, NULL, NULL);

  std::vector<std::string> server_dns_names;
  server_dns_names.reserve(servers->size());
  for (unsigned i = 0; i < servers->size(); ++i) {
    std::string host = dns::ExtractHost((*servers)[i]);
    server_dns_names.push_back(host.empty() ? (*servers)[i] : host);
  }
  std::string host_list = JoinStrings(server_dns_names, ",");

  vector<string> host_chain_shuffled;
  {
    // Protect against concurrent access to prng_
    MutexLockGuard m(lock_options_);
    // Determine random hosts for the Geo-API query
    host_chain_shuffled = Shuffle(host_chain, &prng_);
  }
  // Request ordered list via Geo-API
  bool success = false;
  unsigned max_attempts = std::min(host_chain_shuffled.size(), size_t(3));
  vector<uint64_t> geo_order(servers->size());
  for (unsigned i = 0; i < max_attempts; ++i) {
    string url = host_chain_shuffled[i] + "/api/v1.0/geo/@proxy@/" + host_list;
    LogCvmfs(kLogDownload, kLogDebug,
             "requesting ordered server list from %s", url.c_str());
    JobInfo info(&url, false, false, NULL);
    Failures result = Fetch(&info);
    if (result == kFailOk) {
      string order(info.destination_mem.data, info.destination_mem.size);
      free(info.destination_mem.data);
      bool retval = ValidateGeoReply(order, servers->size(), &geo_order);
      if (!retval) {
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
                 "retrieved invalid GeoAPI reply from %s [%s]",
                 url.c_str(), order.c_str());
      } else {
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslog,
                 "geographic order of servers retrieved from %s",
                 dns::ExtractHost(host_chain_shuffled[i]).c_str());
        LogCvmfs(kLogDownload, kLogDebug, "order is %s", order.c_str());
        success = true;
        break;
      }
    } else {
      LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
               "GeoAPI request %s failed with error %d [%s]",
               url.c_str(), result, Code2Ascii(result));
    }
  }
  if (!success) {
    LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
             "failed to retrieve geographic order from stratum 1 servers");
    return false;
  }

  if (output_order) {
    output_order->swap(geo_order);
  } else {
    std::vector<std::string> sorted_servers;
    sorted_servers.reserve(geo_order.size());
    for (unsigned i = 0; i < geo_order.size(); ++i) {
      uint64_t orderval = geo_order[i];
      sorted_servers.push_back((*servers)[orderval]);
    }
    servers->swap(sorted_servers);
  }
  return true;
}


/**
 * Uses the Geo-API of Stratum 1s to let any of them order the list of servers
 *   and fallback proxies (if any).
 * Tries at most three random Stratum 1s before giving up.
 * If you change the host list in between by SetHostChain() or the fallback
 *   proxy list by SetProxyChain(), they will be overwritten by this function.
 */
bool DownloadManager::ProbeGeo() {
  vector<string> host_chain;
  vector<int> host_rtt;
  unsigned current_host;
  vector< vector<ProxyInfo> > proxy_chain;
  unsigned fallback_group;

  GetHostInfo(&host_chain, &host_rtt, &current_host);
  GetProxyInfo(&proxy_chain, NULL, &fallback_group);
  if ((host_chain.size() < 2) && ((proxy_chain.size() - fallback_group) < 2))
    return true;

  vector<string> host_names;
  for (unsigned i = 0; i < host_chain.size(); ++i)
    host_names.push_back(dns::ExtractHost(host_chain[i]));
  SortTeam(&host_names, &host_chain);
  unsigned last_geo_host = host_names.size();

  if ((fallback_group == 0) && (last_geo_host > 1)) {
    // There are no non-fallback proxies, which means that the client
    // will always use the fallback proxies.  Add a keyword separator
    // between the hosts and fallback proxies so the geosorting service
    // will know to sort the hosts based on the distance from the
    // closest fallback proxy rather than the distance from the client.
    host_names.push_back("+PXYSEP+");
  }

  // Add fallback proxy names to the end of the host list
  unsigned first_geo_fallback = host_names.size();
  for (unsigned i = fallback_group; i < proxy_chain.size(); ++i) {
    // We only take the first fallback proxy name from every group under the
    // assumption that load-balanced servers are at the same location
    host_names.push_back(proxy_chain[i][0].host.name());
  }

  std::vector<uint64_t> geo_order;
  bool success = GeoSortServers(&host_names, &geo_order);
  if (!success) {
    // GeoSortServers already logged a failure message.
    return false;
  }

  // Re-install host chain and proxy chain
  MutexLockGuard m(lock_options_);
  delete opt_host_chain_;
  opt_num_proxies_ = 0;
  opt_host_chain_ = new vector<string>(host_chain.size());
  string old_proxy;
  if (opt_proxy_groups_ != NULL) {
    old_proxy = (*opt_proxy_groups_)[opt_proxy_groups_current_][0].url;
  }

  // It's possible that opt_proxy_groups_fallback_ might have changed while
  // the lock wasn't held
  vector<vector<ProxyInfo> > *proxy_groups = new vector<vector<ProxyInfo> >(
      opt_proxy_groups_fallback_ + proxy_chain.size() - fallback_group);
  // First copy the non-fallback part of the current proxy chain
  for (unsigned i = 0; i < opt_proxy_groups_fallback_; ++i) {
    (*proxy_groups)[i] = (*opt_proxy_groups_)[i];
    opt_num_proxies_ += (*opt_proxy_groups_)[i].size();
  }

  // Copy the host chain and fallback proxies by geo order.  Array indices
  // in geo_order that are smaller than last_geo_host refer to a stratum 1,
  // and those indices greater than or equal to first_geo_fallback refer to
  // a fallback proxy.
  unsigned hosti = 0;
  unsigned proxyi = opt_proxy_groups_fallback_;
  for (unsigned i = 0; i < geo_order.size(); ++i) {
    uint64_t orderval = geo_order[i];
    if (orderval < (uint64_t)last_geo_host) {
      // LogCvmfs(kLogCvmfs, kLogSyslog, "this is orderval %u at host index
      // %u", orderval, hosti);
      (*opt_host_chain_)[hosti++] = host_chain[orderval];
    } else if (orderval >= (uint64_t)first_geo_fallback) {
      // LogCvmfs(kLogCvmfs, kLogSyslog,
      // "this is orderval %u at proxy index %u, using proxy_chain index %u",
      // orderval, proxyi, fallback_group + orderval - first_geo_fallback);
      (*proxy_groups)[proxyi] =
          proxy_chain[fallback_group + orderval - first_geo_fallback];
      opt_num_proxies_ += (*proxy_groups)[proxyi].size();
      proxyi++;
    }
  }

  delete opt_proxy_groups_;
  opt_proxy_groups_ = proxy_groups;
  // In pathological cases, opt_proxy_groups_current_ can be larger now when
  // proxies changed in-between.
  if (opt_proxy_groups_current_ > opt_proxy_groups_->size()) {
    if (opt_proxy_groups_->size() == 0) {
      opt_proxy_groups_current_ = 0;
    } else {
      opt_proxy_groups_current_ = opt_proxy_groups_->size() - 1;
    }
    opt_proxy_groups_current_burned_ = 0;
  }

  string new_proxy;
  if (opt_proxy_groups_ != NULL) {
    new_proxy = (*opt_proxy_groups_)[opt_proxy_groups_current_][0].url;
  }
  if (old_proxy != new_proxy) {
    LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
             "switching proxy from %s to %s (geosort)",
             old_proxy.c_str(), new_proxy.c_str());
  }

  delete opt_host_chain_rtt_;
  opt_host_chain_rtt_ = new vector<int>(host_chain.size(), kProbeGeo);
  opt_host_chain_current_ = 0;

  return true;
}


/**
 * Validates a string of the form "1,4,2,3" representing in which order the
 * the expected_size number of hosts should be put for optimal geographic
 * proximity.  Returns false if the reply_order string is invalid, otherwise
 * fills in the reply_vals array with zero-based order indexes (e.g.
 * [0,3,1,2]) and returns true.
 */
bool DownloadManager::ValidateGeoReply(
  const string &reply_order,
  const unsigned expected_size,
  vector<uint64_t> *reply_vals)
{
  if (reply_order.empty())
    return false;
  sanitizer::InputSanitizer sanitizer("09 , \n");
  if (!sanitizer.IsValid(reply_order))
    return false;
  sanitizer::InputSanitizer strip_newline("09 ,");
  vector<string> reply_strings =
    SplitString(strip_newline.Filter(reply_order), ',');
  vector<uint64_t> tmp_vals;
  for (unsigned i = 0; i < reply_strings.size(); ++i) {
    if (reply_strings[i].empty())
      return false;
    tmp_vals.push_back(String2Uint64(reply_strings[i]));
  }
  if (tmp_vals.size() != expected_size)
    return false;

  // Check if tmp_vals contains the number 1..n
  set<uint64_t> coverage(tmp_vals.begin(), tmp_vals.end());
  if (coverage.size() != tmp_vals.size())
    return false;
  if ((*coverage.begin() != 1) || (*coverage.rbegin() != coverage.size()))
    return false;

  for (unsigned i = 0; i < expected_size; ++i) {
    (*reply_vals)[i] = tmp_vals[i] - 1;
  }
  return true;
}


/**
 * Removes DIRECT from a list of ';' and '|' separated proxies.
 * \return true if DIRECT was present, false otherwise
 */
bool DownloadManager::StripDirect(
  const string &proxy_list,
  string *cleaned_list)
{
  assert(cleaned_list);
  if (proxy_list == "") {
    *cleaned_list = "";
    return false;
  }
  bool result = false;

  vector<string> proxy_groups = SplitString(proxy_list, ';');
  vector<string> cleaned_groups;
  for (unsigned i = 0; i < proxy_groups.size(); ++i) {
    vector<string> group = SplitString(proxy_groups[i], '|');
    vector<string> cleaned;
    for (unsigned j = 0; j < group.size(); ++j) {
      if ((group[j] == "DIRECT") || (group[j] == "")) {
        result = true;
      } else {
        cleaned.push_back(group[j]);
      }
    }
    if (!cleaned.empty())
      cleaned_groups.push_back(JoinStrings(cleaned, "|"));
  }

  *cleaned_list = JoinStrings(cleaned_groups, ";");
  return result;
}


/**
 * Parses a list of ';'- and '|'-separated proxy servers and fallback proxy
 *   servers for the proxy groups.
 * The empty string for both removes the proxy chain.
 * The set_mode parameter can be used to set either proxies (leaving fallback
 *   proxies unchanged) or fallback proxies (leaving regular proxies unchanged)
 *   or both.
 */
void DownloadManager::SetProxyChain(
  const string &proxy_list,
  const string &fallback_proxy_list,
  const ProxySetModes set_mode)
{
  MutexLockGuard m(lock_options_);

  opt_timestamp_backup_proxies_ = 0;
  opt_timestamp_failover_proxies_ = 0;
  string set_proxy_list = opt_proxy_list_;
  string set_proxy_fallback_list = opt_proxy_fallback_list_;
  bool contains_direct;
  if ((set_mode == kSetProxyFallback) || (set_mode == kSetProxyBoth)) {
    opt_proxy_fallback_list_ = fallback_proxy_list;
  }
  if ((set_mode == kSetProxyRegular) || (set_mode == kSetProxyBoth)) {
    opt_proxy_list_ = proxy_list;
  }
  contains_direct =
    StripDirect(opt_proxy_fallback_list_, &set_proxy_fallback_list);
  if (contains_direct) {
    LogCvmfs(kLogDownload, kLogSyslogWarn | kLogDebug,
             "fallback proxies do not support DIRECT, removing");
  }
  if (set_proxy_fallback_list == "") {
    set_proxy_list = opt_proxy_list_;
  } else {
    bool contains_direct = StripDirect(opt_proxy_list_, &set_proxy_list);
    if (contains_direct) {
      LogCvmfs(kLogDownload, kLogSyslog | kLogDebug,
               "skipping DIRECT proxy to use fallback proxy");
    }
  }

  // From this point on, use set_proxy_list and set_fallback_proxy_list as
  // effective proxy lists!

  delete opt_proxy_groups_;
  if ((set_proxy_list == "") && (set_proxy_fallback_list == "")) {
    opt_proxy_groups_ = NULL;
    opt_proxy_groups_current_ = 0;
    opt_proxy_groups_current_burned_ = 0;
    opt_proxy_groups_fallback_ = 0;
    opt_num_proxies_ = 0;
    return;
  }

  // Determine number of regular proxy groups (== first fallback proxy group)
  opt_proxy_groups_fallback_ = 0;
  if (set_proxy_list != "") {
    opt_proxy_groups_fallback_ = SplitString(set_proxy_list, ';').size();
  }
  LogCvmfs(kLogDownload, kLogDebug, "first fallback proxy group %u",
           opt_proxy_groups_fallback_);

  // Concatenate regular proxies and fallback proxies, both of which can be
  // empty.
  string all_proxy_list = set_proxy_list;
  if (set_proxy_fallback_list != "") {
    if (all_proxy_list != "")
      all_proxy_list += ";";
    all_proxy_list += set_proxy_fallback_list;
  }
  LogCvmfs(kLogDownload, kLogDebug, "full proxy list %s",
           all_proxy_list.c_str());

  // Resolve server names in provided urls
  vector<string> hostnames;  // All encountered hostnames
  vector<string> proxy_groups;
  if (all_proxy_list != "")
    proxy_groups = SplitString(all_proxy_list, ';');
  for (unsigned i = 0; i < proxy_groups.size(); ++i) {
    vector<string> this_group = SplitString(proxy_groups[i], '|');
    for (unsigned j = 0; j < this_group.size(); ++j) {
      this_group[j] = dns::AddDefaultScheme(this_group[j]);
      // Note: DIRECT strings will be "extracted" to an empty string.
      string hostname = dns::ExtractHost(this_group[j]);
      // Save the hostname.  Leave empty (DIRECT) names so indexes will
      // match later.
      hostnames.push_back(hostname);
    }
  }
  vector<dns::Host> hosts;
  LogCvmfs(kLogDownload, kLogDebug, "resolving %u proxy addresses",
           hostnames.size());
  resolver_->ResolveMany(hostnames, &hosts);

  // Construct opt_proxy_groups_: traverse proxy list in same order and expand
  // names to resolved IP addresses.
  opt_proxy_groups_ = new vector< vector<ProxyInfo> >();
  opt_num_proxies_ = 0;
  unsigned num_proxy = 0;  // Combined i, j counter
  for (unsigned i = 0; i < proxy_groups.size(); ++i) {
    vector<string> this_group = SplitString(proxy_groups[i], '|');
    // Construct ProxyInfo objects from proxy string and DNS resolver result for
    // every proxy in this_group.  One URL can result in multiple ProxyInfo
    // objects, one for each IP address.
    vector<ProxyInfo> infos;
    for (unsigned j = 0; j < this_group.size(); ++j, ++num_proxy) {
      this_group[j] = dns::AddDefaultScheme(this_group[j]);
      if (this_group[j] == "DIRECT") {
        infos.push_back(ProxyInfo("DIRECT"));
        continue;
      }

      if (hosts[num_proxy].status() != dns::kFailOk) {
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
                 "failed to resolve IP addresses for %s (%d - %s)",
                 hosts[num_proxy].name().c_str(), hosts[num_proxy].status(),
                 dns::Code2Ascii(hosts[num_proxy].status()));
        dns::Host failed_host =
          dns::Host::ExtendDeadline(hosts[num_proxy], resolver_->min_ttl());
        infos.push_back(ProxyInfo(failed_host, this_group[j]));
        continue;
      }

      // IPv4 addresses have precedence
      set<string> best_addresses =
        hosts[num_proxy].ViewBestAddresses(opt_ip_preference_);
      set<string>::const_iterator iter_ips = best_addresses.begin();
      for (; iter_ips != best_addresses.end(); ++iter_ips) {
        string url_ip = dns::RewriteUrl(this_group[j], *iter_ips);
        infos.push_back(ProxyInfo(hosts[num_proxy], url_ip));
      }
    }
    opt_proxy_groups_->push_back(infos);
    opt_num_proxies_ += infos.size();
  }
  LogCvmfs(kLogDownload, kLogDebug,
           "installed %u proxies in %u load-balance groups",
           opt_num_proxies_, opt_proxy_groups_->size());
  opt_proxy_groups_current_ = 0;
  opt_proxy_groups_current_burned_ = 1;

  // Select random start proxy from the first group.
  if (opt_proxy_groups_->size() > 0) {
    // Select random start proxy from the first group.
    if ((*opt_proxy_groups_)[0].size() > 1) {
      int random_index = prng_.Next((*opt_proxy_groups_)[0].size());
      swap((*opt_proxy_groups_)[0][0], (*opt_proxy_groups_)[0][random_index]);
    }
    // LogCvmfs(kLogDownload, kLogSyslog, "using proxy %s",
    //          (*opt_proxy_groups_)[0][0].c_str());
  }
}


/**
 * Retrieves the proxy chain, optionally the currently active load-balancing
 *   group, and optionally the index of the first fallback proxy group.
 *   If there are no fallback proxies, the index will equal the size of
 *   the proxy chain.
 */
void DownloadManager::GetProxyInfo(vector< vector<ProxyInfo> > *proxy_chain,
                                   unsigned *current_group,
                                   unsigned *fallback_group)
{
  assert(proxy_chain != NULL);
  MutexLockGuard m(lock_options_);


  if (!opt_proxy_groups_) {
    vector< vector<ProxyInfo> > empty_chain;
    *proxy_chain = empty_chain;
    if (current_group != NULL)
      *current_group = 0;
    if (fallback_group != NULL)
      *fallback_group = 0;
    return;
  }

  *proxy_chain = *opt_proxy_groups_;
  if (current_group != NULL)
    *current_group = opt_proxy_groups_current_;
  if (fallback_group != NULL)
    *fallback_group = opt_proxy_groups_fallback_;
}

string DownloadManager::GetProxyList() {
  return opt_proxy_list_;
}

string DownloadManager::GetFallbackProxyList() {
  return opt_proxy_fallback_list_;
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
  // LogCvmfs(kLogDownload, kLogDebug | kLogSyslog,
  //          "switching proxy from %s to %s (rebalance)",
  //          (*group)[select].c_str(), swap.c_str());
}


void DownloadManager::RebalanceProxies() {
  MutexLockGuard m(lock_options_);
  RebalanceProxiesUnlocked();
}


/**
 * Switches to the next load-balancing group of proxy servers.
 */
void DownloadManager::SwitchProxyGroup() {
  MutexLockGuard m(lock_options_);

  if (!opt_proxy_groups_ || (opt_proxy_groups_->size() < 2)) {
    return;
  }

  // string old_proxy = (*opt_proxy_groups_)[opt_proxy_groups_current_][0];
  opt_proxy_groups_current_ = (opt_proxy_groups_current_ + 1) %
  opt_proxy_groups_->size();
  opt_proxy_groups_current_burned_ = 1;
  opt_timestamp_backup_proxies_ = time(NULL);
  opt_timestamp_failover_proxies_ = 0;
  // LogCvmfs(kLogDownload, kLogDebug | kLogSyslog,
  //          "switching proxy from %s to %s (manual group change)",
  //          old_proxy.c_str(),
  //          (*opt_proxy_groups_)[opt_proxy_groups_current_][0].c_str());
}


void DownloadManager::SetProxyGroupResetDelay(const unsigned seconds) {
  MutexLockGuard m(lock_options_);
  opt_proxy_groups_reset_after_ = seconds;
  if (opt_proxy_groups_reset_after_ == 0) {
    opt_timestamp_backup_proxies_ = 0;
    opt_timestamp_failover_proxies_ = 0;
  }
}


void DownloadManager::SetHostResetDelay(const unsigned seconds)
{
  MutexLockGuard m(lock_options_);
  opt_host_reset_after_ = seconds;
  if (opt_host_reset_after_ == 0)
    opt_timestamp_backup_host_ = 0;
}


void DownloadManager::SetRetryParameters(const unsigned max_retries,
                                         const unsigned backoff_init_ms,
                                         const unsigned backoff_max_ms)
{
  MutexLockGuard m(lock_options_);
  opt_max_retries_ = max_retries;
  opt_backoff_init_ms_ = backoff_init_ms;
  opt_backoff_max_ms_ = backoff_max_ms;
}


void DownloadManager::SetMaxIpaddrPerProxy(unsigned limit) {
  MutexLockGuard m(lock_options_);
  resolver_->set_throttle(limit);
}


void DownloadManager::SetProxyTemplates(
  const std::string &direct,
  const std::string &forced)
{
  MutexLockGuard m(lock_options_);
  proxy_template_direct_ = direct;
  proxy_template_forced_ = forced;
}


void DownloadManager::EnableInfoHeader() {
  enable_info_header_ = true;
}


void DownloadManager::EnableRedirects() {
  follow_redirects_ = true;
}


/**
 * Creates a copy of the existing download manager.  Must only be called in
 * single-threaded stage because it calls curl_global_init().
 */
DownloadManager *DownloadManager::Clone(perf::StatisticsTemplate statistics) {
  DownloadManager *clone = new DownloadManager();
  clone->Init(pool_max_handles_, use_system_proxy_, statistics);
  if (resolver_) {
    clone->SetDnsParameters(resolver_->retries(), resolver_->timeout_ms());
    clone->SetDnsTtlLimits(resolver_->min_ttl(), resolver_->max_ttl());
    clone->SetMaxIpaddrPerProxy(resolver_->throttle());
  }
  if (!opt_dns_server_.empty())
    clone->SetDnsServer(opt_dns_server_);
  clone->opt_timeout_proxy_ = opt_timeout_proxy_;
  clone->opt_timeout_direct_ = opt_timeout_direct_;
  clone->opt_low_speed_limit_ = opt_low_speed_limit_;
  clone->opt_max_retries_ = opt_max_retries_;
  clone->opt_backoff_init_ms_ = opt_backoff_init_ms_;
  clone->opt_backoff_max_ms_ = opt_backoff_max_ms_;
  clone->enable_info_header_ = enable_info_header_;
  clone->follow_redirects_ = follow_redirects_;
  if (opt_host_chain_) {
    clone->opt_host_chain_ = new vector<string>(*opt_host_chain_);
    clone->opt_host_chain_rtt_ = new vector<int>(*opt_host_chain_rtt_);
  }
  CloneProxyConfig(clone);
  clone->opt_ip_preference_ = opt_ip_preference_;
  clone->proxy_template_direct_ = proxy_template_direct_;
  clone->proxy_template_forced_ = proxy_template_forced_;
  clone->opt_proxy_groups_reset_after_ = opt_proxy_groups_reset_after_;
  clone->opt_host_reset_after_ = opt_host_reset_after_;
  clone->credentials_attachment_ = credentials_attachment_;

  return clone;
}


void DownloadManager::CloneProxyConfig(DownloadManager *clone) {
  clone->opt_proxy_groups_current_ = opt_proxy_groups_current_;
  clone->opt_proxy_groups_current_burned_ = opt_proxy_groups_current_burned_;
  clone->opt_proxy_groups_fallback_ = opt_proxy_groups_fallback_;
  clone->opt_num_proxies_ = opt_num_proxies_;
  clone->opt_proxy_list_ = opt_proxy_list_;
  clone->opt_proxy_fallback_list_ = opt_proxy_fallback_list_;
  if (opt_proxy_groups_ == NULL)
    return;

  clone->opt_proxy_groups_ = new vector< vector<ProxyInfo> >(
    *opt_proxy_groups_);
}

}  // namespace download
