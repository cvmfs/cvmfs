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

#include "download.h"

#include <alloca.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <set>
#include <utility>

#include "compression/compression.h"
#include "crypto/hash.h"
#include "duplex_curl.h"  // IWYU pragma: keep
#include "interrupt.h"
#include "network/sink_mem.h"
#include "network/sink_path.h"
#include "sanitizer.h"
#include "ssl.h"
#include "util/algorithm.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/prng.h"
#include "util/smalloc.h"
#include "util/string.h"

using namespace std;  // NOLINT

namespace download {

/**
 * Returns the status if an interrupt happened for a given repository.
 *
 * Used only in case CVMFS_FAILOVER_INDEFINITELY (failover_indefinitely_) is set
 * where failed downloads are retried indefinitely, unless an interrupt occurred
 *
 * @note If you use this functionality you need to change the source code of
 * e.g. cvmfs_config reload to create a sentinel file. See comment below.
 *
 * @return true if an interrupt occurred
 *         false otherwise
 */
bool Interrupted(const std::string &fqrn, JobInfo *info) {
  if (info->allow_failure()) {
    return true;
  }

  if (!fqrn.empty()) {
    // it is up to the user the create this sentinel file ("pause_file") if
    // CVMFS_FAILOVER_INDEFINITELY is used. It must be created during
    // "cvmfs_config reload" and "cvmfs_config reload $fqrn"
    const std::string pause_file = std::string("/var/run/cvmfs/interrupt.")
                                   + fqrn;

    LogCvmfs(kLogDownload, kLogDebug,
             "(id %" PRId64 ") Interrupted(): checking for existence of %s",
             info->id(), pause_file.c_str());
    if (FileExists(pause_file)) {
      LogCvmfs(kLogDownload, kLogDebug,
               "(id %" PRId64 ") Interrupt marker found - "
               "Interrupting current download, this will EIO outstanding IO.",
               info->id());
      if (0 != unlink(pause_file.c_str())) {
        LogCvmfs(kLogDownload, kLogDebug,
                 "(id %" PRId64 ") Couldn't delete interrupt marker: errno=%d",
                 info->id(), errno);
      }
      return true;
    }
  }
  return false;
}

static Failures PrepareDownloadDestination(JobInfo *info) {
  if (info->sink() != NULL && !info->sink()->IsValid()) {
    cvmfs::PathSink *psink = dynamic_cast<cvmfs::PathSink *>(info->sink());
    if (psink != NULL) {
      LogCvmfs(kLogDownload, kLogDebug,
               "(id %" PRId64 ") Failed to open path %s: %s  (errno=%d).",
               info->id(), psink->path().c_str(), strerror(errno), errno);
      return kFailLocalIO;
    } else {
      LogCvmfs(kLogDownload, kLogDebug,
               "(id %" PRId64 ") "
               "Failed to create a valid sink: \n %s",
               info->id(), info->sink()->Describe().c_str());
      return kFailOther;
    }
  }

  return kFailOk;
}


/**
 * Called by curl for every HTTP header. Not called for file:// transfers.
 */
/**
 * True when the TCP connect for this transfer completed, whatever happened
 * afterwards.  It tells a peer that never answered apart from one that
 * answered and then misbehaved, which is the difference between "look
 * elsewhere" and "stay here".
 *
 * CURLINFO_CONNECT_TIME_T arrived in libcurl 7.61.0 but the build accepts
 * 7.55.0 (see externals/libcurl/CMakeLists.txt), so fall back to the
 * double-valued CURLINFO_CONNECT_TIME, which has been there since 7.4.1 and
 * carries the same meaning at lower resolution.
 */
static bool ConnectCompleted(CURL *handle) {
#if LIBCURL_VERSION_NUM >= 0x073d00  // 7.61.0
  curl_off_t connect_time = 0;
  if (curl_easy_getinfo(handle, CURLINFO_CONNECT_TIME_T, &connect_time)
      != CURLE_OK) {
    return false;
  }
  return connect_time > 0;
#else
  double connect_time = 0.0;
  if (curl_easy_getinfo(handle, CURLINFO_CONNECT_TIME, &connect_time)
      != CURLE_OK) {
    return false;
  }
  return connect_time > 0.0;
#endif
}


static size_t CallbackCurlHeader(void *ptr, size_t size, size_t nmemb,
                                 void *info_link) {
  const size_t num_bytes = size * nmemb;
  const string header_line(static_cast<const char *>(ptr), num_bytes);
  JobInfo *info = static_cast<JobInfo *>(info_link);

  // LogCvmfs(kLogDownload, kLogDebug, "REMOVE-ME: Header callback with %s",
  //          header_line.c_str());

  // Check http status codes
  if (HasPrefix(header_line, "HTTP/", false)) {
    // The status code follows the version token.  That token is "1.1" or
    // "1.0", but also a bare "2" or "3", so it has no fixed width: locate the
    // code after the first space rather than at a hard-coded offset.  Assuming
    // "HTTP/1." left http_code at -1 for every HTTP/2 reply, which suppresses
    // the error handling below and the 2xx check that gates resuming.
    const size_t pos_code = header_line.find(' ');
    if (pos_code == string::npos) {
      return 0;
    }

    unsigned i;
    for (i = pos_code; (i < header_line.length()) && (header_line[i] == ' ');
         ++i) {
    }

    // Code is initialized to -1
    if (header_line.length() > i + 2) {
      info->SetHttpCode(DownloadManager::ParseHttpCode(&header_line[i]));
    }

    if ((info->http_code() / 100) == 2) {
      if ((info->resume_offset() > 0) && (info->http_code() != 206)) {
        // Resuming, but the server ignored the Range header.  Abort before
        // any body byte is delivered and let the retry start from scratch.
        LogCvmfs(kLogDownload, kLogDebug,
                 "(id %" PRId64 ") server ignored resume range, restarting",
                 info->id());
        info->SetResumeOffset(0);
        info->SetErrorCode((info->proxy() == "DIRECT")
                               ? kFailHostShortTransfer
                               : kFailProxyShortTransfer);
        return 0;
      }
      return num_bytes;
    } else if ((info->http_code() == 301) || (info->http_code() == 302)
               || (info->http_code() == 303) || (info->http_code() == 307)) {
      if (!info->follow_redirects()) {
        LogCvmfs(kLogDownload, kLogDebug,
                 "(id %" PRId64 ") redirect support not enabled: %s",
                 info->id(), header_line.c_str());
        info->SetErrorCode(kFailHostHttp);
        return 0;
      }
      LogCvmfs(kLogDownload, kLogDebug, "(id %" PRId64 ") http redirect: %s",
               info->id(), header_line.c_str());
      // libcurl will handle this because of CURLOPT_FOLLOWLOCATION
      return num_bytes;
    } else {
      LogCvmfs(kLogDownload, kLogDebug,
               "(id %" PRId64 ") http status error code: %s [%d]", info->id(),
               header_line.c_str(), info->http_code());
      if (((info->http_code() / 100) == 5) || (info->http_code() == 400)
          || (info->http_code() == 404)) {
        // 5XX returned by host
        // 400: error from the GeoAPI module
        // 404: the stratum 1 does not have the newest files
        info->SetErrorCode(kFailHostHttp);
      } else if (info->http_code() == 429) {
        // 429: rate throttling (we ignore the backoff hint for the time being)
        info->SetErrorCode(kFailHostConnection);
      } else {
        info->SetErrorCode((info->proxy() == "DIRECT") ? kFailHostHttp
                                                       : kFailProxyHttp);
      }
      return 0;
    }
  }

  // If needed: allocate space in sink
  if (HasPrefix(header_line, "CONTENT-LENGTH:", true)) {
    char *tmp = reinterpret_cast<char *>(alloca(num_bytes + 1));
    uint64_t length = 0;
    sscanf(header_line.c_str(), "%s %" PRIu64, tmp, &length);
    info->SetContentLength(static_cast<int64_t>(length));
    if (info->sink() == NULL || !info->sink()->RequiresReserve()) {
      // nothing to reserve
    } else if (length > 0) {
      if (!info->sink()->Reserve(length)) {
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslogErr,
                 "(id %" PRId64 ") "
                 "resource %s too large to store in memory (%" PRIu64 ")",
                 info->id(), info->url()->c_str(), length);
        info->SetErrorCode(kFailTooBig);
        return 0;
      }
    } else {
      // Empty resource
      info->sink()->Reserve(0);
    }
  } else if (HasPrefix(header_line, "LOCATION:", true)) {
    // This comes along with redirects
    LogCvmfs(kLogDownload, kLogDebug, "(id %" PRId64 ") %s", info->id(),
             header_line.c_str());
  } else if (HasPrefix(header_line, "LINK:", true)) {
    // This is metalink info
    LogCvmfs(kLogDownload, kLogDebug, "(id %" PRId64 ") %s", info->id(),
             header_line.c_str());
    std::string link = info->link();
    if (link.size() != 0) {
      // multiple LINK headers are allowed
      link = link + ", " + header_line.substr(5);
    } else {
      link = header_line.substr(5);
    }
    info->SetLink(link);
  } else if (HasPrefix(header_line, "X-SQUID-ERROR:", true)) {
    // Reinterpret host error as proxy error
    if (info->error_code() == kFailHostHttp) {
      info->SetErrorCode(kFailProxyHttp);
    }
  } else if (HasPrefix(header_line, "PROXY-STATUS:", true)) {
    // Reinterpret host error as proxy error if applicable
    if ((info->error_code() == kFailHostHttp)
        && (header_line.find("error=") != string::npos)) {
      info->SetErrorCode(kFailProxyHttp);
    }
  }

  return num_bytes;
}


/**
 * Called by curl for every received data chunk.
 *
 * In multi-threaded mode the chunk's hash is updated here (cheap) and the
 * raw bytes are handed off to the calling Fetch() thread via JobInfo's
 * data tube; the (relatively expensive) zlib decompression and sink->Write
 * happen on that caller thread. This lets multiple callers — e.g. main
 * thread plus N file-bundle prefetch workers — run their decompression in
 * parallel instead of serializing on this single MainDownload thread.
 *
 * In single-threaded mode (no MainDownload thread, e.g. unit tests) the
 * old inline path is kept.
 */
static size_t CallbackCurlData(void *ptr, size_t size, size_t nmemb,
                               void *info_link) {
  const size_t num_bytes = size * nmemb;
  JobInfo *info = static_cast<JobInfo *>(info_link);

  // TODO(heretherebedragons) remove if no error comes up
  // as this means only jobinfo data request (and not header only)
  // come here
  assert(info->sink() != NULL);

  if (num_bytes == 0)
    return 0;

  if (info->expected_hash()) {
    shash::Update(reinterpret_cast<unsigned char *>(ptr), num_bytes,
                  info->hash_context());
  }

  if (info->IsValidDataTube()) {
    // Parallel-decompress path: copy bytes onto a buffer the caller will own
    // and enqueue. The caller thread (DownloadManager::Fetch) pops these
    // elements and runs DecompressZStream2Sink / sink->Write itself.
    char *buf = new char[num_bytes];
    memcpy(buf, ptr, num_bytes);
    DataTubeElement *ele = new DataTubeElement(buf, num_bytes,
                                               kActionDecompress);
    info->GetDataTubePtr()->EnqueueBack(ele);
    return num_bytes;
  }

  if (info->compressed()) {
    const zlib::StreamStates retval = zlib::DecompressZStream2Sink(
        ptr, static_cast<int64_t>(num_bytes), info->GetZstreamPtr(),
        info->sink());
    if (retval == zlib::kStreamDataError) {
      LogCvmfs(kLogDownload, kLogSyslogErr,
               "(id %" PRId64 ") failed to decompress %s", info->id(),
               info->url()->c_str());
      info->SetErrorCode(kFailBadData);
      return 0;
    } else if (retval == zlib::kStreamIOError) {
      LogCvmfs(kLogDownload, kLogSyslogErr,
               "(id %" PRId64 ") decompressing %s, local IO error", info->id(),
               info->url()->c_str());
      info->SetErrorCode(kFailLocalIO);
      return 0;
    }
  } else {
    const int64_t written = info->sink()->Write(ptr, num_bytes);
    if (written < 0 || static_cast<uint64_t>(written) != num_bytes) {
      LogCvmfs(kLogDownload, kLogDebug,
               "(id %" PRId64 ") "
               "Failed to perform write of %zu bytes to sink %s with errno %ld",
               info->id(), num_bytes, info->sink()->Describe().c_str(),
               written);
    }
  }

  return num_bytes;
}

#ifdef DEBUGMSG
static int CallbackCurlDebug(CURL *handle,
                             curl_infotype type,
                             char *data,
                             size_t size,
                             void * /* clientp */) {
  JobInfo *info;
  curl_easy_getinfo(handle, CURLINFO_PRIVATE, &info);

  std::string prefix = "(id " + StringifyInt(info->id()) + ") ";
  switch (type) {
    case CURLINFO_TEXT:
      prefix += "{info} ";
      break;
    case CURLINFO_HEADER_IN:
      prefix += "{header/recv} ";
      break;
    case CURLINFO_HEADER_OUT:
      prefix += "{header/sent} ";
      break;
    case CURLINFO_DATA_IN:
      if (size < 50) {
        prefix += "{data/recv} ";
        break;
      } else {
        LogCvmfs(kLogCurl, kLogDebug, "%s{data/recv} <snip>", prefix.c_str());
        return 0;
      }
    case CURLINFO_DATA_OUT:
      if (size < 50) {
        prefix += "{data/sent} ";
        break;
      } else {
        LogCvmfs(kLogCurl, kLogDebug, "%s{data/sent} <snip>", prefix.c_str());
        return 0;
      }
    case CURLINFO_SSL_DATA_IN:
      if (size < 50) {
        prefix += "{ssldata/recv} ";
        break;
      } else {
        LogCvmfs(kLogCurl, kLogDebug, "%s{ssldata/recv} <snip>",
                 prefix.c_str());
        return 0;
      }
    case CURLINFO_SSL_DATA_OUT:
      if (size < 50) {
        prefix += "{ssldata/sent} ";
        break;
      } else {
        LogCvmfs(kLogCurl, kLogDebug, "%s{ssldata/sent} <snip>",
                 prefix.c_str());
        return 0;
      }
    default:
      // just log the message
      break;
  }

  bool valid_char = true;
  std::string msg(data, size);
  for (size_t i = 0; i < msg.length(); ++i) {
    if (msg[i] == '\0') {
      msg[i] = '~';
    }

    // verify that char is a valid printable char
    if ((msg[i] < ' ' || msg[i] > '~')
        && (msg[i] != 10 /*line feed*/
            && msg[i] != 13 /*carriage return*/)) {
      valid_char = false;
    }
  }

  if (!valid_char) {
    msg = "<Non-plaintext sequence>";
  }

  LogCvmfs(kLogCurl, kLogDebug, "%s%s", prefix.c_str(),
           Trim(msg, true /* trim_newline */).c_str());
  return 0;
}
#endif

//------------------------------------------------------------------------------


const int DownloadManager::kProbeUnprobed = -1;
const int DownloadManager::kProbeDown = -2;
const int DownloadManager::kProbeGeo = -3;

/**
 * Escape special chars from the URL, except for ':' and '/',
 * which should keep their meaning.
 */
string DownloadManager::EscapeUrl(const int64_t jobinfo_id, const string &url) {
  string escaped;
  escaped.reserve(url.length());

  char escaped_char[3];
  for (unsigned i = 0, s = url.length(); i < s; ++i) {
    if (JobInfo::EscapeUrlChar(url[i], escaped_char)) {
      escaped.append(escaped_char, 3);
    } else {
      escaped.push_back(escaped_char[0]);
    }
  }
  LogCvmfs(kLogDownload, kLogDebug, "(id %" PRId64 ") escaped %s to %s",
           jobinfo_id, url.c_str(), escaped.c_str());

  return escaped;
}

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
/**
 * libcurl tells us when it next needs curl_multi_socket_action(); MainDownload
 * uses this as its poll timeout instead of waking up every millisecond.
 */
int DownloadManager::CallbackCurlTimer(CURLM * /* multi */,
                                       long timeout_ms,  // NOLINT(runtime/int)
                                       void *userp) {
  DownloadManager *download_mgr = static_cast<DownloadManager *>(userp);
  download_mgr->curl_timeout_ms_ = timeout_ms;
  return 0;
}


int DownloadManager::CallbackCurlSocket(CURL * /* easy */,
                                        curl_socket_t s,
                                        int action,
                                        void *userp,
                                        void * /* socketp */) {
  // LogCvmfs(kLogDownload, kLogDebug, "CallbackCurlSocket called with easy "
  //          "handle %p, socket %d, action %d", easy, s, action);
  DownloadManager *download_mgr = static_cast<DownloadManager *>(userp);
  if (action == CURL_POLL_NONE)
    return 0;

  // Find s in watch_fds_
  unsigned index;

  // TODO(heretherebedragons) why start at index = 0 and not 2?
  // fd[0] and fd[1] are fixed?
  for (index = 0; index < download_mgr->watch_fds_inuse_; ++index) {
    if (download_mgr->watch_fds_[index].fd == s)
      break;
  }
  // Or create newly
  if (index == download_mgr->watch_fds_inuse_) {
    // Extend array if necessary
    if (download_mgr->watch_fds_inuse_ == download_mgr->watch_fds_size_) {
      assert(download_mgr->watch_fds_size_ > 0);
      download_mgr->watch_fds_size_ *= 2;
      download_mgr->watch_fds_ = static_cast<struct pollfd *>(
          srealloc(download_mgr->watch_fds_,
                   download_mgr->watch_fds_size_ * sizeof(struct pollfd)));
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
      download_mgr->watch_fds_[index].events = POLLIN | POLLPRI | POLLOUT
                                               | POLLWRBAND;
      break;
    case CURL_POLL_REMOVE:
      if (index < download_mgr->watch_fds_inuse_ - 1) {
        download_mgr
            ->watch_fds_[index] = download_mgr->watch_fds_
                                      [download_mgr->watch_fds_inuse_ - 1];
      }
      download_mgr->watch_fds_inuse_--;
      // Shrink array if necessary
      // watch_fds_max_ is the floor for the allocation; the test has to be
      // on the array size, not on how much of it is in use.  Comparing
      // watch_fds_inuse_ meant the array only ever shrank while it was still
      // heavily used, so after a burst it stayed at its peak size for good.
      if ((download_mgr->watch_fds_size_ > download_mgr->watch_fds_max_)
          && (download_mgr->watch_fds_inuse_
              < download_mgr->watch_fds_size_ / 2)) {
        download_mgr->watch_fds_size_ /= 2;
        // LogCvmfs(kLogDownload, kLogDebug, "shrinking watch_fds_ (%d)",
        //          watch_fds_size_);
        download_mgr->watch_fds_ = static_cast<struct pollfd *>(
            srealloc(download_mgr->watch_fds_,
                     download_mgr->watch_fds_size_ * sizeof(struct pollfd)));
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
  DownloadManager *download_mgr = static_cast<DownloadManager *>(data);
  LogCvmfs(kLogDownload, kLogDebug,
           "download I/O thread of DownloadManager '%s' started",
           download_mgr->name_.c_str());

  const int kIdxPipeTerminate = 0;
  const int kIdxPipeJobs = 1;

  download_mgr->watch_fds_ = static_cast<struct pollfd *>(
      smalloc(2 * sizeof(struct pollfd)));
  download_mgr->watch_fds_size_ = 2;
  download_mgr->watch_fds_[kIdxPipeTerminate].fd = download_mgr->pipe_terminate_
                                                       ->GetReadFd();
  download_mgr->watch_fds_[kIdxPipeTerminate].events = POLLIN | POLLPRI;
  download_mgr->watch_fds_[kIdxPipeTerminate].revents = 0;
  download_mgr->watch_fds_[kIdxPipeJobs].fd = download_mgr->pipe_jobs_
                                                  ->GetReadFd();
  download_mgr->watch_fds_[kIdxPipeJobs].events = POLLIN | POLLPRI;
  download_mgr->watch_fds_[kIdxPipeJobs].revents = 0;
  download_mgr->watch_fds_inuse_ = 2;

  int still_running = 0;
  struct timeval timeval_start, timeval_stop;
  gettimeofday(&timeval_start, NULL);
  // Retries waiting for their backoff to expire (see Backoff())
  std::vector<JobInfo *> deferred;

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
      // Wake up when libcurl asked for it (timer callback), at most every
      // 100 ms as a safety net, instead of spinning with a 1 ms timeout.
      const long curl_timeout = download_mgr->curl_timeout_ms_;  // NOLINT
      timeout = ((curl_timeout >= 0) && (curl_timeout < 100))
                    ? static_cast<int>(curl_timeout)
                    : 100;
    } else {
      timeout = -1;
      gettimeofday(&timeval_stop, NULL);
      const int64_t delta = static_cast<int64_t>(
          1000 * DiffTimeSeconds(timeval_start, timeval_stop));
      perf::Xadd(download_mgr->counters_->sz_transfer_time, delta);
    }
    // Retries waiting out their backoff must not be slept through here: this
    // is the single I/O thread for every transfer of this manager.  Instead
    // the poll() below is shortened to the earliest moment at which one of
    // them becomes due, so the thread keeps serving other transfers and wakes
    // exactly when there is something to re-issue.
    if (!deferred.empty()) {
      const uint64_t now_ms = platform_monotonic_time_ns() / 1000000;
      uint64_t next_ms = deferred[0]->retry_not_before_ms();
      for (unsigned i = 1; i < deferred.size(); ++i)
        next_ms = std::min(next_ms, deferred[i]->retry_not_before_ms());
      const int wait_ms = (next_ms > now_ms)
                              ? static_cast<int>(next_ms - now_ms)
                              : 0;
      if ((timeout < 0) || (wait_ms < timeout))
        timeout = wait_ms;
    }
    const int retval = poll(download_mgr->watch_fds_,
                            download_mgr->watch_fds_inuse_, timeout);
    if (retval < 0) {
      continue;
    }

    // Handle timeout
    if (retval == 0) {
      curl_multi_socket_action(download_mgr->curl_multi_, CURL_SOCKET_TIMEOUT,
                               0, &still_running);
    }

    // Terminate I/O thread
    if (download_mgr->watch_fds_[kIdxPipeTerminate].revents)
      break;

    // Re-issue retries whose backoff has expired.  This has to stay below
    // the termination check: a job taken out of `deferred` and handed to
    // curl_multi_ is no longer covered by the shutdown cleanup after this
    // loop, so its caller would wait for a result that never comes.
    if (!deferred.empty()) {
      const uint64_t now_ms = platform_monotonic_time_ns() / 1000000;
      for (unsigned i = 0; i < deferred.size();) {
        if (deferred[i]->retry_not_before_ms() <= now_ms) {
          JobInfo *info = deferred[i];
          info->SetRetryNotBeforeMs(0);
          deferred.erase(deferred.begin() + i);
          curl_multi_add_handle(download_mgr->curl_multi_,
                                info->curl_handle());
          curl_multi_socket_action(download_mgr->curl_multi_,
                                   CURL_SOCKET_TIMEOUT, 0, &still_running);
        } else {
          ++i;
        }
      }
    }

    // New job arrives
    if (download_mgr->watch_fds_[kIdxPipeJobs].revents) {
      download_mgr->watch_fds_[kIdxPipeJobs].revents = 0;
      JobInfo *info;
      download_mgr->pipe_jobs_->Read<JobInfo *>(&info);
      if (!still_running) {
        gettimeofday(&timeval_start, NULL);
      }
      CURL *handle = download_mgr->AcquireCurlHandle();
      download_mgr->InitializeRequest(info, handle);
      download_mgr->SetUrlOptions(info);
      curl_multi_add_handle(download_mgr->curl_multi_, handle);
      curl_multi_socket_action(download_mgr->curl_multi_, CURL_SOCKET_TIMEOUT,
                               0, &still_running);
    }

    // Activity on curl sockets
    // Within this loop the curl_multi_socket_action() may cause socket(s)
    // to be removed from watch_fds_. If a socket is removed it is replaced
    // by the socket at the end of the array and the inuse count is decreased.
    // Therefore loop over the array in reverse order.
    for (int64_t i = download_mgr->watch_fds_inuse_ - 1; i >= 2; --i) {
      if (i >= download_mgr->watch_fds_inuse_) {
        continue;
      }
      if (download_mgr->watch_fds_[i].revents) {
        int ev_bitmask = 0;
        if (download_mgr->watch_fds_[i].revents & (POLLIN | POLLPRI))
          ev_bitmask |= CURL_CSELECT_IN;
        if (download_mgr->watch_fds_[i].revents & (POLLOUT | POLLWRBAND))
          ev_bitmask |= CURL_CSELECT_OUT;
        if (download_mgr->watch_fds_[i].revents
            & (POLLERR | POLLHUP | POLLNVAL)) {
          ev_bitmask |= CURL_CSELECT_ERR;
        }
        download_mgr->watch_fds_[i].revents = 0;

        curl_multi_socket_action(download_mgr->curl_multi_,
                                 download_mgr->watch_fds_[i].fd,
                                 ev_bitmask,
                                 &still_running);
      }
    }

    // Check if transfers are completed
    CURLMsg *curl_msg;
    int msgs_in_queue;
    while ((curl_msg = curl_multi_info_read(download_mgr->curl_multi_,
                                            &msgs_in_queue))) {
      if (curl_msg->msg == CURLMSG_DONE) {
        perf::Inc(download_mgr->counters_->n_requests);
        JobInfo *info;
        CURL *easy_handle = curl_msg->easy_handle;
        const int curl_error = curl_msg->data.result;
        curl_easy_getinfo(easy_handle, CURLINFO_PRIVATE, &info);

        int64_t redir_count;
        curl_easy_getinfo(easy_handle, CURLINFO_REDIRECT_COUNT, &redir_count);
        LogCvmfs(kLogDownload, kLogDebug,
                 "(manager '%s' - id %" PRId64 ") "
                 "Number of CURL redirects %" PRId64,
                 download_mgr->name_.c_str(), info->id(), redir_count);

        curl_multi_remove_handle(download_mgr->curl_multi_, easy_handle);
        if (download_mgr->VerifyAndFinalize(curl_error, info)) {
          // Backoff() marked this retry as not-before a given time rather
          // than sleeping.  Park it until then; its easy handle stays out of
          // the multi stack meanwhile and is re-added above when due.
          if (info->retry_not_before_ms() > 0) {
            deferred.push_back(info);
          } else {
            curl_multi_add_handle(download_mgr->curl_multi_, easy_handle);
            curl_multi_socket_action(download_mgr->curl_multi_,
                                     CURL_SOCKET_TIMEOUT,
                                     0,
                                     &still_running);
          }
        } else {
          // Return easy handle into pool and write result back
          download_mgr->ReleaseCurlHandle(easy_handle, true /* allow_reuse */);

          DataTubeElement *ele = new DataTubeElement(kActionStop);
          info->GetDataTubePtr()->EnqueueBack(ele);
          info->GetPipeJobResultPtr()->Write<download::Failures>(
              info->error_code());
        }
      }
    }
  }

  // The thread is terminating, so retries still waiting out their backoff will
  // never be re-issued.  Fail them explicitly rather than leaving their callers
  // blocked forever on a result that is no longer coming.
  for (unsigned i = 0; i < deferred.size(); ++i) {
    JobInfo *info = deferred[i];
    info->SetErrorCode(kFailOther);
    info->GetDataTubePtr()->EnqueueBack(new DataTubeElement(kActionStop));
    info->GetPipeJobResultPtr()->Write<download::Failures>(info->error_code());
  }
  deferred.clear();

  for (set<CURL *>::iterator i = download_mgr->pool_handles_inuse_->begin(),
                             iEnd = download_mgr->pool_handles_inuse_->end();
       i != iEnd;
       ++i) {
    curl_multi_remove_handle(download_mgr->curl_multi_, *i);
    curl_easy_cleanup(*i);
  }
  download_mgr->pool_handles_inuse_->clear();
  free(download_mgr->watch_fds_);

  LogCvmfs(kLogDownload, kLogDebug,
           "download I/O thread of DownloadManager '%s' terminated",
           download_mgr->name_.c_str());
  return NULL;
}


//------------------------------------------------------------------------------


HeaderLists::~HeaderLists() {
  for (unsigned i = 0; i < blocks_.size(); ++i) {
    delete[] blocks_[i];
  }
  blocks_.clear();
}


curl_slist *HeaderLists::GetList(const char *header) { return Get(header); }


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
  blocks_[blocks_.size() - 1][0].data = const_cast<char *>(header);
  return &(blocks_[blocks_.size() - 1][0]);
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
  const int remaining = static_cast<int>(host.deadline())
                        - static_cast<int>(time(NULL));
  string expinfo = (remaining >= 0) ? "+" : "";
  if (abs(remaining) >= 3600) {
    expinfo += StringifyInt(remaining / 3600) + "h";
  } else if (abs(remaining) >= 60) {
    expinfo += StringifyInt(remaining / 60) + "m";
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
/**
 * Upper bound on how long data may stay unacknowledged before the kernel gives
 * up on a connection, and how many keep-alive probes may go unanswered.
 *
 * Keep-alive only governs a connection that is *idle*.  As soon as anything is
 * in flight -- a request, or the FIN that closes the connection -- the
 * retransmit timer takes over, and that is bounded by net.ipv4.tcp_retries2,
 * whose default of 15 works out at roughly a quarter of an hour.  So when a
 * firewall swallows the peer's ACK or RST, the socket does not fail: it sits
 * there retransmitting into the filtered path, stuck in LAST-ACK or holding a
 * request that will never be answered, long after the peer has forgotten it.
 *
 * TCP_USER_TIMEOUT overrides tcp_retries2 for this socket, so an unacknowledged
 * teardown, or an unanswered request, is abandoned in seconds rather than
 * minutes.  It costs nothing on a healthy connection: while the peer keeps
 * acknowledging, the timer never fires, so slow-but-alive transfers are not
 * affected.  TCP_KEEPCNT bounds dead-peer detection in the same spirit;
 * libcurl only gained an option for it in 8.9, so it is set directly here.
 */
static const unsigned kTcpUserTimeoutMs = 60000;
static const int kTcpKeepaliveProbes = 3;


/**
 * Applied by libcurl to every socket it opens for a connection.
 */
static int CallbackCurlSockopt(void * /* clientp */,
                               curl_socket_t curlfd,
                               curlsocktype purpose) {
  if (purpose != CURLSOCKTYPE_IPCXN)
    return CURL_SOCKOPT_OK;
#ifdef TCP_USER_TIMEOUT
  const unsigned user_timeout_ms = kTcpUserTimeoutMs;
  setsockopt(curlfd, IPPROTO_TCP, TCP_USER_TIMEOUT, &user_timeout_ms,
             sizeof(user_timeout_ms));
#endif
#ifdef TCP_KEEPCNT
  const int keepalive_probes = kTcpKeepaliveProbes;
  setsockopt(curlfd, IPPROTO_TCP, TCP_KEEPCNT, &keepalive_probes,
             sizeof(keepalive_probes));
#endif
  return CURL_SOCKOPT_OK;
}


/**
 * Thin wrapper so the unit tests can exercise the socket options that libcurl
 * would otherwise only apply from inside a live connection attempt.
 */
int CallbackCurlSockoptForTest(void *clientp, curl_socket_t curlfd,
                               curlsocktype purpose) {
  return CallbackCurlSockopt(clientp, curlfd, purpose);
}


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
    curl_easy_setopt(handle, CURLOPT_SOCKOPTFUNCTION, CallbackCurlSockopt);
  } else {
    handle = *(pool_handles_idle_->begin());
    pool_handles_idle_->erase(pool_handles_idle_->begin());
  }

  pool_handles_inuse_->insert(handle);

  return handle;
}


void DownloadManager::ReleaseCurlHandle(CURL *handle, bool allow_reuse) {
  const set<CURL *>::iterator elem = pool_handles_inuse_->find(handle);
  assert(elem != pool_handles_inuse_->end());

  if (!allow_reuse || (pool_handles_idle_->size() >= pool_max_handles_)) {
    curl_easy_cleanup(*elem);
  } else {
    pool_handles_idle_->insert(*elem);
  }

  pool_handles_inuse_->erase(elem);
}


/**
 * A short transfer is resumed rather than restarted if at least this many
 * bytes arrived, and such a resume does not count against the retry budget.
 */
static const curl_off_t kMinResumeProgress = 8 * 1024;

/**
 * Idle time after which an established connection starts emitting TCP
 * keep-alive probes, and the longest a connection may sit idle before it is
 * considered unfit for reuse.
 *
 * Both exist for the same reason.  A stateful middlebox -- conntrack, NAT, a
 * DPI box -- drops its record of a flow that has been quiet for a while.  Once
 * that record is gone, the peer's eventual FIN or RST no longer matches an
 * established connection, so a firewall that only admits RELATED,ESTABLISHED
 * discards the teardown packet.  The client is then left holding a socket that
 * is dead but looks open, and only finds out by spending a whole request
 * timeout on it -- which in turn burns the proxy and fails over away from a
 * perfectly healthy one.
 *
 * Probing well inside the usual idle budgets keeps the flow on the books, so
 * the teardown is delivered instead of filtered; refusing to reuse a
 * long-idle connection bounds the damage when it is filtered anyway.
 */
static const unsigned kDefaultTcpKeepaliveSecs = 30;
static const long kMaxIdleConnectionReuseSecs = 30;  // NOLINT


/**
 * Byte range of the request: the caller's range (external files) shifted by
 * the bytes already received in earlier attempts of the same transfer.
 */
static void SetRangeOption(JobInfo *info, CURL *handle) {
  const bool has_range = (info->range_offset() != -1) && (info->range_size());
  if (!has_range && (info->resume_offset() == 0)) {
    curl_easy_setopt(handle, CURLOPT_RANGE, NULL);
    return;
  }
  const int64_t lower = (has_range ? static_cast<int64_t>(info->range_offset())
                                   : 0)
                        + static_cast<int64_t>(info->resume_offset());
  char byte_range_array[100];
  int len;
  if (has_range) {
    const int64_t upper = static_cast<int64_t>(info->range_offset()
                                               + info->range_size() - 1);
    len = snprintf(byte_range_array, sizeof(byte_range_array),
                   "%" PRId64 "-%" PRId64, lower, upper);
  } else {
    len = snprintf(byte_range_array, sizeof(byte_range_array), "%" PRId64 "-",
                   lower);
  }
  if (len >= static_cast<int>(sizeof(byte_range_array))) {
    PANIC(NULL);  // Should be impossible given limits on offset size.
  }
  curl_easy_setopt(handle, CURLOPT_RANGE, byte_range_array);
}


/**
 * HTTP request options: set the URL and other options such as timeout and
 * proxy.
 */
void DownloadManager::InitializeRequest(JobInfo *info, CURL *handle) {
  // Initialize internal download state
  info->SetCurlHandle(handle);
  info->SetErrorCode(kFailOk);
  info->SetHttpCode(-1);
  info->SetFollowRedirects(follow_redirects_);
  info->SetNumUsedProxies(1);
  info->SetNumUsedMetalinks(1);
  info->SetNumUsedHosts(1);
  info->SetNumRetries(0);
  info->SetBackoffMs(0);
  info->SetHeaders(header_lists_->DuplicateList(default_headers_));
  if (info->info_header()) {
    header_lists_->AppendHeader(info->headers(), info->info_header());
  }
  if (enable_http_tracing_) {
    for (unsigned int i = 0; i < http_tracing_headers_.size(); i++) {
      header_lists_->AppendHeader(info->headers(),
                                  (http_tracing_headers_)[i].c_str());
    }

    header_lists_->AppendHeader(info->headers(), info->tracing_header_pid());
    header_lists_->AppendHeader(info->headers(), info->tracing_header_gid());
    header_lists_->AppendHeader(info->headers(), info->tracing_header_uid());

    LogCvmfs(kLogDownload, kLogDebug,
             "(manager '%s' - id %" PRId64 ") "
             "CURL Header for URL: %s is:\n %s",
             name_.c_str(), info->id(), info->url()->c_str(),
             header_lists_->Print(info->headers()).c_str());
  }

  if (info->force_nocache()) {
    SetNocache(info);
  } else {
    info->SetNocache(false);
  }
  if (info->compressed()) {
    zlib::DecompressInit(info->GetZstreamPtr());
  }
  if (info->expected_hash()) {
    assert(info->hash_context().buffer != NULL);
    shash::Init(info->hash_context());
  }

  // Fetcher hands the same thread-local JobInfo back for the next
  // object, so every per-transfer field has to be cleared here.  A
  // stale peer_unresponsive_ in particular would let an unrelated
  // later failure escalate away from the proxy.
  info->SetResumeOffset(0);
  info->SetContentLength(-1);
  info->SetPeerUnresponsive(false);
  info->SetNoProgressSinceMs(0);
  info->SetRetryNotBeforeMs(0);
  SetRangeOption(info, handle);

  // Set curl parameters
  curl_easy_setopt(handle, CURLOPT_PRIVATE, static_cast<void *>(info));
  curl_easy_setopt(handle, CURLOPT_WRITEHEADER, static_cast<void *>(info));
  curl_easy_setopt(handle, CURLOPT_WRITEDATA, static_cast<void *>(info));
  curl_easy_setopt(handle, CURLOPT_HTTPHEADER, info->headers());
  if (info->head_request()) {
    curl_easy_setopt(handle, CURLOPT_NOBODY, 1);
  } else {
    curl_easy_setopt(handle, CURLOPT_HTTPGET, 1);
  }
  if (opt_ipv4_only_) {
    curl_easy_setopt(handle, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
  }
  // Handles are pooled; a previous retry may have requested a fresh connection
  curl_easy_setopt(handle, CURLOPT_FRESH_CONNECT, 0L);
  if (follow_redirects_) {
    curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 1);
    curl_easy_setopt(handle, CURLOPT_MAXREDIRS, 4);
  }
#ifdef DEBUGMSG
  curl_easy_setopt(handle, CURLOPT_VERBOSE, 1);
  curl_easy_setopt(handle, CURLOPT_DEBUGFUNCTION, CallbackCurlDebug);
#endif
}

void DownloadManager::CheckHostInfoReset(const std::string &typ,
                                         HostInfo &info,
                                         JobInfo *jobinfo,
                                         time_t &now) {
  if (info.timestamp_backup > 0) {
    if (now == 0)
      now = time(NULL);
    if (static_cast<int64_t>(now)
        > static_cast<int64_t>(info.timestamp_backup + info.reset_after)) {
      LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
               "(manager %s - id %" PRId64 ") "
               "switching %s from %s to %s (reset %s)",
               name_.c_str(), jobinfo->id(), typ.c_str(),
               (*info.chain)[info.current].c_str(), (*info.chain)[0].c_str(),
               typ.c_str());
      info.current = 0;
      info.timestamp_backup = 0;
    }
  }
}


/**
 * Sets the URL specific options such as host to use and timeout.  It might also
 * set an error code, in which case the further processing should react on.
 */
void DownloadManager::SetUrlOptions(JobInfo *info) {
  CURL *curl_handle = info->curl_handle();
  string url_prefix;
  time_t now = 0;

  const MutexLockGuard m(lock_options_);

  // sharding policy
  if (sharding_policy_.UseCount() > 0) {
    if (info->proxy() != "") {
      // proxy already set, so this is a failover event
      perf::Inc(counters_->n_proxy_failover);
    }
    info->SetProxy(sharding_policy_->GetNextProxy(
        info->url(), info->proxy(),
        info->range_offset() == -1 ? 0 : info->range_offset()));

    curl_easy_setopt(info->curl_handle(), CURLOPT_PROXY, info->proxy().c_str());
  } else {  // no sharding policy
    // Check if proxy group needs to be reset from backup to primary
    if (opt_timestamp_backup_proxies_ > 0) {
      now = time(NULL);
      if (static_cast<int64_t>(now) > static_cast<int64_t>(
              opt_timestamp_backup_proxies_ + opt_proxy_groups_reset_after_)) {
        opt_proxy_groups_current_ = 0;
        opt_timestamp_backup_proxies_ = 0;
        RebalanceProxiesUnlocked("Reset proxy group from backup to primary");
      }
    }
    // Check if load-balanced proxies within the group need to be reset
    if (opt_timestamp_failover_proxies_ > 0) {
      if (now == 0)
        now = time(NULL);
      if (static_cast<int64_t>(now)
          > static_cast<int64_t>(opt_timestamp_failover_proxies_
                                 + opt_proxy_groups_reset_after_)) {
        RebalanceProxiesUnlocked(
            "Reset load-balanced proxies within the active group");
      }
    }

    ProxyInfo *proxy = ChooseProxyUnlocked(info->expected_hash());
    if (!proxy || (proxy->url == "DIRECT")) {
      if (opt_proxy_mandatory_) {
        // Every configured proxy is currently unusable.  Connecting directly
        // would bypass the site proxy, so fail the request and let the normal
        // retry/backoff path report the outage.
        LogCvmfs(kLogDownload, kLogSyslogErr | kLogDebug,
                 "(manager '%s' - id %" PRId64 ") "
                 "no usable proxy available, refusing to connect directly",
                 name_.c_str(), info->id());
        info->SetProxy("BLOCKED");
        curl_easy_setopt(info->curl_handle(), CURLOPT_PROXY, "0.0.0.0:1");
      } else {
        info->SetProxy("DIRECT");
        curl_easy_setopt(info->curl_handle(), CURLOPT_PROXY, "");
      }
    } else {
      // Note: inside ValidateProxyIpsUnlocked() we may change the proxy data
      // structure, so we must not pass proxy->... (== current_proxy())
      // parameters directly
      const std::string purl = proxy->url;
      const dns::Host phost = proxy->host;
      const bool changed = ValidateProxyIpsUnlocked(purl, phost);
      // Current proxy may have changed
      if (changed) {
        proxy = ChooseProxyUnlocked(info->expected_hash());
      }
      info->SetProxy(proxy->url);
      if (proxy->host.status() == dns::kFailOk) {
        curl_easy_setopt(info->curl_handle(), CURLOPT_PROXY,
                         info->proxy().c_str());
      } else {
        // We know it can't work, don't even try to download
        curl_easy_setopt(info->curl_handle(), CURLOPT_PROXY, "0.0.0.0");
      }
    }
  }  // end !sharding

  // Check if metalink and host chains need to be reset
  CheckHostInfoReset("metalink", opt_metalink_, info, now);
  CheckHostInfoReset("host", opt_host_, info, now);

  curl_easy_setopt(curl_handle, CURLOPT_LOW_SPEED_LIMIT, opt_low_speed_limit_);
  if (opt_tcp_keepalive_ > 0) {
    // Keep idle keep-alive connections visible to stateful middleboxes
    // (NAT/conntrack/DPI) that silently drop flows after an idle period
    curl_easy_setopt(curl_handle, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl_handle, CURLOPT_TCP_KEEPIDLE,
                     static_cast<long>(opt_tcp_keepalive_));  // NOLINT
    curl_easy_setopt(curl_handle, CURLOPT_TCP_KEEPINTVL,
                     static_cast<long>(opt_tcp_keepalive_));  // NOLINT
  } else {
    curl_easy_setopt(curl_handle, CURLOPT_TCP_KEEPALIVE, 0L);
  }
#if LIBCURL_VERSION_NUM >= 0x074100  // 7.65.0
  // libcurl's own default is ~2 minutes, which outlives the idle timeout of a
  // typical middlebox; a connection it has already forgotten must not be
  // picked up again.
  curl_easy_setopt(curl_handle, CURLOPT_MAXAGE_CONN,
                   kMaxIdleConnectionReuseSecs);
#endif
  if (opt_fresh_connect_until_ > 0) {
    if (now == 0)
      now = time(NULL);
    if (now < opt_fresh_connect_until_)
      curl_easy_setopt(curl_handle, CURLOPT_FRESH_CONNECT, 1L);
    else
      opt_fresh_connect_until_ = 0;
  }
  if (info->proxy() != "DIRECT") {
    curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, opt_timeout_proxy_);
    curl_easy_setopt(curl_handle, CURLOPT_LOW_SPEED_TIME, opt_timeout_proxy_);
  } else {
    curl_easy_setopt(curl_handle, CURLOPT_CONNECTTIMEOUT, opt_timeout_direct_);
    curl_easy_setopt(curl_handle, CURLOPT_LOW_SPEED_TIME, opt_timeout_direct_);
  }
  if (!opt_dns_server_.empty())
    curl_easy_setopt(curl_handle, CURLOPT_DNS_SERVERS, opt_dns_server_.c_str());

  if (info->probe_hosts()) {
    if (CheckMetalinkChain(now)) {
      url_prefix = (*opt_metalink_.chain)[opt_metalink_.current];
      info->SetCurrentMetalinkChainIndex(opt_metalink_.current);
      LogCvmfs(kLogDownload, kLogDebug,
               "(manager %s - id %" PRId64 ") "
               "reading from metalink %d",
               name_.c_str(), info->id(), opt_metalink_.current);
    } else if (opt_host_.chain) {
      url_prefix = (*opt_host_.chain)[opt_host_.current];
      info->SetCurrentHostChainIndex(opt_host_.current);
      LogCvmfs(kLogDownload, kLogDebug,
               "(manager %s - id %" PRId64 ") "
               "reading from host %d",
               name_.c_str(), info->id(), opt_host_.current);
    }
  }

  string url = url_prefix + *(info->url());

  curl_easy_setopt(curl_handle, CURLOPT_SSL_VERIFYPEER, 1L);
  if (url.substr(0, 5) == "https") {
    const bool rvb = ssl_certificate_store_.ApplySslCertificatePath(
        curl_handle);
    if (!rvb) {
      LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
               "(manager %s - id %" PRId64 ") "
               "Failed to set SSL certificate path %s",
               name_.c_str(), info->id(),
               ssl_certificate_store_.GetCaPath().c_str());
    }
    if (info->pid() != -1) {
      if (credentials_attachment_ == NULL) {
        LogCvmfs(kLogDownload, kLogDebug,
                 "(manager %s - id %" PRId64 ") "
                 "uses secure downloads but no credentials attachment set",
                 name_.c_str(), info->id());
      } else {
        const bool retval = credentials_attachment_->ConfigureCurlHandle(
            curl_handle, info->pid(), info->GetCredDataPtr());
        if (!retval) {
          LogCvmfs(kLogDownload, kLogDebug,
                   "(manager %s - id %" PRId64 ") "
                   "failed attaching credentials",
                   name_.c_str(), info->id());
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
    } else if (info->proxy() == "DIRECT") {
      replacement = proxy_template_direct_;
    } else {
      if (opt_proxy_groups_current_ >= opt_proxy_groups_fallback_) {
        // It doesn't make sense to use the fallback proxies in Geo-API requests
        // since the fallback proxies are supposed to get sorted, too.
        // Dropping to an unproxied request to achieve that is only acceptable
        // where direct connections are acceptable at all.  This branch is
        // reached exactly while the local proxy is being failed over, so an
        // unconditional switch here put the Geo-API query on the wire
        // unproxied at the one moment a mandatory-proxy site can least afford
        // it.  The query still goes out, through whichever proxy is current;
        // only the cache-key template falls back to the direct form.
        if (!opt_proxy_mandatory_) {
          info->SetProxy("DIRECT");
          curl_easy_setopt(info->curl_handle(), CURLOPT_PROXY, "");
        }
        replacement = proxy_template_direct_;
      } else {
        const ProxyInfo *proxy = ChooseProxyUnlocked(info->expected_hash());
        replacement = proxy ? proxy->host.name() : proxy_template_direct_;
      }
    }
    replacement = (replacement == "") ? proxy_template_direct_ : replacement;
    LogCvmfs(kLogDownload, kLogDebug,
             "(manager %s - id %" PRId64 ") "
             "replacing @proxy@ by %s",
             name_.c_str(), info->id(), replacement.c_str());
    url = ReplaceAll(url, "@proxy@", replacement);
  }

  // TODO(heretherebedragons) before removing
  // static_cast<cvmfs::MemSink*>(info->sink)->size() == 0
  // and just always call info->sink->Reserve()
  // we should do a speed check
  if ((info->sink() != NULL) && info->sink()->RequiresReserve()
      && (static_cast<cvmfs::MemSink *>(info->sink())->size() == 0)
      && HasPrefix(url, "file://", false)) {
    platform_stat64 stat_buf;
    const int retval = platform_stat(url.c_str(), &stat_buf);
    if (retval != 0) {
      // this is an error: file does not exist or out of memory
      // error is caught in other code section.
      info->sink()->Reserve(64ul * 1024ul);
    } else {
      info->sink()->Reserve(stat_buf.st_size);
    }
  }

  curl_easy_setopt(curl_handle, CURLOPT_URL,
                   EscapeUrl(info->id(), url).c_str());
}


/**
 * Checks if the name resolving information is still up to date.  The host
 * object should be one from the current load-balance group.  If the information
 * changed, gather new set of resolved IPs and, if different, exchange them in
 * the load-balance group on the fly.  In the latter case, also rebalance the
 * proxies.  The options mutex needs to be open.
 *
 * Returns true if proxies may have changed.
 */
bool DownloadManager::ValidateProxyIpsUnlocked(const string &url,
                                               const dns::Host &host) {
  if (!host.IsExpired())
    return false;
  LogCvmfs(kLogDownload, kLogDebug, "(manager '%s') validate DNS entry for %s",
           name_.c_str(), host.name().c_str());

  const unsigned group_idx = opt_proxy_groups_current_;
  dns::Host new_host = resolver_->Resolve(host.name());

  bool update_only = true;  // No changes to the list of IP addresses.
  if (new_host.status() != dns::kFailOk) {
    // Try again later in case resolving fails.
    LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
             "(manager '%s') failed to resolve IP addresses for %s (%d - %s)",
             name_.c_str(), host.name().c_str(), new_host.status(),
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
    return false;
  }

  assert(new_host.status() == dns::kFailOk);

  // Remove old host objects, insert new objects, and rebalance.
  LogCvmfs(kLogDownload, kLogDebug | kLogSyslog,
           "(manager '%s') DNS entries for proxy %s changed, adjusting",
           name_.c_str(), host.name().c_str());
  vector<ProxyInfo> *group = current_proxy_group();
  opt_num_proxies_ -= group->size();
  for (unsigned i = 0; i < group->size();) {
    if ((*group)[i].host.id() == host.id()) {
      group->erase(group->begin() + i);
    } else {
      i++;
    }
  }
  vector<ProxyInfo> new_infos;
  set<string> const best_addresses = new_host.ViewBestAddresses(
      opt_ip_preference_);
  set<string>::const_iterator iter_ips = best_addresses.begin();
  for (; iter_ips != best_addresses.end(); ++iter_ips) {
    const string url_ip = dns::RewriteUrl(url, *iter_ips);
    new_infos.push_back(ProxyInfo(new_host, url_ip));
  }
  group->insert(group->end(), new_infos.begin(), new_infos.end());
  opt_num_proxies_ += new_infos.size();

  const std::string msg = "DNS entries for proxy " + host.name() + " changed";

  RebalanceProxiesUnlocked(msg);
  return true;
}


/**
 * Adds transfer time and downloaded bytes to the global counters.
 */
void DownloadManager::UpdateStatistics(CURL *handle) {
  curl_off_t val;
  int retval;
  int64_t sum = 0;

  retval = curl_easy_getinfo(handle, CURLINFO_SIZE_DOWNLOAD_T, &val);
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
  const MutexLockGuard m(lock_options_);
  const unsigned max_retries = opt_max_retries_;

  const bool retryable_error = IsProxyTransferError(info->error_code())
                               || IsHostTransferError(info->error_code())
                               || (info->error_code() == kFailHostResolve)
                               || (info->error_code() == kFailProxyResolve);
  if (info->nocache() || !retryable_error)
    return false;
  if (info->num_retries() < max_retries)
    return true;
  // Beyond the retry count, keep retrying failures that are cheap: as long
  // as the attempts without progress have not yet cost one timeout period in
  // total, the peer is answering quickly (severed streams, not a dead host)
  // and another attempt is worth it.  A dead peer times out on each attempt
  // and therefore still exhausts the budget after max_retries attempts.
  if (info->no_progress_since_ms() > 0) {
    const uint64_t now_ms = platform_monotonic_time_ns() / 1000000;
    const uint64_t budget_ms = 1000ULL * ((info->proxy() == "DIRECT")
                                              ? opt_timeout_direct_
                                              : opt_timeout_proxy_);
    return (now_ms - info->no_progress_since_ms()) < budget_ms;
  }
  return false;
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
    const MutexLockGuard m(lock_options_);
    backoff_init_ms = opt_backoff_init_ms_;
    backoff_max_ms = opt_backoff_max_ms_;
  }

  info->SetNumRetries(info->num_retries() + 1);
  perf::Inc(counters_->n_retries);
  if ((info->error_code() == kFailHostShortTransfer)
      || (info->error_code() == kFailProxyShortTransfer)) {
    // A cut stream is a lossy link, not an overloaded server: retry promptly
    // with a small jitter instead of the exponential backoff
    info->SetBackoffMs(10 + prng_.Next(90));
  } else if (info->backoff_ms() == 0) {
    info->SetBackoffMs(prng_.Next(backoff_init_ms + 1));  // Must be != 0
  } else {
    info->SetBackoffMs(info->backoff_ms() * 2);
  }
  if (info->backoff_ms() > backoff_max_ms) {
    info->SetBackoffMs(backoff_max_ms);
  }

  LogCvmfs(kLogDownload, kLogDebug,
           "(manager '%s' - id %" PRId64 ") backing off for %d ms",
           name_.c_str(), info->id(), info->backoff_ms());
  if (atomic_xadd32(&multi_threaded_, 0) == 1) {
    // Sleeping here would stall the I/O thread and with it every other
    // transfer of this manager; MainDownload re-queues the job when due.
    info->SetRetryNotBeforeMs(platform_monotonic_time_ns() / 1000000
                              + info->backoff_ms());
  } else {
    SafeSleepMs(info->backoff_ms());
  }
}

void DownloadManager::SetNocache(JobInfo *info) {
  if (info->nocache())
    return;
  header_lists_->AppendHeader(info->headers(), "Pragma: no-cache");
  header_lists_->AppendHeader(info->headers(), "Cache-Control: no-cache");
  curl_easy_setopt(info->curl_handle(), CURLOPT_HTTPHEADER, info->headers());
  info->SetNocache(true);
}


/**
 * Reverse operation of SetNocache. Makes sure that "no-cache" header
 * disappears from the list of headers to let proxies work normally.
 */
void DownloadManager::SetRegularCache(JobInfo *info) {
  if (info->nocache() == false)
    return;
  header_lists_->CutHeader("Pragma: no-cache", info->GetHeadersPtr());
  header_lists_->CutHeader("Cache-Control: no-cache", info->GetHeadersPtr());
  curl_easy_setopt(info->curl_handle(), CURLOPT_HTTPHEADER, info->headers());
  info->SetNocache(false);
}


/**
 * Frees the storage associated with the authz attachment from the job
 */
void DownloadManager::ReleaseCredential(JobInfo *info) {
  if (info->cred_data()) {
    assert(credentials_attachment_ != NULL);  // Someone must have set it
    credentials_attachment_->ReleaseCurlHandle(info->curl_handle(),
                                               info->cred_data());
    info->SetCredData(NULL);
  }
}


/* Sort links based on the "pri=" parameter */
static bool sortlinks(const std::string &s1, const std::string &s2) {
  const size_t pos1 = s1.find("; pri=");
  const size_t pos2 = s2.find("; pri=");
  int pri1, pri2;
  if ((pos1 != std::string::npos) && (pos2 != std::string::npos)
      && (sscanf(s1.substr(pos1 + 6).c_str(), "%d", &pri1) == 1)
      && (sscanf(s2.substr(pos2 + 6).c_str(), "%d", &pri2) == 1)) {
    return pri1 < pri2;
  }
  return false;
}

/**
 * Parses Link header and uses it to set a new host chain.
 * See rfc6249.
 */
void DownloadManager::ProcessLink(JobInfo *info) {
  std::vector<std::string> links = SplitString(info->link(), ',');
  if (info->link().find("; pri=") != std::string::npos)
    std::sort(links.begin(), links.end(), sortlinks);

  std::vector<std::string> host_list;

  std::vector<std::string>::const_iterator il = links.begin();
  for (; il != links.end(); ++il) {
    const std::string &link = *il;
    if ((link.find("; rel=duplicate") == std::string::npos)
        && (link.find("; rel=\"duplicate\"") == std::string::npos)) {
      LogCvmfs(kLogDownload, kLogDebug,
               "skipping link '%s' because it does not contain rel=duplicate",
               link.c_str());
      continue;
    }
    // ignore depth= field since there's nothing useful we can do with it

    size_t start = link.find('<');
    if (start == std::string::npos) {
      LogCvmfs(
          kLogDownload, kLogDebug,
          "skipping link '%s' because it does not have a left angle bracket",
          link.c_str());
      continue;
    }

    start++;
    if ((link.substr(start, 7) != "http://")
        && (link.substr(start, 8) != "https://")) {
      LogCvmfs(kLogDownload, kLogDebug,
               "skipping link '%s' of unrecognized url protocol", link.c_str());
      continue;
    }

    size_t end = link.find('/', start + 8);
    if (end == std::string::npos)
      end = link.find('>');
    if (end == std::string::npos) {
      LogCvmfs(kLogDownload, kLogDebug,
               "skipping link '%s' because no slash in url and no right angle "
               "bracket",
               link.c_str());
      continue;
    }
    const std::string host = link.substr(start, end - start);
    LogCvmfs(kLogDownload, kLogDebug, "adding linked host '%s'", host.c_str());
    host_list.push_back(host);
  }

  if (host_list.size() > 0) {
    SetHostChain(host_list);
    opt_metalink_timestamp_link_ = time(NULL);
    // Set the index to the first linked host because the protocol includes
    // redirecting to the first host
    info->SetCurrentHostChainIndex(0);
    // Don't process the same links again if there are retries
    info->SetLink("");
    LogCvmfs(kLogDownload, kLogDebug | kLogSyslog,
                 "(manager '%s' - id %" PRId64 ") "
                 "received %zu hosts from metalink server, starting with %s",
                 name_.c_str(), info->id(),
                 host_list.size(),
                 host_list[0].c_str());
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
           "(manager '%s' - id %" PRId64 ") "
           "Verify downloaded url %s, proxy %s (curl error %d)",
           name_.c_str(), info->id(), info->url()->c_str(),
           info->proxy().c_str(), curl_error);
  UpdateStatistics(info->curl_handle());

  // Classify this attempt on its own evidence.  A retry reuses the same
  // JobInfo and the same curl handle -- InitializeRequest() runs only when the
  // job first arrives from the pipe -- so a peer_unresponsive_ left true by an
  // earlier attempt would otherwise let SwitchProxy() escalate on evidence
  // that no longer applies.  The branches below set it from this result.
  info->SetPeerUnresponsive(false);

  bool was_metalink;
  std::string typ;
  if (info->current_metalink_chain_index() >= 0) {
    if (info->link() != "") {
      // process Link header whether or not the redirected URL got an error
      ProcessLink(info);
      // The metalink lookup succeeded, so if there was an error it was
      // with the redirect
      was_metalink = false;
      typ = "host";
    } else {
      // no Link headers were received, so the metalink lookup failed
      was_metalink = true;
      typ = "metalink";
    }
  } else {
    was_metalink = false;
    typ = "host";
  }

  // Verification and error classification
  switch (curl_error) {
    case CURLE_OK:
      // Verify content hash
      if (info->expected_hash()) {
        shash::Any match_hash;
        shash::Final(info->hash_context(), &match_hash);
        if (match_hash != *(info->expected_hash())) {
          if (ignore_signature_failures_) {
            LogCvmfs(
                kLogDownload, kLogDebug | kLogSyslogErr,
                "(manager '%s' - id %" PRId64 ") "
                "ignoring failed hash verification of %s (expected %s, got %s)",
                name_.c_str(), info->id(), info->url()->c_str(),
                info->expected_hash()->ToString().c_str(),
                match_hash.ToString().c_str());
          } else {
            LogCvmfs(kLogDownload, kLogDebug,
                     "(manager '%s' - id %" PRId64 ") "
                     "hash verification of %s failed (expected %s, got %s)",
                     name_.c_str(), info->id(), info->url()->c_str(),
                     info->expected_hash()->ToString().c_str(),
                     match_hash.ToString().c_str());
            info->SetErrorCode(kFailBadData);
            break;
          }
        }
      }

      info->SetErrorCode(kFailOk);
      break;
    case CURLE_UNSUPPORTED_PROTOCOL: {
      // Also returned for a non-HTTP reply on an established connection
      // (e.g. a middlebox injecting a plain-text block banner), which libcurl
      // rejects as HTTP/0.9.  That is a mangled transfer, not a bad URL, so
      // let the usual retry and proxy/host fail-over apply instead of EIO.
      if (ConnectCompleted(info->curl_handle())) {
        info->SetErrorCode((info->proxy() == "DIRECT")
                               ? kFailHostShortTransfer
                               : kFailProxyShortTransfer);
      } else {
        info->SetErrorCode(kFailUnsupportedProtocol);
      }
      break;
    }
    case CURLE_URL_MALFORMAT:
      info->SetErrorCode(kFailBadUrl);
      break;
    case CURLE_COULDNT_RESOLVE_PROXY:
      info->SetErrorCode(kFailProxyResolve);
      break;
    case CURLE_COULDNT_RESOLVE_HOST:
      info->SetErrorCode(kFailHostResolve);
      break;
    case CURLE_OPERATION_TIMEDOUT: {
      // A timeout means one of two opposite things.  If the connection never
      // came up, nobody answered: the peer is unreachable, which is exactly
      // the case that justifies looking elsewhere.  If it did come up and the
      // transfer then crawled, the peer is alive and merely saturated, and
      // abandoning it would move traffic off-site for no good reason.
      // The connect time stays zero while the connect has not completed, so
      // it separates the two; see ConnectCompleted().
      info->SetPeerUnresponsive(!ConnectCompleted(info->curl_handle()));
      info->SetErrorCode((info->proxy() == "DIRECT") ? kFailHostTooSlow
                                                     : kFailProxyTooSlow);
      // Quarantine the connection pool for one timeout period (see
      // opt_fresh_connect_until_)
      const MutexLockGuard m(lock_options_);
      opt_fresh_connect_until_ = time(NULL)
                                 + ((info->proxy() == "DIRECT")
                                        ? opt_timeout_direct_
                                        : opt_timeout_proxy_);
      break;
    }
    case CURLE_PARTIAL_FILE:
    case CURLE_GOT_NOTHING:
    case CURLE_RECV_ERROR:
      info->SetErrorCode((info->proxy() == "DIRECT") ? kFailHostShortTransfer
                                                     : kFailProxyShortTransfer);
      break;
    case CURLE_FILE_COULDNT_READ_FILE:
    case CURLE_COULDNT_CONNECT: {
      // A refused connection comes back in about a round trip and proves that
      // something is listening and actively rejecting -- a proxy out of slots,
      // not a dead one.  It is the opposite of unresponsive, so retry it here
      // rather than treating it as grounds to leave the local proxy.
      //
      // CURLE_COULDNT_CONNECT is not only a refusal, though: it also covers
      // ENETUNREACH, EHOSTUNREACH and similar, where nothing was reached at
      // all.  Those are exactly the case escalation exists for, so ask the OS
      // error which of the two happened rather than assuming the peer is up.
      if (curl_error == CURLE_COULDNT_CONNECT) {
        long os_errno = 0;  // NOLINT(runtime/int) -- libcurl's getinfo type
        if (curl_easy_getinfo(info->curl_handle(), CURLINFO_OS_ERRNO,
                              &os_errno)
            == CURLE_OK) {
          info->SetPeerUnresponsive(os_errno != ECONNREFUSED);
        }
      }
      if (info->proxy() != "DIRECT") {
        // This is a guess.  Fail-over can still change to switching host
        info->SetErrorCode(kFailProxyConnection);
      } else {
        info->SetErrorCode(kFailHostConnection);
      }
      break;
    }
    case CURLE_TOO_MANY_REDIRECTS:
      info->SetErrorCode(kFailHostConnection);
      break;
    case CURLE_SSL_CACERT_BADFILE:
      LogCvmfs(kLogDownload, kLogDebug | kLogSyslogErr,
               "(manager '%s' -id %" PRId64 ") "
               "Failed to load certificate bundle. "
               "X509_CERT_BUNDLE might point to the wrong location.",
               name_.c_str(), info->id());
      info->SetErrorCode(kFailHostConnection);
      break;
    // As of curl 7.62.0, CURLE_SSL_CACERT is the same as
    // CURLE_PEER_FAILED_VERIFICATION
    case CURLE_PEER_FAILED_VERIFICATION:
      LogCvmfs(kLogDownload, kLogDebug | kLogSyslogErr,
               "(manager '%s' - id %" PRId64 ") "
               "invalid SSL certificate of remote host. "
               "X509_CERT_DIR and/or X509_CERT_BUNDLE might point to the wrong "
               "location.",
               name_.c_str(), info->id());
      info->SetErrorCode(kFailHostConnection);
      break;
    case CURLE_ABORTED_BY_CALLBACK:
    case CURLE_WRITE_ERROR:
      // Error set by callback
      break;
    case CURLE_SEND_ERROR:
      // The curl error CURLE_SEND_ERROR can be seen when a cache is misbehaving
      // and closing connections before the http request send is completed.
      // Handle this error, treating it as a short transfer error.
      info->SetErrorCode((info->proxy() == "DIRECT") ? kFailHostShortTransfer
                                                     : kFailProxyShortTransfer);
      break;
    default:
      LogCvmfs(kLogDownload, kLogSyslogErr,
               "(manager '%s' - id %" PRId64 ") "
               "unexpected curl error (%d) while trying to fetch %s",
               name_.c_str(), info->id(), curl_error, info->url()->c_str());
      info->SetErrorCode(kFailOther);
      break;
  }

  std::vector<std::string> *host_chain;
  unsigned char num_used_hosts;
  if (was_metalink) {
    host_chain = opt_metalink_.chain;
    num_used_hosts = info->num_used_metalinks();
  } else {
    host_chain = opt_host_.chain;
    num_used_hosts = info->num_used_hosts();
  }

  // Determination if download should be repeated
  bool try_again = false;
  bool same_url_retry = CanRetry(info);
  if (info->error_code() != kFailOk) {
    const MutexLockGuard m(lock_options_);
    if (info->error_code() == kFailBadData) {
      if (!info->nocache()) {
        try_again = true;
      } else {
        // Make it a host failure, or a proxy failure if a proxy is in the
        // path: with the no-cache retry also corrupt, a tampering proxy (or
        // middlebox on the proxy path) is at least as likely as a bad host,
        // and only a proxy switch can route around it.  If all proxies fail
        // the same way, the regular proxy-to-host fail-over still follows.
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
                 "(manager '%s' - id %" PRId64 ") "
                 "data corruption with no-cache header, try another %s",
                 name_.c_str(), info->id(),
                 (info->proxy() == "DIRECT") ? typ.c_str() : "proxy");

        info->SetErrorCode((info->proxy() == "DIRECT") ? kFailHostHttp
                                                       : kFailProxyHttp);
      }
    }
    if (same_url_retry
        || (((info->error_code() == kFailHostResolve)
             || IsHostTransferError(info->error_code())
             || (info->error_code() == kFailHostHttp))
            && info->probe_hosts() && host_chain
            && (num_used_hosts < host_chain->size()))) {
      try_again = true;
    }
    if (same_url_retry
        || (((info->error_code() == kFailProxyResolve)
             || IsProxyTransferError(info->error_code())
             || (info->error_code() == kFailProxyHttp)))) {
      if (sharding_policy_.UseCount() > 0) {  // sharding policy
        try_again = true;
        same_url_retry = false;
      } else {  // no sharding
        try_again = true;
        // If all proxies failed, do a next round with the next host
        if (!same_url_retry && (info->num_used_proxies() >= opt_num_proxies_)) {
          // Check if this can be made a host fail-over
          if (info->probe_hosts() && host_chain
              && (num_used_hosts < host_chain->size())) {
            // reset proxy group if not already performed by other handle
            if (opt_proxy_groups_) {
              if ((opt_proxy_groups_current_ > 0)
                  || (opt_proxy_groups_current_burned_ > 0)) {
                opt_proxy_groups_current_ = 0;
                opt_timestamp_backup_proxies_ = 0;
                const std::string msg = "reset proxies for " + typ
                                        + " failover";
                RebalanceProxiesUnlocked(msg);
              }
            }

            // Make it a host failure
            LogCvmfs(kLogDownload, kLogDebug,
                     "(manager '%s' - id %" PRId64 ") make it a %s failure",
                     name_.c_str(), info->id(), typ.c_str());
            info->SetNumUsedProxies(1);
            info->SetErrorCode(kFailHostAfterProxy);
          } else {
            if (failover_indefinitely_) {
              // Instead of giving up, reset the num_used_proxies counter,
              // switch proxy and try again
              LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
                       "(manager '%s' - id %" PRId64 ") "
                       "VerifyAndFinalize() would fail the download here. "
                       "Instead switch proxy and retry download. "
                       "typ=%s "
                       "info->probe_hosts=%d host_chain=%p num_used_hosts=%d "
                       "host_chain->size()=%lu same_url_retry=%d "
                       "info->num_used_proxies=%d opt_num_proxies_=%d",
                       name_.c_str(), info->id(), typ.c_str(),
                       static_cast<int>(info->probe_hosts()), host_chain,
                       num_used_hosts, host_chain ? host_chain->size() : -1,
                       static_cast<int>(same_url_retry),
                       info->num_used_proxies(), opt_num_proxies_);
              info->SetNumUsedProxies(1);
              RebalanceProxiesUnlocked(
                  "download failed - failover indefinitely");
              try_again = !Interrupted(fqrn_, info);
            } else {
              try_again = false;
            }
          }
        }  // Make a proxy failure a host failure
      }  // Proxy failure assumed
    }  // end !sharding
  }

  if (try_again) {
    LogCvmfs(kLogDownload, kLogDebug,
             "(manager '%s' - id %" PRId64 ") "
             "Trying again on same curl handle, %s"
             "error code %d%s",
             name_.c_str(), info->id(), same_url_retry ? "same url, " : "",
             info->error_code(), info->nocache() ? ", no cache" : "");
    // Reset internal state and destination. In parallel-decompress mode the
    // sink and zstream are owned by the caller thread (it pops and decompresses
    // the queued chunks). Resetting them here would race the caller and, worse,
    // leave the bytes of this failed attempt in the tube to be decompressed
    // into the output. Defer the reset by enqueuing an ordered marker; the
    // caller discards this attempt's bytes when it pops it (see below).
    // Resume instead of restart: the content hash covers the bytes on the
    // wire, and hash context, zlib stream and sink all keep their state, so a
    // transfer that was cut or stalled after delivering data can continue
    // with a Range request from the first missing byte.  A retry that made
    // real progress does not consume the retry budget either, otherwise a
    // large object could never complete on a link that drops long flows.
    bool resume = false;
    bool free_retry = false;
    if (info->expected_hash() && (info->sink() != NULL)
        && !info->sink()->RequiresReserve() && !info->nocache()
        && (IsHostTransferError(info->error_code())
            || IsProxyTransferError(info->error_code()))) {
      // Bytes of this attempt count only if they came with a 2xx; an attempt
      // that died before any body byte keeps the progress of earlier ones.
      // The object is content-addressed, so the resume is valid on another
      // host or proxy as well.
      curl_off_t received = 0;
      if ((info->http_code() / 100) == 2) {
        curl_easy_getinfo(info->curl_handle(), CURLINFO_SIZE_DOWNLOAD_T,
                          &received);
      }
      resume = (received > 0) || (info->resume_offset() > 0);
      if (received > 0) {
        free_retry = (received >= kMinResumeProgress);
        if (free_retry) {
          // Progress: the peer is alive and the retry budget starts afresh
          info->SetNumRetries(0);
          info->SetNoProgressSinceMs(0);
        }
        info->SetResumeOffset(info->resume_offset()
                              + static_cast<uint64_t>(received));
        LogCvmfs(kLogDownload, kLogDebug,
                 "(manager '%s' - id %" PRId64 ") "
                 "resuming %s at byte %" PRIu64,
                 name_.c_str(), info->id(), info->url()->c_str(),
                 info->resume_offset());
      }
    }
    if (!resume) {
      info->SetResumeOffset(0);
    }
    if (!free_retry && (info->no_progress_since_ms() == 0)) {
      info->SetNoProgressSinceMs(platform_monotonic_time_ns() / 1000000);
    }

    const bool defer_reset = info->IsValidDataTube();
    if (!resume && !defer_reset) {
      if (info->sink() != NULL && info->sink()->Reset() != 0) {
        info->SetErrorCode(kFailLocalIO);
        goto verify_and_finalize_stop;
      }
    }
    if (info->interrupt_cue() && info->interrupt_cue()->IsCanceled()) {
      info->SetErrorCode(kFailCanceled);
      goto verify_and_finalize_stop;
    }

    // The hash context is updated on this (MainDownload) thread in
    // CallbackCurlData(), so it is reset here regardless of the decompress
    // mode.
    if (!resume && info->expected_hash()) {
      shash::Init(info->hash_context());
    }
    if (!resume && info->compressed() && !defer_reset) {
      zlib::DecompressInit(info->GetZstreamPtr());
    }
    if (!resume && defer_reset) {
      info->GetDataTubePtr()->EnqueueBack(new DataTubeElement(kActionReset));
    }

    if (sharding_policy_.UseCount() > 0) {  // sharding policy
      ReleaseCredential(info);
      SetUrlOptions(info);
    } else {  // no sharding policy
      SetRegularCache(info);

      // Failure handling
      bool switch_proxy = false;
      bool switch_host = false;
      switch (info->error_code()) {
        case kFailBadData:
          SetNocache(info);
          break;
        case kFailProxyResolve:
          // A lost DNS packet is not a dead proxy: retry before switching
          if (same_url_retry) {
            Backoff(info);
          } else {
            switch_proxy = true;
          }
          break;
        case kFailProxyHttp:
          switch_proxy = true;
          break;
        case kFailHostResolve:
          if (same_url_retry) {
            Backoff(info);
          } else {
            switch_host = true;
          }
          break;
        case kFailHostHttp:
        case kFailHostAfterProxy:
          switch_host = true;
          break;
        default:
          if (IsProxyTransferError(info->error_code())) {
            if (same_url_retry) {
              if (!free_retry)
                Backoff(info);
            } else {
              switch_proxy = true;
            }
          } else if (IsHostTransferError(info->error_code())) {
            if (same_url_retry) {
              if (!free_retry)
                Backoff(info);
            } else {
              switch_host = true;
            }
          } else {
            // No other errors expected when retrying
            PANIC(NULL);
          }
      }
      if (switch_proxy) {
        ReleaseCredential(info);
        SwitchProxy(info);
        info->SetNumUsedProxies(info->num_used_proxies() + 1);
        SetUrlOptions(info);
      }
      if (switch_host) {
        ReleaseCredential(info);
        if (was_metalink) {
          SwitchMetalink(info);
          info->SetNumUsedMetalinks(num_used_hosts + 1);
        } else {
          SwitchHost(info);
          info->SetNumUsedHosts(num_used_hosts + 1);
        }
        SetUrlOptions(info);
      }
    }  // end !sharding

    SetRangeOption(info, info->curl_handle());

    // Never retry on a pooled keep-alive connection.  A middlebox (conntrack
    // idle timeout, DPI black hole) silently kills idle flows without a FIN or
    // RST, so curl cannot tell that the pooled connections are dead; retrying
    // on one of them burns another full timeout and then wrongly fails over
    // away from a healthy proxy or host.
    curl_easy_setopt(info->curl_handle(), CURLOPT_FRESH_CONNECT, 1L);

    if (failover_indefinitely_) {
      // try again, breaking if there's a cvmfs reload happening and we are in a
      // proxy failover. This will EIO the call application.
      return !Interrupted(fqrn_, info);
    }
    return true;  // try again
  }

verify_and_finalize_stop:
  // Finalize, flush destination file.
  ReleaseCredential(info);

  // In parallel-decompress mode (data_tube_ valid) the caller has not yet
  // pushed bytes through the sink / zstream — those operations happen on
  // the caller thread when it pops the queued chunks. Flushing the sink
  // and tearing down the zstream here would race the caller's decompress
  // and corrupt the output. Defer both to the caller.
  const bool defer_finalize = info->IsValidDataTube();
  if (!defer_finalize) {
    if (info->sink() != NULL && info->sink()->Flush() != 0) {
      info->SetErrorCode(kFailLocalIO);
    }
    if (info->compressed())
      zlib::DecompressFini(info->GetZstreamPtr());
  }

  if (info->headers()) {
    header_lists_->PutList(info->headers());
    info->SetHeaders(NULL);
  }

  return false;  // stop transfer and return to Fetch()
}

DownloadManager::~DownloadManager() {
  // cleaned up fini
  if (sharding_policy_.UseCount() > 0) {
    sharding_policy_.Reset();
  }
  if (health_check_.UseCount() > 0) {
    if (health_check_.Unique()) {
      LogCvmfs(kLogDownload, kLogDebug,
               "(manager '%s') Stopping healthcheck thread", name_.c_str());
      health_check_->StopHealthcheck();
    }
    health_check_.Reset();
  }

  if (multi_threaded_.load() == 1) {
    // Shutdown I/O thread
    pipe_terminate_->Write(kPipeTerminateSignal);
    pthread_join(thread_download_, NULL);
    // All handles are removed from the multi stack
    pipe_terminate_.reset();
    pipe_jobs_.reset();
  }

  for (set<CURL *>::iterator i = pool_handles_idle_->begin(),
                             iEnd = pool_handles_idle_->end();
       i != iEnd;
       ++i) {
    curl_easy_cleanup(*i);
  }

  delete pool_handles_idle_;
  delete pool_handles_inuse_;
  curl_multi_cleanup(curl_multi_);

  delete header_lists_;
  if (user_agent_)
    free(user_agent_);

  delete counters_;
  delete opt_host_.chain;
  delete opt_host_chain_rtt_;
  delete opt_proxy_groups_;

  curl_global_cleanup();
  delete resolver_;

  // old destructor
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
  cernvm_id += string(CVMFS_VERSION);
  if (getenv("CERNVM_UUID") != NULL) {
    cernvm_id += " "
                 + sanitizer::InputSanitizer("az AZ 09 -")
                       .Filter(getenv("CERNVM_UUID"));
  }
  user_agent_ = strdup(cernvm_id.c_str());

  header_lists_ = new HeaderLists();

  default_headers_ = header_lists_->GetList("Connection: Keep-Alive");
  header_lists_->AppendHeader(default_headers_, "Pragma:");
  header_lists_->AppendHeader(default_headers_, user_agent_);
}

DownloadManager::DownloadManager(const unsigned max_pool_handles,
                                 const perf::StatisticsTemplate &statistics,
                                 const std::string &name)
    : prng_(Prng())
    , pool_handles_idle_(new set<CURL *>)
    , pool_handles_inuse_(new set<CURL *>)
    , pool_max_handles_(max_pool_handles)
    , pipe_terminate_(nullptr)
    , pipe_jobs_(nullptr)
    , watch_fds_(nullptr)
    , watch_fds_size_(0)
    , watch_fds_inuse_(0)
    , watch_fds_max_(4 * max_pool_handles)
    , curl_timeout_ms_(-1)
    , opt_timeout_proxy_(5)
    , opt_timeout_direct_(10)
    , opt_low_speed_limit_(1024)
    , opt_tcp_keepalive_(kDefaultTcpKeepaliveSecs)
    , opt_parallel_fetch_(0)
    , opt_fresh_connect_until_(0)
    , opt_max_retries_(0)
    , opt_backoff_init_ms_(0)
    , opt_backoff_max_ms_(0)
    , enable_info_header_(false)
    , opt_ipv4_only_(false)
    , follow_redirects_(false)
    , ignore_signature_failures_(false)
    , enable_http_tracing_(false)
    , opt_metalink_(NULL, 0, 0, 0)
    , opt_metalink_timestamp_link_(0)
    , opt_host_(NULL, 0, 0, 0)
    , opt_host_chain_rtt_(NULL)
    , opt_proxy_groups_(NULL)
    , opt_proxy_groups_current_(0)
    , opt_proxy_groups_current_burned_(0)
    , opt_proxy_groups_fallback_(0)
    , opt_num_proxies_(0)
    , opt_proxy_mandatory_(false)
    , opt_proxy_shard_(false)
    , failover_indefinitely_(false)
    , name_(name)
    , opt_ip_preference_(dns::kIpPreferSystem)
    , opt_timestamp_backup_proxies_(0)
    , opt_timestamp_failover_proxies_(0)
    , opt_proxy_groups_reset_after_(0)
    , credentials_attachment_(NULL)
    , counters_(new Counters(statistics)) {
  multi_threaded_.store(0);

  lock_options_ = reinterpret_cast<pthread_mutex_t *>(
      smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_options_, NULL);
  assert(retval == 0);
  lock_synchronous_mode_ = reinterpret_cast<pthread_mutex_t *>(
      smalloc(sizeof(pthread_mutex_t)));
  retval = pthread_mutex_init(lock_synchronous_mode_, NULL);
  assert(retval == 0);

  retval = curl_global_init(CURL_GLOBAL_ALL);
  assert(retval == CURLE_OK);

  InitHeaders();

  curl_multi_ = curl_multi_init();
  assert(curl_multi_ != NULL);
  curl_multi_setopt(curl_multi_, CURLMOPT_SOCKETFUNCTION, CallbackCurlSocket);
  curl_multi_setopt(curl_multi_, CURLMOPT_TIMERFUNCTION, CallbackCurlTimer);
  curl_multi_setopt(curl_multi_, CURLMOPT_TIMERDATA, static_cast<void *>(this));
  curl_multi_setopt(curl_multi_, CURLMOPT_SOCKETDATA,
                    static_cast<void *>(this));
  curl_multi_setopt(curl_multi_, CURLMOPT_MAXCONNECTS, watch_fds_max_);
  curl_multi_setopt(curl_multi_, CURLMOPT_MAX_TOTAL_CONNECTIONS,
                    pool_max_handles_);

  prng_.InitLocaltime();

  // Name resolving
  if ((getenv("CVMFS_IPV4_ONLY") != NULL)
      && (strlen(getenv("CVMFS_IPV4_ONLY")) > 0)) {
    opt_ipv4_only_ = true;
  }
  resolver_ = dns::NormalResolver::Create(opt_ipv4_only_, kDnsDefaultRetries,
                                          kDnsDefaultTimeoutMs);
  assert(resolver_);
}

/**
 * Spawns the I/O worker thread and switches the module in multi-threaded mode.
 * No way back except Fini(); Init();
 */
void DownloadManager::Spawn() {
  pipe_terminate_ = std::unique_ptr<Pipe<kPipeThreadTerminator> >(
      new Pipe<kPipeThreadTerminator>());
  pipe_jobs_ = std::unique_ptr<Pipe<kPipeDownloadJobs> >(
      new Pipe<kPipeDownloadJobs>());

  const int retval = pthread_create(&thread_download_, NULL, MainDownload,
                                    static_cast<void *>(this));
  assert(retval == 0);

  multi_threaded_.fetch_add(1);

  if (health_check_.UseCount() > 0) {
    LogCvmfs(kLogDownload, kLogDebug,
             "(manager '%s') Starting healthcheck thread", name_.c_str());
    health_check_->StartHealthcheck();
  }
}


/**
 * Downloads data from an insecure outside channel (currently HTTP or file).
 */
namespace {

/** Minimum compressed object size for a parallel multi-range fetch */
const int64_t kParallelFetchMinBytes = 2 * 1024 * 1024;
/** Target size of each range of a parallel fetch */
const int64_t kParallelFetchRangeBytes = 1024 * 1024;

struct SubFetch {
  DownloadManager *mgr;
  JobInfo *job;
  Failures result;
};

void *MainSubFetch(void *data) {
  SubFetch *sub = static_cast<SubFetch *>(data);
  sub->result = sub->mgr->Fetch(sub->job);
  return NULL;
}

}  // anonymous namespace


/**
 * Downloads a large object as several concurrent range requests (one TCP
 * flow each) and assembles, verifies and decompresses it on the caller
 * thread.  On a link where a single flow is slow (latency, loss, per-flow
 * caps) this multiplies the throughput of catalogs and other big single
 * objects, which unlike chunked files cannot profit from read-ahead.
 *
 * Returns kFailUnsupportedProtocol if the object is not eligible (too small,
 * no ranges supported, ...) so that the caller falls back to a plain fetch.
 */
Failures DownloadManager::FetchParallel(JobInfo *info) {
  unsigned num_conns;
  {
    const MutexLockGuard m(lock_options_);
    num_conns = opt_parallel_fetch_;
  }
  if ((num_conns < 2) || !info->parallel_ok()
      || (atomic_xadd32(&multi_threaded_, 0) != 1)
      || (info->expected_hash() == NULL) || (info->sink() == NULL)
      || info->sink()->RequiresReserve() || info->head_request()
      || (info->range_offset() != -1) || info->force_nocache()) {
    return kFailUnsupportedProtocol;
  }

  // Size of the compressed object
  JobInfo head(info->url(), info->probe_hosts());
  *(head.GetPidPtr()) = info->pid();
  *(head.GetUidPtr()) = info->uid();
  *(head.GetGidPtr()) = info->gid();
  head.SetPathInfo(info->path_info());
  head.SetInterruptCue(info->interrupt_cue());
  if ((Fetch(&head) != kFailOk)
      || (head.content_length() < kParallelFetchMinBytes))
    return kFailUnsupportedProtocol;
  const int64_t total = head.content_length();
  num_conns = std::min(static_cast<int64_t>(num_conns),
                       (total + kParallelFetchRangeBytes - 1)
                           / kParallelFetchRangeBytes);
  const int64_t range = (total + num_conns - 1) / num_conns;
  LogCvmfs(kLogDownload, kLogDebug,
           "(manager '%s' - id %" PRId64 ") parallel fetch of %s: "
           "%" PRId64 " bytes in %u ranges",
           name_.c_str(), info->id(), info->url()->c_str(), total, num_conns);

  // One raw (uncompressed, unhashed) range per connection
  std::vector<cvmfs::MemSink *> sinks;
  std::vector<JobInfo *> jobs;
  std::vector<SubFetch> subs(num_conns);
  std::vector<pthread_t> threads(num_conns);
  for (unsigned i = 0; i < num_conns; ++i) {
    const int64_t offset = static_cast<int64_t>(i) * range;
    const int64_t size = std::min(range, total - offset);
    cvmfs::MemSink *sink = new cvmfs::MemSink(static_cast<size_t>(size));
    JobInfo *job = new JobInfo(info->url(), false /* compressed */,
                               info->probe_hosts(), NULL /* hash */, sink);
    job->SetRangeOffset(offset);
    job->SetRangeSize(size);
    *(job->GetPidPtr()) = info->pid();
    *(job->GetUidPtr()) = info->uid();
    *(job->GetGidPtr()) = info->gid();
    // Without these the CVMFS_INFO_HEADER shows "path=-" for every range and
    // a cancellation is ignored until all range threads have finished.
    job->SetPathInfo(info->path_info());
    job->SetInterruptCue(info->interrupt_cue());
    sinks.push_back(sink);
    jobs.push_back(job);
    subs[i].mgr = this;
    subs[i].job = job;
    subs[i].result = kFailOther;
    const int retval = pthread_create(&threads[i], NULL, MainSubFetch,
                                      &subs[i]);
    assert(retval == 0);
  }
  Failures result = kFailOk;
  for (unsigned i = 0; i < num_conns; ++i) {
    pthread_join(threads[i], NULL);
    if (subs[i].result != kFailOk) {
      // A range that did not make it (after its own retries and fail-over)
      // is not fatal: the plain single-stream download resumes on its own
      result = kFailUnsupportedProtocol;
    } else if ((jobs[i]->http_code() != 206)
               || (static_cast<int64_t>(sinks[i]->pos())
                   != jobs[i]->range_size())) {
      // Range not honoured (200 with the full body) or short: not usable
      result = kFailUnsupportedProtocol;
    }
  }

  // Verify the content hash over the concatenated ranges
  if (result == kFailOk) {
    shash::ContextPtr ctx(info->expected_hash()->algorithm);
    ctx.buffer = alloca(ctx.size);
    shash::Init(ctx);
    for (unsigned i = 0; i < num_conns; ++i)
      shash::Update(sinks[i]->data(), sinks[i]->pos(), ctx);
    shash::Any hash(info->expected_hash()->algorithm);
    shash::Final(ctx, &hash);
    if (hash != *(info->expected_hash())) {
      LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
               "(manager '%s' - id %" PRId64 ") "
               "parallel fetch of %s: hash mismatch, falling back",
               name_.c_str(), info->id(), info->url()->c_str());
      result = kFailUnsupportedProtocol;
    }
  }

  // Decompress / copy into the caller's sink
  if (result == kFailOk) {
    if (info->compressed()) {
      z_stream zstream;
      zlib::DecompressInit(&zstream);
      for (unsigned i = 0; (i < num_conns) && (result == kFailOk); ++i) {
        const zlib::StreamStates retval = zlib::DecompressZStream2Sink(
            sinks[i]->data(), static_cast<int64_t>(sinks[i]->pos()), &zstream,
            info->sink());
        if (retval == zlib::kStreamDataError)
          result = kFailBadData;
        else if (retval == zlib::kStreamIOError)
          result = kFailLocalIO;
      }
      zlib::DecompressFini(&zstream);
    } else {
      for (unsigned i = 0; (i < num_conns) && (result == kFailOk); ++i) {
        const int64_t written = info->sink()->Write(sinks[i]->data(),
                                                    sinks[i]->pos());
        if (written != static_cast<int64_t>(sinks[i]->pos()))
          result = kFailLocalIO;
      }
    }
    if ((result == kFailOk) && (info->sink()->Flush() != 0))
      result = kFailLocalIO;
    if (result != kFailOk)
      info->sink()->Purge();
  }

  for (unsigned i = 0; i < num_conns; ++i) {
    delete jobs[i];
    delete sinks[i];
  }
  if (result == kFailUnsupportedProtocol) {
    // Not usable as a parallel fetch; the caller restarts from scratch
    info->sink()->Reset();
  }
  info->SetErrorCode(result);
  return result;
}


Failures DownloadManager::Fetch(JobInfo *info) {
  assert(info != NULL);
  assert(info->url() != NULL);

  Failures result;
  result = PrepareDownloadDestination(info);
  if (result != kFailOk)
    return result;

  // Try the ranged, multi-connection path first.  kFailUnsupportedProtocol is
  // its way of saying "this job is not a candidate", not a real failure, so
  // anything else -- success or a genuine error -- is the final answer and
  // only that sentinel falls through to the ordinary single-stream fetch.
  if (opt_parallel_fetch_ > 1) {
    result = FetchParallel(info);
    if (result != kFailUnsupportedProtocol)
      return result;
  }

  if (info->expected_hash()) {
    const shash::Algorithms algorithm = info->expected_hash()->algorithm;
    info->GetHashContextPtr()->algorithm = algorithm;
    info->GetHashContextPtr()->size = shash::GetContextSize(algorithm);
    info->GetHashContextPtr()->buffer = alloca(info->hash_context().size);
  }

  // Clear out any saved Links in case this will be a metalink request and
  // previous non-metalink requests sent Link headers
  info->SetLink("");

  // Prepare cvmfs-info: header, allocate string on the stack
  info->SetInfoHeader(NULL);
  if (enable_info_header_) {
    const string header_info = info->GetInfoHeaderContents(
        info_header_template_);
    if (header_info != "") {
      const char * const header_name = "cvmfs-info: ";
      const size_t header_name_len = strlen(header_name);
      const size_t info_len = header_info.length();
      char *buf = static_cast<char *>(alloca(header_name_len + info_len + 1));
      memcpy(buf, header_name, header_name_len);
      memcpy(buf + header_name_len, header_info.c_str(), info_len);
      buf[header_name_len + info_len] = '\0';
      info->SetInfoHeader(buf);
    }
  }

  if (enable_http_tracing_) {
    const std::string str_pid = "X-CVMFS-PID: " + StringifyInt(info->pid());
    const std::string str_gid = "X-CVMFS-GID: " + StringifyUint(info->gid());
    const std::string str_uid = "X-CVMFS-UID: " + StringifyUint(info->uid());

    // will be auto freed at the end of this function Fetch(JobInfo *info)
    info->SetTracingHeaderPid(static_cast<char *>(alloca(str_pid.size() + 1)));
    info->SetTracingHeaderGid(static_cast<char *>(alloca(str_gid.size() + 1)));
    info->SetTracingHeaderUid(static_cast<char *>(alloca(str_uid.size() + 1)));

    memcpy(info->tracing_header_pid(), str_pid.c_str(), str_pid.size() + 1);
    memcpy(info->tracing_header_gid(), str_gid.c_str(), str_gid.size() + 1);
    memcpy(info->tracing_header_uid(), str_uid.c_str(), str_uid.size() + 1);
  }

  if (multi_threaded_.load() == 1) {
    if (!info->IsValidPipeJobResults()) {
      info->CreatePipeJobResults();
    }
    if (!info->IsValidDataTube()) {
      info->CreateDataTube();
    }

    // LogCvmfs(kLogDownload, kLogDebug, "send job to thread, pipe %d %d",
    //          info->wait_at[0], info->wait_at[1]);
    pipe_jobs_->Write<JobInfo *>(info);

    // Decompress / copy chunks on this caller thread (in parallel across
    // concurrent Fetch() callers) instead of on the single MainDownload
    // thread. Track decompression errors locally and apply them at the end
    // — VerifyAndFinalize already ran by the time kActionStop arrives.
    Failures decompress_err = kFailOk;
    do {
      DataTubeElement *ele = info->GetDataTubePtr()->PopFront();

      if (ele->action == kActionStop) {
        delete ele;
        break;
      }

      if (ele->action == kActionReset) {
        // The chunks popped so far belonged to a download attempt that was
        // superseded by a retry / host fail-over (VerifyAndFinalize enqueued
        // this marker). Discard their — possibly corrupt — decompressed output
        // and start fresh so the next attempt's bytes are processed cleanly.
        if (info->compressed()) {
          zlib::DecompressFini(info->GetZstreamPtr());
          zlib::DecompressInit(info->GetZstreamPtr());
        }
        if (info->sink() != NULL && info->sink()->Reset() != 0) {
          decompress_err = kFailLocalIO;
        } else {
          decompress_err = kFailOk;
        }
        delete ele;
        continue;
      }

      if (decompress_err == kFailOk && ele->size > 0) {
        if (info->compressed()) {
          const zlib::StreamStates retval = zlib::DecompressZStream2Sink(
              ele->data, static_cast<int64_t>(ele->size), info->GetZstreamPtr(),
              info->sink());
          if (retval == zlib::kStreamDataError) {
            LogCvmfs(kLogDownload, kLogSyslogErr,
                     "(id %" PRId64 ") failed to decompress %s", info->id(),
                     info->url()->c_str());
            decompress_err = kFailBadData;
          } else if (retval == zlib::kStreamIOError) {
            LogCvmfs(kLogDownload, kLogSyslogErr,
                     "(id %" PRId64 ") decompressing %s, local IO error",
                     info->id(), info->url()->c_str());
            decompress_err = kFailLocalIO;
          }
        } else {
          const int64_t written = info->sink()->Write(ele->data, ele->size);
          if (written < 0 || static_cast<uint64_t>(written) != ele->size) {
            LogCvmfs(kLogDownload, kLogDebug,
                     "(id %" PRId64 ") sink write failed for %s (%ld of %zu)",
                     info->id(), info->url()->c_str(),
                     static_cast<long>(written), ele->size);
            decompress_err = kFailLocalIO;
          }
        }
      }
      delete ele;
    } while (true);

    info->GetPipeJobResultPtr()->Read<download::Failures>(&result);

    // Caller-side finalize (deferred from VerifyAndFinalize). Must run after
    // all kActionDecompress chunks have been popped and decompressed.
    if (result == kFailOk && decompress_err == kFailOk) {
      if (info->sink() != NULL && info->sink()->Flush() != 0) {
        decompress_err = kFailLocalIO;
      }
    }
    if (info->compressed()) {
      zlib::DecompressFini(info->GetZstreamPtr());
    }
    if (result == kFailOk && decompress_err != kFailOk) {
      result = decompress_err;
      info->SetErrorCode(decompress_err);
      if (info->sink() != NULL) {
        info->sink()->Purge();
      }
    }
    // LogCvmfs(kLogDownload, kLogDebug, "got result %d", result);
  } else {
    const MutexLockGuard l(lock_synchronous_mode_);
    CURL *handle = AcquireCurlHandle();
    InitializeRequest(info, handle);
    SetUrlOptions(info);
    // curl_easy_setopt(handle, CURLOPT_VERBOSE, 1);
    int retval;
    do {
      retval = curl_easy_perform(handle);
      perf::Inc(counters_->n_requests);
      double elapsed;
      if (curl_easy_getinfo(handle, CURLINFO_TOTAL_TIME, &elapsed)
          == CURLE_OK) {
        perf::Xadd(counters_->sz_transfer_time,
                   static_cast<int64_t>(elapsed * 1000));
      }
    } while (VerifyAndFinalize(retval, info));
    result = info->error_code();
    // Prevent the handle from being added to the idle list, which
    // avoids that it is mistakenly picked up by the multi handle later
    // in multi-threaded context. This, in turn, would fail to wait for
    // for resolver threads that meanwhile vanished due to a fork()
    // in Daemonize()
    ReleaseCurlHandle(info->curl_handle(), false /* allow_reuse */);
  }

  if (result != kFailOk) {
    LogCvmfs(kLogDownload, kLogDebug,
             "(manager '%s' - id %" PRId64 ") "
             "download failed (error %d - %s)",
             name_.c_str(), info->id(), result, Code2Ascii(result));

    if (info->sink() != NULL) {
      info->sink()->Purge();
    }
  }

  return result;
}


/**
 * Used by the client to connect the authz session manager to the download
 * manager.
 */
void DownloadManager::SetCredentialsAttachment(CredentialsAttachment *ca) {
  const MutexLockGuard m(lock_options_);
  credentials_attachment_ = ca;
}

/**
 * Gets the DNS sever.
 */
std::string DownloadManager::GetDnsServer() const { return opt_dns_server_; }

/**
 * Sets a DNS server.  Only for testing as it cannot be reverted to the system
 * default.
 */
void DownloadManager::SetDnsServer(const string &address) {
  if (!address.empty()) {
    const MutexLockGuard m(lock_options_);
    opt_dns_server_ = address;
    assert(!opt_dns_server_.empty());

    vector<string> servers;
    servers.push_back(address);
    const bool retval = resolver_->SetResolvers(servers);
    assert(retval);
  }
  LogCvmfs(kLogDownload, kLogSyslog, "(manager '%s') set nameserver to %s",
           name_.c_str(), address.c_str());
}


/**
 * Sets the DNS query timeout parameters.
 */
void DownloadManager::SetDnsParameters(const unsigned retries,
                                       const unsigned timeout_ms) {
  const MutexLockGuard m(lock_options_);
  if ((resolver_->retries() == retries)
      && (resolver_->timeout_ms() == timeout_ms)) {
    return;
  }
  delete resolver_;
  resolver_ = NULL;
  resolver_ = dns::NormalResolver::Create(opt_ipv4_only_, retries, timeout_ms);
  assert(resolver_);
}


void DownloadManager::SetDnsTtlLimits(const unsigned min_seconds,
                                      const unsigned max_seconds) {
  const MutexLockGuard m(lock_options_);
  resolver_->set_min_ttl(min_seconds);
  resolver_->set_max_ttl(max_seconds);
}


void DownloadManager::SetIpPreference(dns::IpPreference preference) {
  const MutexLockGuard m(lock_options_);
  opt_ip_preference_ = preference;
}


/**
 * Sets two timeout values for proxied and for direct connections, respectively.
 * The timeout counts for all sorts of connection phases,
 * DNS, HTTP connect, etc.
 */
void DownloadManager::SetTimeout(const unsigned seconds_proxy,
                                 const unsigned seconds_direct) {
  const MutexLockGuard m(lock_options_);
  opt_timeout_proxy_ = seconds_proxy;
  opt_timeout_direct_ = seconds_direct;
}


/**
 * Sets contains the average transfer speed in bytes per second that the
 * transfer should be below during CURLOPT_LOW_SPEED_TIME seconds for libcurl to
 * consider it to be too slow and abort.  Only effective for new connections.
 */
void DownloadManager::SetLowSpeedLimit(const unsigned low_speed_limit) {
  const MutexLockGuard m(lock_options_);
  opt_low_speed_limit_ = low_speed_limit;
}

void DownloadManager::SetTcpKeepalive(const unsigned idle_seconds) {
  const MutexLockGuard m(lock_options_);
  opt_tcp_keepalive_ = idle_seconds;
}

void DownloadManager::SetParallelFetch(const unsigned num_connections) {
  const MutexLockGuard m(lock_options_);
  opt_parallel_fetch_ = num_connections;
}


/**
 * Receives the currently active timeout values.
 */
void DownloadManager::GetTimeout(unsigned *seconds_proxy,
                                 unsigned *seconds_direct) {
  const MutexLockGuard m(lock_options_);
  *seconds_proxy = opt_timeout_proxy_;
  *seconds_direct = opt_timeout_direct_;
}


/**
 * Parses a list of ';'-separated hosts for the metalink chain.  The empty
 * string removes the metalink list.
 */
void DownloadManager::SetMetalinkChain(const string &metalink_list) {
  SetMetalinkChain(SplitString(metalink_list, ';'));
}


void DownloadManager::SetMetalinkChain(
    const std::vector<std::string> &metalink_list) {
  const MutexLockGuard m(lock_options_);
  opt_metalink_.timestamp_backup = 0;
  delete opt_metalink_.chain;
  opt_metalink_.current = 0;

  if (metalink_list.empty()) {
    opt_metalink_.chain = NULL;
    return;
  }

  opt_metalink_.chain = new vector<string>(metalink_list);
}


/**
 * Retrieves the currently set chain of metalink hosts and the currently
 * used metalink host.
 */
void DownloadManager::GetMetalinkInfo(vector<string> *metalink_chain,
                                      unsigned *current_metalink) {
  const MutexLockGuard m(lock_options_);
  if (opt_metalink_.chain) {
    if (current_metalink) {
      *current_metalink = opt_metalink_.current;
    }
    if (metalink_chain) {
      *metalink_chain = *opt_metalink_.chain;
    }
  }
}


/**
 * Parses a list of ';'-separated hosts for the host chain.  The empty string
 * removes the host list.
 */
void DownloadManager::SetHostChain(const string &host_list) {
  SetHostChain(SplitString(host_list, ';'));
}


void DownloadManager::SetHostChain(const std::vector<std::string> &host_list) {
  const MutexLockGuard m(lock_options_);
  opt_host_.timestamp_backup = 0;
  delete opt_host_.chain;
  delete opt_host_chain_rtt_;
  opt_host_.current = 0;

  if (host_list.empty()) {
    opt_host_.chain = NULL;
    opt_host_chain_rtt_ = NULL;
    return;
  }

  opt_host_.chain = new vector<string>(host_list);
  opt_host_chain_rtt_ = new vector<int>(opt_host_.chain->size(),
                                        kProbeUnprobed);
  // LogCvmfs(kLogDownload, kLogSyslog, "using host %s",
  //          (*opt_host_.chain)[0].c_str());
}


/**
 * Retrieves the currently set chain of hosts, their round trip times, and the
 * currently used host.
 */
void DownloadManager::GetHostInfo(vector<string> *host_chain, vector<int> *rtt,
                                  unsigned *current_host) {
  const MutexLockGuard m(lock_options_);
  if (opt_host_.chain) {
    if (current_host) {
      *current_host = opt_host_.current;
    }
    if (host_chain) {
      *host_chain = *opt_host_.chain;
    }
    if (rtt) {
      *rtt = *opt_host_chain_rtt_;
    }
  }
}


/**
 * Jumps to the next proxy in the ring of forward proxy servers.
 * Selects one randomly from a load-balancing group.
 *
 * Allow for the fact that the proxy may have already been failed by
 * another transfer, or that the proxy may no longer be part of the
 * current load-balancing group.
 */
void DownloadManager::SwitchProxy(JobInfo *info) {
  const MutexLockGuard m(lock_options_);

  if (!opt_proxy_groups_) {
    return;
  }

  // Fail any matching proxies within the current load-balancing group
  vector<ProxyInfo> *group = current_proxy_group();
  // An empty group would make the group_size - burned subtraction below
  // underflow and index off the end of the vector.  A chain can be left in
  // that state by a DNS refresh, see UpdateProxiesUnlocked().
  if ((group == NULL) || group->empty()
      || (opt_proxy_groups_current_burned_ > group->size())) {
    return;
  }
  const unsigned group_size = group->size();
  unsigned failed = 0;
  for (unsigned i = 0; i < group_size - opt_proxy_groups_current_burned_; ++i) {
    if (info && (info->proxy() == (*group)[i].url)) {
      // Move to list of failed proxies
      opt_proxy_groups_current_burned_++;
      swap((*group)[i],
           (*group)[group_size - opt_proxy_groups_current_burned_]);
      perf::Inc(counters_->n_proxy_failover);
      failed++;
    }
  }

  // Do nothing more unless at least one proxy was marked as failed
  if (!failed)
    return;

  // If all proxies from the current load-balancing group are burned, switch to
  // another group
  if (opt_proxy_groups_current_burned_ == group->size()) {
    opt_proxy_groups_current_burned_ = 0;
    if (opt_proxy_groups_->size() > 1) {
      const unsigned next_group = (opt_proxy_groups_current_ + 1)
                                  % opt_proxy_groups_->size();
      // Leaving the local proxy set -- for an unproxied connection, or for an
      // off-site fallback proxy -- needs evidence that the local proxy is
      // actually unreachable.  A proxy that is merely saturated answers
      // requests, just too slowly, and reports itself as "too slow" or as a
      // short transfer; that is a throughput symptom, not a dead peer, and
      // escalating on it is what silently moved traffic off-site under load.
      // Stay on the group instead; the burn counter has just been cleared, so
      // the proxy is usable again and the normal retry and backoff path
      // decides whether the request ultimately fails.
      // Only a peer that never answered, or one whose name no longer
      // resolves, is grounds for leaving the local proxy.  Note this is not
      // the same as kFailProxyConnection: libcurl reports a refused connect
      // as CURLE_COULDNT_CONNECT but an unanswered one as
      // CURLE_OPERATION_TIMEDOUT, so keying on the error code alone gets both
      // cases exactly backwards.
      const bool proxy_unreachable = info->peer_unresponsive()
                                     || (info->error_code()
                                         == kFailProxyResolve);
      const bool would_escalate = IsEscalatedProxyGroup(next_group)
                                  && !IsEscalatedProxyGroup(
                                         opt_proxy_groups_current_);
      if (would_escalate && !proxy_unreachable) {
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
                 "(manager '%s' - id %" PRId64 ") "
                 "keeping proxy group %u: '%s' does not show the proxy to be "
                 "unreachable, so not falling back to group %u",
                 name_.c_str(), info->id(), opt_proxy_groups_current_,
                 Code2Ascii(info->error_code()), next_group);
        UpdateProxiesUnlocked("failed proxy, escalation withheld");
        return;
      }
      opt_proxy_groups_current_ = next_group;
      // Remember the timestamp of switching to backup proxies
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
    // Record failover time
    if (opt_proxy_groups_reset_after_ > 0) {
      if (opt_timestamp_failover_proxies_ == 0)
        opt_timestamp_failover_proxies_ = time(NULL);
    }
  }

  UpdateProxiesUnlocked("failed proxy");
  LogCvmfs(kLogDownload, kLogDebug,
           "(manager '%s' - id %" PRId64 ") "
           "%lu proxies remain in group",
           name_.c_str(), info->id(),
           current_proxy_group()->size() - opt_proxy_groups_current_burned_);
}


/**
 * Switches to the next host in the chain.  If jobinfo is set, switch only if
 * the current host is identical to the one used by jobinfo, otherwise another
 * transfer has already done the switch.
 */
void DownloadManager::SwitchHostInfo(const std::string &typ,
                                     HostInfo &info,
                                     JobInfo *jobinfo) {
  const MutexLockGuard m(lock_options_);

  if (!info.chain || (info.chain->size() == 1)) {
    return;
  }

  if (jobinfo) {
    int lastused;
    if (typ == "host") {
      lastused = jobinfo->current_host_chain_index();
    } else {
      lastused = jobinfo->current_metalink_chain_index();
    }
    if (lastused != info.current) {
      LogCvmfs(kLogDownload, kLogDebug,
               "(manager '%s' - id %" PRId64 ")"
               "don't switch %s, "
               "last used %s: %s, current %s: %s",
               name_.c_str(), jobinfo->id(), typ.c_str(), typ.c_str(),
               (*info.chain)[lastused].c_str(), typ.c_str(),
               (*info.chain)[info.current].c_str());
      return;
    }
  }

  string reason = "manually triggered";
  string info_id = "(manager '" + name_ + "'";
  if (jobinfo) {
    reason = download::Code2Ascii(jobinfo->error_code());
    info_id += " - id " + StringifyInt(jobinfo->id());
  }
  info_id += ")";

  const std::string old_host = (*info.chain)[info.current];
  info.current = (info.current + 1) % static_cast<int>(info.chain->size());
  if (typ == "host") {
    perf::Inc(counters_->n_host_failover);
  } else {
    perf::Inc(counters_->n_metalink_failover);
  }
  LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
           "%s switching %s from %s to %s (%s)", info_id.c_str(), typ.c_str(),
           old_host.c_str(), (*info.chain)[info.current].c_str(),
           reason.c_str());

  // Remember the timestamp of switching to backup host
  if (info.reset_after > 0) {
    if (info.current != 0) {
      if (info.timestamp_backup == 0)
        info.timestamp_backup = time(NULL);
    } else {
      info.timestamp_backup = 0;
    }
  }
}

void DownloadManager::SwitchHost(JobInfo *info) {
  SwitchHostInfo("host", opt_host_, info);
}

void DownloadManager::SwitchHost() { SwitchHost(NULL); }


void DownloadManager::SwitchMetalink(JobInfo *info) {
  SwitchHostInfo("metalink", opt_metalink_, info);
}


void DownloadManager::SwitchMetalink() { SwitchMetalink(NULL); }

bool DownloadManager::CheckMetalinkChain(time_t now) {
  return (opt_metalink_.chain
          && ((opt_metalink_timestamp_link_ == 0)
              || (static_cast<int64_t>((now == 0) ? time(NULL) : now)
                  > static_cast<int64_t>(opt_metalink_timestamp_link_
                                         + opt_metalink_.reset_after))));
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

  cvmfs::MemSink memsink;
  JobInfo info(&url, false, false, NULL, &memsink);
  for (retries = 0; retries < 2; ++retries) {
    for (i = 0; i < host_chain.size(); ++i) {
      url = host_chain[i] + "/.cvmfspublished";

      struct timeval tv_start, tv_end;
      gettimeofday(&tv_start, NULL);
      const Failures result = Fetch(&info);
      gettimeofday(&tv_end, NULL);
      memsink.Reset();
      if (result == kFailOk) {
        host_rtt[i] = static_cast<int>(DiffTimeSeconds(tv_start, tv_end)
                                       * 1000);
        LogCvmfs(kLogDownload, kLogDebug,
                 "(manager '%s' - id %" PRId64 ") "
                 "probing host %s had %dms rtt",
                 name_.c_str(), info.id(), url.c_str(), host_rtt[i]);
      } else {
        LogCvmfs(kLogDownload, kLogDebug,
                 "(manager '%s' - id %" PRId64 ") "
                 "error while probing host %s: %d %s",
                 name_.c_str(), info.id(), url.c_str(), result,
                 Code2Ascii(result));
        host_rtt[i] = INT_MAX;
      }
    }
  }

  SortTeam(&host_rtt, &host_chain);
  for (i = 0; i < host_chain.size(); ++i) {
    if (host_rtt[i] == INT_MAX)
      host_rtt[i] = kProbeDown;
  }

  const MutexLockGuard m(lock_options_);
  delete opt_host_.chain;
  delete opt_host_chain_rtt_;
  opt_host_.chain = new vector<string>(host_chain);
  opt_host_chain_rtt_ = new vector<int>(host_rtt);
  opt_host_.current = 0;
}

bool DownloadManager::GeoSortServers(std::vector<std::string> *servers,
                                     std::vector<uint64_t> *output_order) {
  if (!servers) {
    return false;
  }
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
    const std::string host = dns::ExtractHost((*servers)[i]);
    server_dns_names.push_back(host.empty() ? (*servers)[i] : host);
  }
  const std::string host_list = JoinStrings(server_dns_names, ",");

  vector<string> host_chain_shuffled;
  {
    // Protect against concurrent access to prng_
    const MutexLockGuard m(lock_options_);
    // Determine random hosts for the Geo-API query
    host_chain_shuffled = Shuffle(host_chain, &prng_);
  }
  // Request ordered list via Geo-API
  bool success = false;
  const unsigned max_attempts = std::min(host_chain_shuffled.size(), size_t(3));
  vector<uint64_t> geo_order(servers->size());
  for (unsigned i = 0; i < max_attempts; ++i) {
    const string url = host_chain_shuffled[i] + "/api/v1.0/geo/@proxy@/"
                       + host_list;
    LogCvmfs(kLogDownload, kLogDebug,
             "(manager '%s') requesting ordered server list from %s",
             name_.c_str(), url.c_str());
    cvmfs::MemSink memsink;
    JobInfo info(&url, false, false, NULL, &memsink);
    const Failures result = Fetch(&info);
    if (result == kFailOk) {
      const string order(reinterpret_cast<char *>(memsink.data()),
                         memsink.pos());
      memsink.Reset();
      const bool retval = ValidateGeoReply(order, servers->size(), &geo_order);
      if (!retval) {
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
                 "(manager '%s') retrieved invalid GeoAPI reply from %s [%s]",
                 name_.c_str(), url.c_str(), order.c_str());
      } else {
        LogCvmfs(kLogDownload, kLogDebug | kLogSyslog,
                 "(manager '%s') "
                 "geographic order of servers retrieved from %s",
                 name_.c_str(),
                 dns::ExtractHost(host_chain_shuffled[i]).c_str());
        // remove new line at end of "order"
        LogCvmfs(kLogDownload, kLogDebug, "order is %s",
                 Trim(order, true /* trim_newline */).c_str());
        success = true;
        break;
      }
    } else {
      LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
               "(manager '%s') GeoAPI request for %s failed with error %d [%s]",
               name_.c_str(), url.c_str(), result, Code2Ascii(result));
    }
  }
  if (!success) {
    LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
             "(manager '%s') "
             "failed to retrieve geographic order from stratum 1 servers",
             name_.c_str());
    return false;
  }

  if (output_order) {
    output_order->swap(geo_order);
  } else {
    std::vector<std::string> sorted_servers;
    sorted_servers.reserve(geo_order.size());
    for (unsigned i = 0; i < geo_order.size(); ++i) {
      const uint64_t orderval = geo_order[i];
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
  vector<vector<ProxyInfo> > proxy_chain;
  unsigned fallback_group;

  GetHostInfo(&host_chain, &host_rtt, &current_host);
  GetProxyInfo(&proxy_chain, NULL, &fallback_group);
  if ((host_chain.size() < 2) && ((proxy_chain.size() - fallback_group) < 2))
    return true;

  vector<string> host_names;
  for (unsigned i = 0; i < host_chain.size(); ++i)
    host_names.push_back(dns::ExtractHost(host_chain[i]));
  SortTeam(&host_names, &host_chain);
  const unsigned last_geo_host = host_names.size();

  if ((fallback_group == 0) && (last_geo_host > 1)) {
    // There are no non-fallback proxies, which means that the client
    // will always use the fallback proxies.  Add a keyword separator
    // between the hosts and fallback proxies so the geosorting service
    // will know to sort the hosts based on the distance from the
    // closest fallback proxy rather than the distance from the client.
    host_names.push_back("+PXYSEP+");
  }

  // Add fallback proxy names to the end of the host list
  const unsigned first_geo_fallback = host_names.size();
  for (unsigned i = fallback_group; i < proxy_chain.size(); ++i) {
    // We only take the first fallback proxy name from every group under the
    // assumption that load-balanced servers are at the same location
    host_names.push_back(proxy_chain[i][0].host.name());
  }

  std::vector<uint64_t> geo_order;
  const bool success = GeoSortServers(&host_names, &geo_order);
  if (!success) {
    // GeoSortServers already logged a failure message.
    return false;
  }

  // Re-install host chain and proxy chain
  const MutexLockGuard m(lock_options_);
  delete opt_host_.chain;
  opt_num_proxies_ = 0;
  opt_host_.chain = new vector<string>(host_chain.size());

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
    const uint64_t orderval = geo_order[i];
    if (orderval < static_cast<uint64_t>(last_geo_host)) {
      // LogCvmfs(kLogCvmfs, kLogSyslog, "this is orderval %u at host index
      // %u", orderval, hosti);
      (*opt_host_.chain)[hosti++] = host_chain[orderval];
    } else if (orderval >= static_cast<uint64_t>(first_geo_fallback)) {
      // LogCvmfs(kLogCvmfs, kLogSyslog,
      // "this is orderval %u at proxy index %u, using proxy_chain index %u",
      // orderval, proxyi, fallback_group + orderval - first_geo_fallback);
      (*proxy_groups)[proxyi] = proxy_chain[fallback_group + orderval
                                            - first_geo_fallback];
      opt_num_proxies_ += (*proxy_groups)[proxyi].size();
      proxyi++;
    }
  }

  opt_proxy_map_.clear();
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

  UpdateProxiesUnlocked("geosort");

  delete opt_host_chain_rtt_;
  opt_host_chain_rtt_ = new vector<int>(host_chain.size(), kProbeGeo);
  opt_host_.current = 0;

  return true;
}


/**
 * Validates a string of the form "1,4,2,3" representing in which order the
 * the expected_size number of hosts should be put for optimal geographic
 * proximity.  Returns false if the reply_order string is invalid, otherwise
 * fills in the reply_vals array with zero-based order indexes (e.g.
 * [0,3,1,2]) and returns true.
 */
bool DownloadManager::ValidateGeoReply(const string &reply_order,
                                       const unsigned expected_size,
                                       vector<uint64_t> *reply_vals) {
  if (reply_order.empty())
    return false;
  const sanitizer::InputSanitizer sanitizer("09 , \n");
  if (!sanitizer.IsValid(reply_order))
    return false;
  const sanitizer::InputSanitizer strip_newline("09 ,");
  vector<string> reply_strings = SplitString(strip_newline.Filter(reply_order),
                                             ',');
  vector<uint64_t> tmp_vals;
  for (unsigned i = 0; i < reply_strings.size(); ++i) {
    if (reply_strings[i].empty())
      return false;
    tmp_vals.push_back(String2Uint64(reply_strings[i]));
  }
  if (tmp_vals.size() != expected_size)
    return false;

  // Check if tmp_vals contains the number 1..n
  set<uint64_t> const coverage(tmp_vals.begin(), tmp_vals.end());
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
/**
 * Keeps DIRECT as a failover tier of its own but never as a peer of a real
 * proxy.
 *
 * "proxy;DIRECT" is the documented way of asking for "use the proxy, and only
 * if it has failed connect directly".  Load-balance groups, however, are
 * chosen from at random, so a DIRECT sharing a group with a real proxy
 * ("proxy|DIRECT") would send a share of the traffic unproxied while the
 * proxy is perfectly healthy.  DIRECT is therefore dropped from any group that
 * also holds a real proxy, and kept when it forms a group on its own.  Because
 * proxy groups are tried in order, the surviving DIRECT group is only reached
 * after every proxy in the preceding groups has been burned.
 *
 * *has_direct_group is set when such a DIRECT-only group survives.
 */
string DownloadManager::DemoteDirect(const string &proxy_list,
                                     bool *has_direct_group) {
  assert(has_direct_group);
  *has_direct_group = false;
  if (proxy_list == "")
    return "";

  const vector<string> groups = SplitString(proxy_list, ';');
  vector<string> kept_groups;
  for (unsigned i = 0; i < groups.size(); ++i) {
    const vector<string> members = SplitString(groups[i], '|');
    vector<string> real_members;
    bool group_has_direct = false;
    for (unsigned j = 0; j < members.size(); ++j) {
      if (members[j] == "DIRECT") {
        group_has_direct = true;
        continue;
      }
      if (members[j] == "")
        continue;
      real_members.push_back(members[j]);
    }
    if (!real_members.empty()) {
      kept_groups.push_back(JoinStrings(real_members, "|"));
    } else if (group_has_direct) {
      kept_groups.push_back("DIRECT");
      *has_direct_group = true;
    }
  }

  return JoinStrings(kept_groups, ";");
}


bool DownloadManager::StripDirect(const string &proxy_list,
                                  string *cleaned_list) {
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
void DownloadManager::SetProxyChain(const string &proxy_list,
                                    const string &fallback_proxy_list,
                                    const ProxySetModes set_mode) {
  const MutexLockGuard m(lock_options_);

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
  contains_direct = StripDirect(opt_proxy_fallback_list_,
                                &set_proxy_fallback_list);
  if (contains_direct) {
    LogCvmfs(kLogDownload, kLogSyslogWarn | kLogDebug,
             "(manager '%s') fallback proxies do not support DIRECT, removing",
             name_.c_str());
  }
  // Keep a DIRECT tier if one was configured, but never let DIRECT compete
  // with a healthy proxy inside a load-balance group.  Unlike the previous
  // behaviour, a configured DIRECT is no longer discarded merely because
  // fallback proxies also exist: "proxy;DIRECT" keeps meaning "use the proxy,
  // and connect directly only once it has failed".
  bool has_direct_group = false;
  set_proxy_list = DemoteDirect(opt_proxy_list_, &has_direct_group);
  if (set_proxy_list != opt_proxy_list_) {
    LogCvmfs(kLogDownload, kLogSyslog | kLogDebug,
             "(manager '%s') proxy chain '%s' normalised to '%s': DIRECT is "
             "only used as a last resort, never alongside a live proxy",
             name_.c_str(), opt_proxy_list_.c_str(), set_proxy_list.c_str());
  }

  // Direct connections are forbidden only when nothing asked for them.
  {
    string probe;
    StripDirect(set_proxy_list, &probe);
    const bool have_real_proxy = (probe != "")
                                 || (set_proxy_fallback_list != "");
    opt_proxy_mandatory_ = have_real_proxy && !has_direct_group;
  }

  // From this point on, use set_proxy_list and set_fallback_proxy_list as
  // effective proxy lists!

  opt_proxy_map_.clear();
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
  LogCvmfs(kLogDownload, kLogDebug,
           "(manager '%s') "
           "first fallback proxy group %u",
           name_.c_str(), opt_proxy_groups_fallback_);

  // Concatenate regular proxies and fallback proxies, both of which can be
  // empty.
  string all_proxy_list = set_proxy_list;
  if (set_proxy_fallback_list != "") {
    if (all_proxy_list != "")
      all_proxy_list += ";";
    all_proxy_list += set_proxy_fallback_list;
  }
  LogCvmfs(kLogDownload, kLogDebug, "(manager '%s') full proxy list %s",
           name_.c_str(), all_proxy_list.c_str());

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
      const string hostname = dns::ExtractHost(this_group[j]);
      // Save the hostname.  Leave empty (DIRECT) names so indexes will
      // match later.
      hostnames.push_back(hostname);
    }
  }
  vector<dns::Host> hosts;
  LogCvmfs(kLogDownload, kLogDebug,
           "(manager '%s') "
           "resolving %lu proxy addresses",
           name_.c_str(), hostnames.size());
  resolver_->ResolveMany(hostnames, &hosts);

  // Construct opt_proxy_groups_: traverse proxy list in same order and expand
  // names to resolved IP addresses.
  opt_proxy_groups_ = new vector<vector<ProxyInfo> >();
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
                 "(manager '%s') "
                 "failed to resolve IP addresses for %s (%d - %s)",
                 name_.c_str(), hosts[num_proxy].name().c_str(),
                 hosts[num_proxy].status(),
                 dns::Code2Ascii(hosts[num_proxy].status()));
        const dns::Host failed_host = dns::Host::ExtendDeadline(
            hosts[num_proxy], resolver_->min_ttl());
        infos.push_back(ProxyInfo(failed_host, this_group[j]));
        continue;
      }

      // IPv4 addresses have precedence
      set<string> const best_addresses = hosts[num_proxy].ViewBestAddresses(
          opt_ip_preference_);
      set<string>::const_iterator iter_ips = best_addresses.begin();
      for (; iter_ips != best_addresses.end(); ++iter_ips) {
        const string url_ip = dns::RewriteUrl(this_group[j], *iter_ips);
        infos.push_back(ProxyInfo(hosts[num_proxy], url_ip));

        if (sharding_policy_.UseCount() > 0) {
          sharding_policy_->AddProxy(url_ip);
        }
      }
    }
    opt_proxy_groups_->push_back(infos);
    opt_num_proxies_ += infos.size();
  }
  LogCvmfs(kLogDownload, kLogDebug,
           "(manager '%s') installed %u proxies in %lu load-balance groups",
           name_.c_str(), opt_num_proxies_, opt_proxy_groups_->size());
  opt_proxy_groups_current_ = 0;
  opt_proxy_groups_current_burned_ = 0;

  // Select random start proxy from the first group.
  if (opt_proxy_groups_->size() > 0) {
    // Select random start proxy from the first group.
    UpdateProxiesUnlocked("set random start proxy from the first proxy group");
  }
}


/**
 * Retrieves the proxy chain, optionally the currently active load-balancing
 *   group, and optionally the index of the first fallback proxy group.
 *   If there are no fallback proxies, the index will equal the size of
 *   the proxy chain.
 */
void DownloadManager::GetProxyInfo(vector<vector<ProxyInfo> > *proxy_chain,
                                   unsigned *current_group,
                                   unsigned *fallback_group) {
  assert(proxy_chain != NULL);
  const MutexLockGuard m(lock_options_);

  if (!opt_proxy_groups_) {
    const vector<vector<ProxyInfo> > empty_chain;
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

string DownloadManager::GetProxyList() { return opt_proxy_list_; }

string DownloadManager::GetFallbackProxyList() {
  return opt_proxy_fallback_list_;
}

/**
 * True when using this proxy group means the request has left the local proxy
 * set: either it is a DIRECT tier, so the request goes out unproxied, or it is
 * one of the fallback groups, which are by construction off-site.
 *
 * Moving between the regular groups is ordinary load-balancing failover and is
 * not escalation.
 */
bool DownloadManager::IsEscalatedProxyGroup(unsigned group_idx) const {
  if (!opt_proxy_groups_ || (group_idx >= opt_proxy_groups_->size()))
    return false;
  if (group_idx >= opt_proxy_groups_fallback_)
    return true;
  const std::vector<ProxyInfo> &group = (*opt_proxy_groups_)[group_idx];
  return !group.empty() && (group[0].url == "DIRECT");
}


/**
 * Choose proxy
 */
DownloadManager::ProxyInfo *DownloadManager::ChooseProxyUnlocked(
    const shash::Any *hash) {
  if (!opt_proxy_groups_)
    return NULL;

  const uint32_t key = (hash ? hash->Partial32() : 0);
  const map<uint32_t, ProxyInfo *>::iterator it = opt_proxy_map_.lower_bound(
      key);
  if (it == opt_proxy_map_.end())
    return NULL;
  ProxyInfo *proxy = it->second;

  return proxy;
}

/**
 * Update currently selected proxy
 */
void DownloadManager::UpdateProxiesUnlocked(const string &reason) {
  if (!opt_proxy_groups_)
    return;

  // Identify number of non-burned proxies within the current group
  vector<ProxyInfo> *group = current_proxy_group();
  // Nothing selectable: either no chain is installed or every proxy in the
  // current group is burned.  Both would otherwise reach prng_.Next(0) and
  // index an empty vector below, which is undefined behaviour rather than a
  // clean "no proxy available".
  if ((group == NULL) || group->empty()
      || (opt_proxy_groups_current_burned_ >= group->size())) {
    // The map holds ProxyInfo pointers into the group, so leaving the previous
    // selection in place here would hand ChooseProxyUnlocked() pointers into
    // entries that have just been erased.  Nothing is selectable, and that is
    // what the selection must say.
    opt_proxy_map_.clear();
    opt_proxies_.clear();
    return;
  }
  const unsigned num_alive = (group->size() - opt_proxy_groups_current_burned_);
  const string old_proxy = JoinStrings(opt_proxies_, "|");

  // Rebuild proxy map and URL list
  opt_proxy_map_.clear();
  opt_proxies_.clear();
  const uint32_t max_key = 0xffffffffUL;
  if (opt_proxy_shard_) {
    // Build a consistent map with multiple entries for each proxy
    for (unsigned i = 0; i < num_alive; ++i) {
      ProxyInfo *proxy = &(*group)[i];
      shash::Any proxy_hash(shash::kSha1);
      HashString(proxy->url, &proxy_hash);
      Prng prng;
      prng.InitSeed(proxy_hash.Partial32());
      for (unsigned j = 0; j < kProxyMapScale; ++j) {
        const std::pair<uint32_t, ProxyInfo *> entry(prng.Next(max_key), proxy);
        opt_proxy_map_.insert(entry);
      }
      const std::string proxy_name = proxy->host.name().empty()
                                         ? ""
                                         : " (" + proxy->host.name() + ")";
      opt_proxies_.push_back(proxy->url + proxy_name);
    }
    // Ensure lower_bound() finds a value for all keys
    if (opt_proxy_map_.empty())
      return;
    ProxyInfo *first_proxy = opt_proxy_map_.begin()->second;
    const std::pair<uint32_t, ProxyInfo *> last_entry(max_key, first_proxy);
    opt_proxy_map_.insert(last_entry);
  } else {
    // Build a map with a single entry for one randomly selected proxy
    const unsigned select = prng_.Next(num_alive);
    ProxyInfo *proxy = &(*group)[select];
    const std::pair<uint32_t, ProxyInfo *> entry(max_key, proxy);
    opt_proxy_map_.insert(entry);
    const std::string proxy_name = proxy->host.name().empty()
                                       ? ""
                                       : " (" + proxy->host.name() + ")";
    opt_proxies_.push_back(proxy->url + proxy_name);
  }
  sort(opt_proxies_.begin(), opt_proxies_.end());

  // Report any change in proxy usage
  const string new_proxy = JoinStrings(opt_proxies_, "|");
  const string curr_host = "Current host: "
                           + (opt_host_.chain
                                  ? (*opt_host_.chain)[opt_host_.current]
                                  : "");
  if (new_proxy != old_proxy) {
    LogCvmfs(kLogDownload, kLogDebug | kLogSyslogWarn,
             "(manager '%s') switching proxy from %s to %s. Reason: %s [%s]",
             name_.c_str(), (old_proxy.empty() ? "(none)" : old_proxy.c_str()),
             (new_proxy.empty() ? "(none)" : new_proxy.c_str()), reason.c_str(),
             curr_host.c_str());
  }
}

/**
 * Enable proxy sharding
 */
void DownloadManager::ShardProxies() {
  opt_proxy_shard_ = true;
  RebalanceProxiesUnlocked("enable sharding");
}

/**
 * Selects a new random proxy in the current load-balancing group.  Resets the
 * "burned" counter.
 */
void DownloadManager::RebalanceProxiesUnlocked(const string &reason) {
  if (!opt_proxy_groups_)
    return;

  opt_timestamp_failover_proxies_ = 0;
  opt_proxy_groups_current_burned_ = 0;
  UpdateProxiesUnlocked(reason);
}


void DownloadManager::RebalanceProxies() {
  const MutexLockGuard m(lock_options_);
  RebalanceProxiesUnlocked("rebalance invoked manually");
}


/**
 * Switches to the next load-balancing group of proxy servers.
 */
void DownloadManager::SwitchProxyGroup() {
  const MutexLockGuard m(lock_options_);

  if (!opt_proxy_groups_ || (opt_proxy_groups_->size() < 2)) {
    return;
  }

  opt_proxy_groups_current_ = (opt_proxy_groups_current_ + 1)
                              % opt_proxy_groups_->size();
  opt_timestamp_backup_proxies_ = time(NULL);

  const std::string msg = "switch to proxy group "
                          + StringifyUint(opt_proxy_groups_current_);
  RebalanceProxiesUnlocked(msg);
}


void DownloadManager::SetProxyGroupResetDelay(const unsigned seconds) {
  const MutexLockGuard m(lock_options_);
  opt_proxy_groups_reset_after_ = seconds;
  if (opt_proxy_groups_reset_after_ == 0) {
    opt_timestamp_backup_proxies_ = 0;
    opt_timestamp_failover_proxies_ = 0;
  }
}


void DownloadManager::SetMetalinkResetDelay(const unsigned seconds) {
  const MutexLockGuard m(lock_options_);
  opt_metalink_.reset_after = seconds;
  if (opt_metalink_.reset_after == 0)
    opt_metalink_.timestamp_backup = 0;
}


void DownloadManager::SetHostResetDelay(const unsigned seconds) {
  const MutexLockGuard m(lock_options_);
  opt_host_.reset_after = seconds;
  if (opt_host_.reset_after == 0)
    opt_host_.timestamp_backup = 0;
}


void DownloadManager::SetRetryParameters(const unsigned max_retries,
                                         const unsigned backoff_init_ms,
                                         const unsigned backoff_max_ms) {
  const MutexLockGuard m(lock_options_);
  opt_max_retries_ = max_retries;
  opt_backoff_init_ms_ = backoff_init_ms;
  opt_backoff_max_ms_ = backoff_max_ms;
}


void DownloadManager::SetMaxIpaddrPerProxy(unsigned limit) {
  const MutexLockGuard m(lock_options_);
  resolver_->set_throttle(limit);
}


void DownloadManager::SetProxyTemplates(const std::string &direct,
                                        const std::string &forced) {
  const MutexLockGuard m(lock_options_);
  proxy_template_direct_ = direct;
  proxy_template_forced_ = forced;
}


void DownloadManager::EnableInfoHeader() { enable_info_header_ = true; }


void DownloadManager::EnableRedirects() { follow_redirects_ = true; }

void DownloadManager::EnableIgnoreSignatureFailures() {
  ignore_signature_failures_ = true;
}

void DownloadManager::EnableHTTPTracing() { enable_http_tracing_ = true; }

void DownloadManager::AddHTTPTracingHeader(const std::string &header) {
  http_tracing_headers_.push_back(header);
}

void DownloadManager::UseSystemCertificatePath() {
  ssl_certificate_store_.UseSystemCertificatePath();
}

bool DownloadManager::SetShardingPolicy(const ShardingPolicySelector type) {
  const bool success = false;
  switch (type) {
    default:
      LogCvmfs(
          kLogDownload, kLogDebug | kLogSyslogErr,
          "(manager '%s') "
          "Proposed sharding policy does not exist. Falling back to default",
          name_.c_str());
  }
  return success;
}

void DownloadManager::SetFailoverIndefinitely() {
  failover_indefinitely_ = true;
}

/**
 * Creates a copy of the existing download manager.  Must only be called in
 * single-threaded stage because it calls curl_global_init().
 */
DownloadManager *DownloadManager::Clone(
    const perf::StatisticsTemplate &statistics,
    const std::string &cloned_name) {
  DownloadManager *clone = new DownloadManager(pool_max_handles_, statistics,
                                               cloned_name);

  clone->SetDnsParameters(resolver_->retries(), resolver_->timeout_ms());
  clone->SetDnsTtlLimits(resolver_->min_ttl(), resolver_->max_ttl());
  clone->SetMaxIpaddrPerProxy(resolver_->throttle());

  if (!opt_dns_server_.empty())
    clone->SetDnsServer(opt_dns_server_);
  clone->opt_timeout_proxy_ = opt_timeout_proxy_;
  clone->opt_timeout_direct_ = opt_timeout_direct_;
  clone->opt_low_speed_limit_ = opt_low_speed_limit_;
  clone->opt_tcp_keepalive_ = opt_tcp_keepalive_;
  clone->opt_parallel_fetch_ = opt_parallel_fetch_;
  clone->opt_max_retries_ = opt_max_retries_;
  clone->opt_backoff_init_ms_ = opt_backoff_init_ms_;
  clone->opt_backoff_max_ms_ = opt_backoff_max_ms_;
  clone->enable_info_header_ = enable_info_header_;
  clone->enable_http_tracing_ = enable_http_tracing_;
  clone->http_tracing_headers_ = http_tracing_headers_;
  clone->follow_redirects_ = follow_redirects_;
  clone->ignore_signature_failures_ = ignore_signature_failures_;
  if (opt_host_.chain) {
    clone->opt_host_.chain = new vector<string>(*opt_host_.chain);
    clone->opt_host_chain_rtt_ = new vector<int>(*opt_host_chain_rtt_);
  }

  CloneProxyConfig(clone);
  clone->opt_ip_preference_ = opt_ip_preference_;
  clone->proxy_template_direct_ = proxy_template_direct_;
  clone->proxy_template_forced_ = proxy_template_forced_;
  clone->opt_proxy_groups_reset_after_ = opt_proxy_groups_reset_after_;
  clone->opt_metalink_.reset_after = opt_metalink_.reset_after;
  clone->opt_host_.reset_after = opt_host_.reset_after;
  clone->credentials_attachment_ = credentials_attachment_;
  clone->ssl_certificate_store_ = ssl_certificate_store_;

  clone->health_check_ = health_check_;
  clone->sharding_policy_ = sharding_policy_;
  clone->failover_indefinitely_ = failover_indefinitely_;
  clone->fqrn_ = fqrn_;

  return clone;
}


void DownloadManager::CloneProxyConfig(DownloadManager *clone) {
  clone->opt_proxy_groups_current_ = opt_proxy_groups_current_;
  clone->opt_proxy_groups_current_burned_ = opt_proxy_groups_current_burned_;
  clone->opt_proxy_groups_fallback_ = opt_proxy_groups_fallback_;
  clone->opt_proxy_mandatory_ = opt_proxy_mandatory_;
  clone->opt_num_proxies_ = opt_num_proxies_;
  clone->opt_proxy_shard_ = opt_proxy_shard_;
  clone->opt_proxy_list_ = opt_proxy_list_;
  clone->opt_proxy_fallback_list_ = opt_proxy_fallback_list_;
  if (opt_proxy_groups_ == NULL)
    return;

  clone->opt_proxy_groups_ = new vector<vector<ProxyInfo> >(*opt_proxy_groups_);
  clone->UpdateProxiesUnlocked("cloned");
}

}  // namespace download
