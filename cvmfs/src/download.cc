/**
 * This file is part of the CernVM File System.
 */

#include "download.h"

#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <alloca.h>
#include <errno.h>
#include <poll.h>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <cstdio>

#include <set>

#include "cvmfs_config.h"
#include "curl_duplex.h"
#include "logging.h"
#include "atomic.h"
#include "hash.h"
extern "C" {
  #include "smalloc.h"
  #include "compression.h"
  #include "sha1.h"
}

using namespace std;  // NOLINT

namespace download {

set<CURL *>  *pool_handles_idle_ = NULL;
set<CURL *>  *pool_handles_inuse_ = NULL;
uint32_t pool_max_handles_ = 10;
CURLM *curl_multi_ = NULL;
curl_slist *http_headers_ = NULL;
curl_slist *http_headers_nocache_ = NULL;

pthread_t thread_download_;
atomic_int multi_threaded_;
int pipe_terminate_[2];

int pipe_jobs_[2];
struct pollfd *watch_fds_ = NULL;
uint32_t watch_fds_size_ = 0;
uint32_t watch_fds_inuse_ = 0;
uint32_t watch_fds_max = 10;


/**
 * Don't let any fancy stuff kill the URL, except for ':' and '/',
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


/* TODO: in util.cc */
static bool HasPrefix(const string &str, const string &prefix,
                      const bool ignore_case)
{
  if (prefix.length() > str.length())
    return false;

  for (unsigned i = 0, l = prefix.length(); i < l; ++i) {
    if (ignore_case) {
      if (toupper(str[i]) != toupper(prefix[i]))
        return false;
    } else {
      if (str[i] != prefix[i])
        return false;
    }
  }
  return true;
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

  //LogCvmfs(kLogDownload, kLogDebug, "Header callback with line %s", header_line.c_str());

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
      info->error_code = kFailHttp;
      const string http_status = header_line.substr(i, 3);
      // TODO: PROXY handling
      return 0;
    }
  }

  // Allocate memory for kDestinationMemory
  if ((info->destination == kDestinationMem) &&
      HasPrefix(header_line, "CONTENT-LENGTH:", true))
  {
    char *tmp = (char *)alloca(num_bytes+1);
    unsigned long length = 0;
    sscanf(header_line.c_str(), "%s %lu", tmp, &length);
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
    sha1_update(&info->sha1_context, (unsigned char *)ptr, num_bytes);

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
      int retval = decompress_strm_file(&info->zstream, info->destination_file,
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
  info->num_retries = 0;
  if (info->compressed) {
    int retval = decompress_strm_init(&(info->zstream));
    assert(retval == Z_OK);
  }
  if (info->expected_hash)
    sha1_init(&info->sha1_context);

  if ((info->destination == kDestinationMem) &&
      (HasPrefix(*(info->url), "file://", false)))
  {
    info->destination_mem.size = 64*1024;
    info->destination_mem.data = static_cast<char *>(smalloc(64*1024));
  }

  // Set request specific curl parameters
  curl_easy_setopt(handle, CURLOPT_PRIVATE, static_cast<void *>(info));
  curl_easy_setopt(handle, CURLOPT_WRITEHEADER,
                   static_cast<void *>(info));
  curl_easy_setopt(handle, CURLOPT_WRITEDATA, static_cast<void *>(info));
  curl_easy_setopt(handle, CURLOPT_URL, EscapeUrl(*(info->url)).c_str());
  if (info->nocache)
    curl_easy_setopt(handle, CURLOPT_HTTPHEADER, http_headers_nocache_);
  else
    curl_easy_setopt(handle, CURLOPT_HTTPHEADER, http_headers_);
}


/**
 * Checks the result of a curl download and implements the failure logic, such
 * as changing the proxy server.  Takes care of cleanup.
 *
 * \return true if another download should be performed, false otherwise
 */
static bool VerifyAndFinalize(const int curl_error, JobInfo *info) {
  bool try_again = false;

  //LogCvmfs(kLogDownload, kLogDebug, "Verify Download (curl error %d)", curl_error);

  // Verification and error classification, determination if download should be
  // repeated.
  switch (curl_error) {
    case CURLE_OK:
      // Decompress memory in a single run
      if ((info->destination == kDestinationMem) && info->compressed) {
        void *buf;
        size_t size;
        int retval = decompress_mem(info->destination_mem.data,
                                    info->destination_mem.size, &buf, &size);
        if (retval == 0) {
          free(info->destination_mem.data);
          info->destination_mem.data = static_cast<char *>(buf);
          info->destination_mem.size = size;
        } else {
          info->error_code = kFailBadData;
          break;
        }
      }

      // Verify content hash
      if (info->expected_hash) {
        unsigned char digest[SHA1_DIGEST_LENGTH];
        sha1_final(digest, &info->sha1_context);
        if (memcmp(digest, info->expected_hash->digest, SHA1_DIGEST_LENGTH)) {
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
    case CURLE_ABORTED_BY_CALLBACK:
    case CURLE_WRITE_ERROR:
      // TODO: on (zlib) verification failure same as on hash verification failure
      break;
    default:
      LogCvmfs(kLogDownload, kLogSyslog, "unexpected curl error (%d) while "
               "trying to fetch %s", curl_error, info->url->c_str());
      info->error_code = kFailOther;
      break;
  }

  // TODO: check if number of attempts is too high for retry

  if (!try_again) {
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

    // Close Zlib stream
    if (info->compressed)
      decompress_strm_fini(&(info->zstream));
  } else {
    // Reset, switch host or proxy if necessary
  }

  return try_again;
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

  if (atomic_xadd(&multi_threaded_, 0) == 1) {
    int retval;
    if (info->wait_at[0] == -1) {
      retval = pipe(info->wait_at);
      assert(retval == 0);
    }

    //LogCvmfs(kLogDownload, kLogDebug, "send job to thread, pipe %d %d", info->wait_at[0], info->wait_at[1]);
    retval = write(pipe_jobs_[1], &info, sizeof(info));
    assert(retval == sizeof(info));
    retval = read(info->wait_at[0], &result, sizeof(result));
    assert(retval == sizeof(result));

    //LogCvmfs(kLogDownload, kLogDebug, "got result %d", result);
  } else {
    CURL *handle = AcquireCurlHandle();
    InitializeRequest(info, handle);
    //curl_easy_setopt(handle, CURLOPT_VERBOSE, 1);
    int retval;
    do {
      retval = curl_easy_perform(handle);
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

  return result;
}


/**
 * Called when new curl sockets arrive or existing curl sockets departure.
 */
static int CallbackCurlSocket(CURL *easy, curl_socket_t s, int action,
                              void *userp, void *socketp)
{
  //LogCvmfs(kLogDownload, kLogDebug, "CallbackCurlSocket called with easy handle %p, socket %d, action %d", easy, s, action);
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
      if ((watch_fds_inuse_ > watch_fds_max) &&
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

  int still_running;
  while (true) {
    int retval = poll(watch_fds_, watch_fds_inuse_, -1);
    assert(retval > 0);

    // Terminate I/O thread
    if (watch_fds_[0].revents)
      break;

    // New job arrives
    if (watch_fds_[1].revents) {
      watch_fds_[1].revents = 0;
      JobInfo *info;
      retval = read(pipe_jobs_[0], &info, sizeof(info));
      assert(retval == sizeof(info));
      //LogCvmfs(kLogDownload, kLogDebug, "IO thread, got job: url %s, compressed %d, nocache %d, destination %d, file %p, expected hash %p, wait at %d", info->url->c_str(), info->compressed, info->nocache,
      //         info->destination, info->destination_file, info->expected_hash, info->wait_at[1]);

      CURL *handle = AcquireCurlHandle();
      InitializeRequest(info, handle);
      curl_multi_add_handle(curl_multi_, handle);
      retval = curl_multi_socket_action(curl_multi_, CURL_SOCKET_TIMEOUT, 0, &still_running);
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
        } else {
          // Return easy handle into pool
          ReleaseCurlHandle(easy_handle);
        }

        int written = write(info->wait_at[1], &info->error_code,
                            sizeof(info->error_code));
        assert(written == sizeof(info->error_code));
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


void Init() {
  atomic_init(&multi_threaded_);
  int retval = curl_global_init(CURL_GLOBAL_ALL);
  assert(retval == CURLE_OK);
  pool_handles_idle_ = new set<CURL *>;
  pool_handles_inuse_ = new set<CURL *>;
  pool_max_handles_ = 10;

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
  //curl_multi_setopt(curl_multi_, CURLMOPT_PIPELINING, 1);

  // Initialize random number engine with system time
  struct timeval tv_now;
  gettimeofday(&tv_now, NULL);
  srandom(tv_now.tv_usec);
}


void Fini() {
  if (atomic_xadd(&multi_threaded_, 0) == 1) {
    // Shutdown I/O thread
    char buf = 'T';
    int retval = write(pipe_terminate_[1], &buf, 1);
    assert(retval == 1);
    LogCvmfs(kLogDownload, kLogDebug, "written into pipe");
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

  curl_global_cleanup();
}


/**
 * Spawns the worker thread.
 */
void Spawn() {
  int retval = pipe(pipe_terminate_);
  assert(retval == 0);
  retval = pipe(pipe_jobs_);
  assert(retval == 0);

  retval = pthread_create(&thread_download_, NULL, MainDownload, NULL);
  assert(retval == 0);


  // TODO: create pipe
  atomic_inc(&multi_threaded_);
}

}  // namespace download
