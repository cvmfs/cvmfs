/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"

#include "clientctx.h"
#include "direct_download.h"
#include "util/logging.h"

using namespace std;  // NOLINT

namespace download {

static void curl_share_lock_cb(CURL *handle, curl_lock_data data,
                    curl_lock_access access, void *userptr) {
  pthread_mutex_t *locks = static_cast<pthread_mutex_t*>(userptr);
  pthread_mutex_lock(&locks[data]);
}

static void curl_share_unlock_cb(CURL *handle, curl_lock_data data, void *userptr) {
  pthread_mutex_t *locks = static_cast<pthread_mutex_t*>(userptr);
  pthread_mutex_unlock(&locks[data]);
}

DirectDownload::DirectDownload(perf::StatisticsTemplate statistics) {
  boundry_ = 0;

  n_eio = statistics.RegisterTemplated("n_eio", "IO errors");

  for (size_t i = 0; i < CURL_LOCK_DATA_LAST; i++) {
    int retval = pthread_mutex_init(&curl_share_locks_[i], NULL);
    assert(retval == 0);
  }

  curl_share_ = curl_share_init();
  assert(curl_share_ != NULL);
  assert(curl_share_setopt(curl_share_, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS) == CURLSHE_OK);
  assert(curl_share_setopt(curl_share_, CURLSHOPT_SHARE, CURL_LOCK_DATA_CONNECT) == CURLSHE_OK);
  assert(curl_share_setopt(curl_share_, CURLSHOPT_LOCKFUNC, curl_share_lock_cb) == CURLSHE_OK);
  assert(curl_share_setopt(curl_share_, CURLSHOPT_UNLOCKFUNC, curl_share_unlock_cb) == CURLSHE_OK);
  assert(curl_share_setopt(curl_share_, CURLSHOPT_USERDATA,
                           static_cast<void *>(curl_share_locks_)) == CURLSHE_OK);
}

DirectDownload::~DirectDownload() {
  curl_share_cleanup(curl_share_);
  curl_share_ = NULL;

  for (size_t i = 0; i < CURL_LOCK_DATA_LAST; i++) {
    pthread_mutex_destroy(&curl_share_locks_[i]);
  }
}

const char *DirectDownload::StripAuth(const char *url)
{
    const char *path = strrchr(url, '@');
    return path ? path + 1 : url;
}

// Make sure the start(inclusize)-end(inclusive) stays within a boundry. Return the end.
uint64_t DirectDownload::RangeBoundry(uint64_t start, uint64_t end) {
  if (boundry_ == 0) {
    return end;
  }

  uint64_t end_max = start + boundry_;
  end_max = (end_max / boundry_) * boundry_;

  if (end >= end_max) {
    end = end_max - 1;
  }

  return end;
}

string DirectDownload::GetProxy(download::DownloadManager *mgr, const string &path,
  const string &current_proxy, uint64_t offset)
{
  string proxy;

  shash::Any hash(shash::kMd5);
  if (mgr->sharding_policy_.UseCount() > 0) {
    proxy = mgr->sharding_policy_->GetNextProxy(&path, current_proxy, offset);
  } else {
    shash::Any hash(shash::kMd5);
    HashString(path, &hash);
    DownloadManager::ProxyInfo *proxyinfo = mgr->ChooseProxyUnlocked(&hash);
    if (proxyinfo && (proxyinfo->url != "DIRECT")) {
      proxy = proxyinfo->url;
    }
  }

  return proxy;
}

static size_t curl_write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
  DirectResponse *response = static_cast<DirectResponse*>(userdata);

  if (response->error) {
    return 0;
  }

  // libcurl says size is always 1
  if (size != 1) {
    nmemb *= size;
  }

  if (nmemb > response->size - response->bytes) {
    response->error = 1;
    return 0;
  }

  memcpy(response->buffer + response->bytes, ptr, nmemb);
  response->bytes += nmemb;
  response->calls++;
  assert(response->bytes <= response->size);

  return nmemb;
}

void DirectDownload::Read(Read_f read_f, Error_f error_f, void *priv, download::DownloadManager *mgr,
    const string &path, uint64_t offset, uint64_t size)
{
  if (size == 0) {
    read_f(priv, NULL, 0);
    return;
  }

  DirectResponse response;
  memset(&response, 0, sizeof(response));
  response.size = size;
  response.buffer = (char*)smalloc(response.size);
  if (!response.buffer) {
    LogCvmfs(kLogDownload, kLogDebug, "direct read(%s) malloc failed", path.c_str());
    error_f(priv, EIO);
    perf::Inc(n_eio);
    return;
  }

  vector<string> host_chain;
  unsigned current_host = 0;
  mgr->GetHostInfo(&host_chain, NULL, &current_host);
  if (host_chain.size() <= current_host) {
    LogCvmfs(kLogDownload, kLogDebug, "direct read(%s) bad host_chain", path.c_str());
    error_f(priv, EIO);
    perf::Inc(n_eio);
    return;
  }

  string url = host_chain[current_host] + path;
  string proxy = GetProxy(mgr, path, "", offset);

  CURL *curl = curl_easy_init();

  if (!curl) {
    LogCvmfs(kLogDownload, kLogDebug, "direct read(%s) curl_easy_init failed", path.c_str());
    error_f(priv, EIO);
    perf::Inc(n_eio);
    return;
  }

  unsigned proxy_timeout, direct_timeout;
  mgr->GetTimeout(&proxy_timeout, &direct_timeout);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, proxy_timeout);

  if (curl_easy_setopt(curl, CURLOPT_SHARE, curl_share_) != CURLE_OK) {
    LogCvmfs(kLogDownload, kLogSyslogWarn, "direct read(%s) CURLOPT_SHARE failed", path.c_str());
  }

  curl_easy_setopt(curl, CURLOPT_USERAGENT, "cvmfs direct " VERSION);
  curl_easy_setopt(curl, CURLOPT_MAXCONNECTS, 100);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_callback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*)&response);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
  curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1);
  curl_easy_setopt(curl, CURLOPT_URL, mgr->EscapeUrl(0, url).c_str());

  if (proxy.length() > 0) {
    curl_easy_setopt(curl, CURLOPT_PROXY, proxy.c_str());
  }

  // Tracing headers
  char hbuf[256];
  struct curl_slist *slist = NULL;

  if (mgr->enable_info_header_) {
    string path_escaped = mgr->EscapeUrl(0, path);
    snprintf(hbuf, sizeof(hbuf), "cvmfs-info: %s", path_escaped.c_str());
    slist = curl_slist_append(slist, hbuf);
  }

  if (mgr->enable_http_tracing_) {
    uid_t uid;
    gid_t gid;
    pid_t pid;
    InterruptCue *ic;
    ClientCtx *ctx = ClientCtx::GetInstance();
    ctx->Get(&uid, &gid, &pid, &ic);
    snprintf(hbuf, sizeof(hbuf), "X-CVMFS-UID: %u", uid);
    slist = curl_slist_append(slist, hbuf);
    snprintf(hbuf, sizeof(hbuf), "X-CVMFS-GID: %u", gid);
    slist = curl_slist_append(slist, hbuf);
    snprintf(hbuf, sizeof(hbuf), "X-CVMFS-PID: %d", pid);
    slist = curl_slist_append(slist, hbuf);

    for (size_t i = 0; i < mgr->http_tracing_headers_.size(); i++) {
      slist = curl_slist_append(slist, mgr->http_tracing_headers_[i].c_str());
    }
  }

  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, slist);

  // Ranges are inclusive
  uint64_t offset_max = offset + size - 1;
  unsigned int offset_tries = 0;
  char range_buf[100];
  bool done = false, retry, error = false;
  unsigned int retries = 0;
  unsigned int max_retries = mgr->opt_max_retries_;
  unsigned int backoff_sleep = 0;
  long response_code = -1;
  while (!done && !error) {
    // Make sure we stay within a boundry
    uint64_t offset_end = RangeBoundry(offset, offset_max);
    snprintf(range_buf, sizeof(range_buf), "%lu-%lu", offset, offset_end);
    curl_easy_setopt(curl, CURLOPT_RANGE, range_buf);

    LogCvmfs(kLogDownload, kLogDebug, "direct read(%s, %lu, %lu/%lu) proxy: %s",
      path.c_str(), offset, offset_end - offset + 1, size,
      StripAuth(proxy.c_str()));

    CURLcode res = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

    offset_tries++;
    retry = false;

    // These curl error codes are retried (see download.cc)
    switch (res) {
      case CURLE_COULDNT_RESOLVE_PROXY:
      case CURLE_COULDNT_RESOLVE_HOST:
      case CURLE_OPERATION_TIMEDOUT:
      case CURLE_PARTIAL_FILE:
      case CURLE_GOT_NOTHING:
      case CURLE_RECV_ERROR:
      case CURLE_COULDNT_CONNECT:
        retry = true;
        break;
      case CURLE_WRITE_ERROR:
        error = true;
        break;
      default:
        break;
    }

    // All HTTP errors are marked for retry
    if (response_code >= 400) {
      retry = true;
    }

    // Do not retry these errors
    if (response_code == 404 || response.error) {
      error = true;
    }

    // We reached the end
    if (response_code == 416) {
      done = true;
    }

    // Too many retries
    if (retries > max_retries) {
      done = true;
    }

    if (done || error) {
      break;
    }

    if (retry) {
      if (proxy.length() > 0) {
        proxy = GetProxy(mgr, path, "", offset);
        curl_easy_setopt(curl, CURLOPT_PROXY, proxy.c_str());
      }

      LogCvmfs(kLogDownload, kLogDebug, "direct read(%s) curl error (%d, %ld) new proxy: %s",
        path.c_str(), res, response_code, StripAuth(proxy.c_str()));

      perf::Inc(mgr->counters_->n_proxy_failover);

      retries++;
      if (backoff_sleep == 0) {
        backoff_sleep = mgr->prng_.Next(mgr->opt_backoff_init_ms_ + 1);
      } else {
        backoff_sleep *= 2;
      }
      if (backoff_sleep > mgr->opt_backoff_max_ms_) {
        backoff_sleep = mgr->opt_backoff_max_ms_;
      }
      SafeSleepMs(backoff_sleep);
    } else {
      offset = offset_end + 1;
      if (offset <= offset_max) {
        // Still more left
        offset_end = RangeBoundry(offset, offset_max);
        snprintf(range_buf, sizeof(range_buf), "%lu-%lu", offset, offset_end);
        curl_easy_setopt(curl, CURLOPT_RANGE, range_buf);

        if (proxy.length() > 0) {
          proxy = GetProxy(mgr, path, "", offset);
          curl_easy_setopt(curl, CURLOPT_PROXY, proxy.c_str());
        }
      } else {
        done = true;
      }
    }
  }

  curl_slist_free_all(slist);
  curl_easy_cleanup(curl);

  int response_error = 1;
  if (!error && !response.error) {
    response_error = read_f(priv, response.buffer, response.bytes);
  }
  if (response_error) {
    LogCvmfs(kLogDownload, kLogDebug, "direct read(%s) read_f error %d, %ld response, %zu bytes",
      path.c_str(), response_error, response_code, response.bytes);
    error_f(priv, EIO);
    perf::Inc(n_eio);

    free(response.buffer);
    response.buffer = NULL;

    return;
  }

  LogCvmfs(kLogDownload, kLogDebug, "direct read(%s) %ld response, %zu bytes in %zu calls %u boundries %u retries",
    path.c_str(), response_code, response.bytes, response.calls, offset_tries, retries);

  free(response.buffer);
  response.buffer = NULL;

  return;
}

}  // namespace download
