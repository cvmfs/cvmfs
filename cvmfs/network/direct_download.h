/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_DIRECT_DOWNLOAD_H_
#define CVMFS_NETWORK_DIRECT_DOWNLOAD_H_

#include <pthread.h>
#include <stddef.h>

#include "directory_entry.h"
#include "download.h"
#include "duplex_curl.h"
#include "statistics.h"

namespace download {

struct DirectResponse {
  char *buffer;
  size_t size;
  size_t bytes;
  size_t calls;
  int error;
};

class DirectDownload {
 public:
  static const unsigned int kBitDirectDownload = 61;

  typedef int (Read_f)(void *priv, const char *buf, size_t size);
  typedef int (Error_f)(void *priv, int error);

  DirectDownload(perf::StatisticsTemplate statistics);
  ~DirectDownload();

  void SetBoundry(uint64_t boundry) { boundry_ = boundry; }
  void Read(Read_f read_f, Error_f error_f, void *priv, download::DownloadManager *mgr, const std::string &path,
    uint64_t offset, uint64_t size);

 private:

  uint64_t RangeBoundry(uint64_t start, uint64_t end);
  const char *StripAuth(const char *url);

  CURLSH *curl_share_;
  pthread_mutex_t curl_share_locks_[CURL_LOCK_DATA_LAST];
  uint64_t boundry_;
  perf::Counter *n_eio;

};  // DirectDownload

}  // namespace download

#endif  // CVMFS_NETWORK_DOWNLOAD_H_
