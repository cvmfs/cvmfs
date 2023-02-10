/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_NETWORK_JOBINFO_H_
#define CVMFS_NETWORK_JOBINFO_H_

#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>

#include <cstdio>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "compression.h"
#include "crypto/hash.h"
#include "duplex_curl.h"
#include "network_errors.h"
#include "sink.h"
#include "sink_mem.h"
#include "sink_file.h"
#include "sink_path.h"

class InterruptCue;

namespace download {

/**
 * Where to store downloaded data.
 */
enum Destination {
  kDestinationMem = 1,
  kDestinationFile,
  kDestinationPath,
  kDestinationTransaction,
  kDestinationNone
};  // Destination

/**
 * Contains all the information to specify a download job.
 */
struct JobInfo {
  const std::string *url;
  bool compressed;
  bool probe_hosts;
  bool head_request;
  bool follow_redirects;
  bool force_nocache;
  pid_t pid;
  uid_t uid;
  gid_t gid;
  void *cred_data;  // Per-transfer credential data
  InterruptCue *interrupt_cue;
  Destination destination;
  cvmfs::Sink *destination_sink;
  const shash::Any *expected_hash;
  const std::string *extra_info;

  // Allow byte ranges to be specified.
  off_t range_offset;
  off_t range_size;

  // Default initialization of fields
  void Init() {
    url = NULL;
    compressed = false;
    probe_hosts = false;
    head_request = false;
    follow_redirects = false;
    force_nocache = false;
    pid = -1;
    uid = -1;
    gid = -1;
    cred_data = NULL;
    interrupt_cue = NULL;
    destination = kDestinationNone;
    destination_sink = NULL;
    expected_hash = NULL;
    extra_info = NULL;

    curl_handle = NULL;
    headers = NULL;
    memset(&zstream, 0, sizeof(zstream));
    info_header = NULL;
    pipe_job_results = NULL;
    nocache = false;
    error_code = kFailOther;
    num_used_proxies = num_used_hosts = num_retries = 0;
    backoff_ms = 0;
    current_host_chain_index = 0;

    range_offset = -1;
    range_size = -1;
    http_code = -1;
  }

  // One constructor per destination + head request
  JobInfo() { Init(); }
  JobInfo(const std::string *u, const bool c, const bool ph,
          const shash::Any *h, cvmfs::Sink *s, enum Destination dest) {
    Init();
    url = u;
    compressed = c;
    probe_hosts = ph;
    destination = dest;
    expected_hash = h;
    destination_sink = s;
  }

  JobInfo(const std::string *u, const bool ph) {
    Init();
    url = u;
    probe_hosts = ph;
    head_request = true;
  }

  ~JobInfo() {
    if (pipe_job_results.IsValid()) {
      pipe_job_results.Destroy();
    }
  }

  /**
   * Tells whether the error is because of a non-existing file. Should only
   * be called if error_code is not kFailOk
   */
  bool IsFileNotFound();

  // Internal state, don't touch
  CURL *curl_handle;
  curl_slist *headers;
  char *info_header;
  z_stream zstream;
  shash::ContextPtr hash_context;
  /// Pipe used for the return value
  UniquePtr<Pipe<kPipeDownloadJobsResults> > pipe_job_results;
  std::string proxy;
  bool nocache;
  Failures error_code;
  int http_code;
  unsigned char num_used_proxies;
  unsigned char num_used_hosts;
  unsigned char num_retries;
  unsigned backoff_ms;
  unsigned int current_host_chain_index;
};  // JobInfo

}  // namespace download

#endif  // CVMFS_NETWORK_JOBINFO_H_
