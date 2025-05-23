/**
 * This file is part of the CernVM File System.
 */

#include "jobinfo.h"

#include "util/string.h"

namespace download {

atomic_int64 JobInfo::next_uuid = 0;

JobInfo::JobInfo(const std::string *u, const bool c, const bool ph,
                 const shash::Any *h, cvmfs::Sink *s) {
  Init();

  url_ = u;
  compressed_ = c;
  probe_hosts_ = ph;
  expected_hash_ = h;
  sink_ = s;
}

JobInfo::JobInfo(const std::string *u, const bool ph) {
  Init();

  url_ = u;
  probe_hosts_ = ph;
  head_request_ = true;
}


bool JobInfo::IsFileNotFound() {
  if (HasPrefix(*url_, "file://", true /* ignore_case */))
    return error_code_ == kFailHostConnection;

  return http_code_ == 404;
}

void JobInfo::Init() {
  id_ = atomic_xadd64(&next_uuid, 1);
  pipe_job_results = NULL;
  url_ = NULL;
  compressed_ = false;
  probe_hosts_ = false;
  head_request_ = false;
  follow_redirects_ = false;
  force_nocache_ = false;
  pid_ = -1;
  uid_ = -1;
  gid_ = -1;
  cred_data_ = NULL;
  interrupt_cue_ = NULL;
  sink_ = NULL;
  expected_hash_ = NULL;
  extra_info_ = NULL;
  //
  range_offset_ = -1;
  range_size_ = -1;
  //
  curl_handle_ = NULL;
  headers_ = NULL;
  info_header_ = NULL;
  tracing_header_pid_ = NULL;
  tracing_header_gid_ = NULL;
  tracing_header_uid_ = NULL;
  nocache_ = false;
  error_code_ = kFailOther;
  http_code_ = -1;
  link_ = "";
  num_used_proxies_ = 0;
  num_used_metalinks_ = 0;
  num_used_hosts_ = 0;
  num_retries_ = 0;
  backoff_ms_ = 0;
  current_metalink_chain_index_ = -1;
  current_host_chain_index_ = -1;

  allow_failure_ = false;

  memset(&zstream_, 0, sizeof(zstream_));
}

}  // namespace download
