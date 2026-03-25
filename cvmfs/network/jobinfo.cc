/**
 * This file is part of the CernVM File System.
 */

#include "jobinfo.h"

#include <inttypes.h>

#include "util/string.h"

namespace download {

atomic_int64 JobInfo::next_uuid = 0;

JobInfo::JobInfo(const std::string *u, const bool compressed, const bool ph,
         const shash::Any *h, cvmfs::Sink *s)
{
  Init(compressed ? zip::Algorithm::kGuessDecompression : zip::Algorithm::kNoCompression);

  url_ = u;
  probe_hosts_ = ph;
  head_request_ = false;
  expected_hash_ = h;
  sink_ = s;
}

JobInfo::JobInfo(const std::string *u, zip::Algorithm compression, const bool ph,
         const shash::Any *h, cvmfs::Sink *s)
{
  Init(compression);

  url_ = u;
  probe_hosts_ = ph;
  head_request_ = false;
  expected_hash_ = h;
  sink_ = s;
}

JobInfo::JobInfo(const std::string *u, const bool ph)
{
  Init(zip::kNoCompression);

  url_ = u;
  probe_hosts_ = ph;
  head_request_ = true;
  expected_hash_ = NULL;
  sink_ = NULL;
}


bool JobInfo::IsFileNotFound() {
  if (HasPrefix(*url_, "file://", true /* ignore_case */))
    return error_code_ == kFailHostConnection;

  return http_code_ == 404;
}

void JobInfo::SetDecompressor(zip::Algorithm decompressor_alg) {
  decompressor_alg_ = decompressor_alg;
  decomp_ = zip::Decompressor::Construct(decompressor_alg);
}

bool JobInfo::ResetDecompression() {
  return decomp_->Reset();
}

bool JobInfo::DecompressToSink(zip::InputAbstract *in) {
  const zip::StreamStates ret = decomp_->DecompressStream(in, sink_);

  switch (ret) {
    case zip::kStreamEnd:
    case zip::kStreamContinue:
      return true;
    break;
    case zip::kStreamDataError:
      LogCvmfs(kLogDownload, kLogSyslogErr,
                        "(id %" PRId64 ") %s failed for input %s: bad data",
                        id_, decomp_->Describe().c_str(), url_->c_str());
      SetErrorCode(kFailBadData);
    break;
    case zip::kStreamIOError:
      LogCvmfs(kLogDownload, kLogSyslogErr,
                      "(id %" PRId64 ") %s failed for input %s: local IO error",
                      id_, decomp_->Describe().c_str(), url_->c_str());
      SetErrorCode(kFailLocalIO);
    break;
    case zip::kStreamError:
      LogCvmfs(kLogDownload, kLogSyslogErr,
                    "(id %" PRId64 ") %s failed for input %s: unhealthy status",
                    id_, decomp_->Describe().c_str(), url_->c_str());
      SetErrorCode(kFailLocalIO);
    break;
    default:
      LogCvmfs(kLogDownload, kLogSyslogErr,
                    "(id %" PRId64 ") %s failed for input %s: unknown error %d",
                    id_, decomp_->Describe().c_str(), url_->c_str(), ret);
      SetErrorCode(kFailLocalIO);
  }

  return false;
}

void JobInfo::Init(zip::Algorithm decompressor_alg) {
  id_ = atomic_xadd64(&next_uuid, 1);
  pipe_job_results = NULL;
  url_ = NULL;
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
  num_used_proxies_ = 0;
  num_used_hosts_ = 0;
  num_retries_ = 0;
  backoff_ms_ = 0;
  current_host_chain_index_ = 0;

  allow_failure_ = false;

  SetDecompressor(decompressor_alg);
}

}  // namespace download
