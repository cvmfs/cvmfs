/**
 * This file is part of the CernVM File System.
 */

#include "jobinfo.h"

#include <inttypes.h>

#include "util/string.h"

namespace download {

atomic_int64 JobInfo::next_uuid = 0;

JobInfo::JobInfo(const std::string *u, const bool c, const bool ph,
         const shash::Any *h, cvmfs::Sink *s) {
  if (c) {
    Init(kCreateZlib);
  } else {
    Init(kCreateEcho);
  }

  url_ = u;
  probe_hosts_ = ph;
  expected_hash_ = h;
  sink_ = s;
}

JobInfo::JobInfo(const std::string *u, const bool ph) {
  Init(kCreateNone);

  url_ = u;
  probe_hosts_ = ph;
  head_request_ = true;
}


bool JobInfo::IsFileNotFound() {
  if (HasPrefix(*url_, "file://", true /* ignore_case */))
    return error_code_ == kFailHostConnection;

  return http_code_ == 404;
}

void JobInfo::SetDecompressor(const DecompressorType decompressor_type) {
  if (decompressor_type != decompressor_type_) {
    decompressor_type_ = decompressor_type;

    if (decomp_.IsValid()) {
      decomp_.Destroy();
    }

    switch (decompressor_type_) {
      case kCreateNone:
        return;
      break;
      case kCreateZlib:
        decomp_ = zlib::Decompressor::Construct(zlib::kZlibDefault);
      break;
      case kCreateEcho:
        decomp_ = zlib::Decompressor::Construct(zlib::kNoCompression);
      break;
    }
  }
}

bool JobInfo::ResetDecompression() {
  if (!decomp_.IsValid()) {
    return true;
  }
  return decomp_->Reset();
}

bool JobInfo::DecompressToSink(zlib::InputAbstract *in) {
  assert(decomp_.IsValid());

  const zlib::StreamStates ret = decomp_->DecompressStream(in, sink_);

  switch (ret) {
    case zlib::kStreamEnd:
    case zlib::kStreamContinue:
      return true;
    break;
    case zlib::kStreamDataError:
      LogCvmfs(kLogDownload, kLogSyslogErr,
                            "(id %" PRId64 ") %s failed for input %s: bad data",
                            id_, decomp_->Describe().c_str(), url_->c_str());
      SetErrorCode(kFailBadData);
    break;
    case zlib::kStreamIOError:
      LogCvmfs(kLogDownload, kLogSyslogErr,
                      "(id %" PRId64 ") %s failed for input %s: local IO error",
                      id_, decomp_->Describe().c_str(), url_->c_str());
      SetErrorCode(kFailLocalIO);
    break;
    case zlib::kStreamError:
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

void JobInfo::Init(const DecompressorType decompressor_type) {
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

  decompressor_type_ = kCreateNone;
  SetDecompressor(decompressor_type);
}

}  // namespace download
