/**
 * This file is part of the CernVM File System.
 */

#include "jobinfo.h"
#include "util/string.h"

namespace download {

JobInfo::JobInfo(const std::string *u, const bool c, const bool ph,
         const shash::Any *h, cvmfs::Sink *s) : JobInfo() {
  url_ = u;
  compressed_ = c;
  probe_hosts_ = ph;
  expected_hash_ = h;
  sink_ = s;
}

JobInfo::JobInfo(const std::string *u, const bool ph) : JobInfo() {
  url_ = u;
  probe_hosts_ = ph;
  head_request_ = true;
}


bool JobInfo::IsFileNotFound() {
  if (HasPrefix(*url_, "file://", true /* ignore_case */))
    return error_code_ == kFailHostConnection;

  return http_code_ == 404;
}

}  // namespace download
