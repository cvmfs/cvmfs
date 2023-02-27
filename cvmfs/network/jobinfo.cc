/**
 * This file is part of the CernVM File System.
 */

#include "jobinfo.h"
#include "util/string.h"

namespace download {

JobInfo* JobInfo::CreateWithSink(const std::string *u, const bool c,
                                 const bool ph, const shash::Any *h,
                                 cvmfs::Sink *s) {
  UniquePtr<JobInfo> jobinfo(new JobInfo());

  jobinfo->SetUrl(u);
  jobinfo->SetCompressed(c);
  jobinfo->SetProbeHosts(ph);
  jobinfo->SetExpectedHash(h);
  jobinfo->SetSink(s);

  return jobinfo.Release();
}

JobInfo* JobInfo::CreateWithoutSink(const std::string *u, const bool ph) {
  UniquePtr<JobInfo> jobinfo(new JobInfo());

  jobinfo->SetUrl(u);
  jobinfo->SetProbeHosts(ph);
  jobinfo->SetHeadRequest(true);

  return jobinfo.Release();
}


bool JobInfo::IsFileNotFound() {
  if (HasPrefix(*url_, "file://", true /* ignore_case */))
    return error_code_ == kFailHostConnection;

  return http_code_ == 404;
}

}  // namespace download
