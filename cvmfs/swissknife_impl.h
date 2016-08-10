/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_IMPL_H_
#define CVMFS_SWISSKNIFE_IMPL_H_

#include <string>

#include "util/posix.h"

template <class ObjectFetcherT>
manifest::Reflog* swissknife::Command::FetchReflog(
                                              ObjectFetcherT    *object_fetcher,
                                              const std::string &repo_name,
                                              const shash::Any  &reflog_hash) {
  // try to fetch the Reflog from the backend storage first
  manifest::Reflog *reflog = NULL;
  typename ObjectFetcherT::Failures f =
    object_fetcher->FetchReflog(reflog_hash, &reflog);

  switch (f) {
    case ObjectFetcherT::kFailOk:
      LogCvmfs(kLogCvmfs, kLogDebug, "fetched reflog '%s' from backend storage",
               reflog->database_file().c_str());
      break;

    case ObjectFetcherT::kFailNotFound:
      LogCvmfs(kLogCvmfs, kLogStderr,
               "reflog for '%s' not found but reflog.chksum is present; "
               "remove reflog.chksum to recreate the reflog",
               repo_name.c_str());
      abort();

    case ObjectFetcherT::kFailBadData:
      LogCvmfs(kLogCvmfs, kLogStderr,
               "data corruption in .cvmfsreflog for %s, remove for automatic "
               "recreation or verify reflog.chksum file", repo_name.c_str());
      abort();

    default:
      LogCvmfs(kLogCvmfs, kLogStderr, "failed loading reflog in '%s' (%d - %s)",
               repo_name.c_str(), f, Code2Ascii(f));
      abort();
  }

  assert(reflog != NULL);
  return reflog;
}

#endif  // CVMFS_SWISSKNIFE_IMPL_H_
