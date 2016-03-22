/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_IMPL_H_
#define CVMFS_SWISSKNIFE_IMPL_H_

#include <string>

#include "util/posix.h"

template <class ObjectFetcherT>
manifest::Reflog* swissknife::Command::GetOrIgnoreReflog(
                                              ObjectFetcherT    *object_fetcher,
                                              const std::string &repo_name) {
  // try to fetch the Reflog from the backend storage first
  manifest::Reflog *reflog = NULL;
  typename ObjectFetcherT::Failures f = object_fetcher->FetchReflog(&reflog);

  switch (f) {
    case ObjectFetcherT::kFailOk:
      LogCvmfs(kLogCvmfs, kLogDebug, "fetched reflog '%s' from backend storage",
                                   reflog->database_file().c_str());
      break;

    case ObjectFetcherT::kFailNotFound:
      LogCvmfs(kLogCvmfs, kLogDebug, "reflog for '%s' not found",
                                     repo_name.c_str());
      reflog = NULL;
      break;

    default:
      LogCvmfs(kLogCvmfs, kLogStderr, "failed loading reflog in '%s' (%d - %s)",
                                          repo_name.c_str(), f, Code2Ascii(f));
      abort();
  }

  return reflog;
}


template <class ObjectFetcherT>
manifest::Reflog* swissknife::Command::GetOrCreateReflog(
                                              ObjectFetcherT    *object_fetcher,
                                              const std::string &repo_name) {
  manifest::Reflog *reflog = GetOrIgnoreReflog(object_fetcher, repo_name);
  if (reflog != NULL) {
    return reflog;
  }

  // create a new Reflog if there was none found yet
  const std::string tmp_path_prefix = object_fetcher->temporary_directory() +
                                      "/new_reflog";
  const std::string tmp_path = CreateTempPath(tmp_path_prefix, 0600);

  LogCvmfs(kLogCvmfs, kLogDebug, "creating new reflog '%s' for %s",
                                 tmp_path.c_str(), repo_name.c_str());
  return manifest::Reflog::Create(tmp_path, repo_name);
}

#endif  // CVMFS_SWISSKNIFE_IMPL_H_
