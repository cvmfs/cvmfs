/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_IMPL_H_
#define CVMFS_SWISSKNIFE_IMPL_H_

template <class ObjectFetcherT>
manifest::Reflog* swissknife::Command::GetOrCreateReflog(
                                              ObjectFetcherT    *object_fetcher,
                                              const std::string &repo_name) {
  // try to fetch the Reflog from the backend storage first
  manifest::Reflog *reflog = NULL;
  typename ObjectFetcherT::Failures f = object_fetcher->FetchReflog(&reflog);

  if (f == ObjectFetcherT::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogDebug, "fetched reflog '%s' from backend storage",
                                   reflog->database_file().c_str());
    return reflog;
  } else if (f != ObjectFetcherT::kFailNotFound) {
    return NULL;
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
