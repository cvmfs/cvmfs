/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SERVER_TOOL_IMPL_H_
#define CVMFS_SERVER_TOOL_IMPL_H_

#include <string>

template<class ObjectFetcherT>
manifest::Reflog *ServerTool::FetchReflog(ObjectFetcherT *object_fetcher,
                                          const std::string &repo_name,
                                          const shash::Any &reflog_hash) {
  // try to fetch the Reflog from the backend storage first
  manifest::Reflog *reflog = NULL;
  typename ObjectFetcherT::Failures f = object_fetcher->FetchReflog(reflog_hash,
                                                                    &reflog);

  switch (f) {
    case ObjectFetcherT::kFailOk:
      LogCvmfs(kLogCvmfs, kLogDebug, "fetched reflog '%s' from backend storage",
               reflog->database_file().c_str());
      break;

    case ObjectFetcherT::kFailNotFound:
      LogCvmfs(kLogCvmfs, kLogStderr,
               "reflog for '%s' not found but reflog checksum is present "
               "(either in the manifest key 'Y' or in the reflog.chksum file); "
               "run `cvmfs_server check -r` to fix.",
               repo_name.c_str());
      return NULL;

    case ObjectFetcherT::kFailBadData:
      LogCvmfs(kLogCvmfs, kLogStderr,
               "data corruption in .cvmfsreflog for %s, run "
               "`cvmfs_server check -r` to fix",
               repo_name.c_str());
      return NULL;

    default:
      LogCvmfs(kLogCvmfs, kLogStderr, "failed loading reflog (%d - %s)", f,
               Code2Ascii(f));
      return NULL;
  }

  assert(reflog != NULL);
  return reflog;
}

#endif  // CVMFS_SERVER_TOOL_IMPL_H_
