/**
 * This file is part of the CernVM File System.
 *
 * This command reads the content of a .cvmfspublished file and exposes it
 * to the user.
 */

#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "swissknife_info.h"

#include <string>

#include "logging.h"
#include "manifest.h"
#include "util.h"
#include "hash.h"
#include "download.h"

using namespace std;  // NOLINT

/**
 * Checks if the given path looks like a remote path
 */
static bool IsRemote(const string &repository) {
  return repository.substr(0, 7) == "http://";
}

/**
 * Checks for existance of a file either locally or via HTTP head
 */
static bool Exists(const string &repository, const string &file) {
  if (IsRemote(repository)) {
    const string url = repository + "/" + file;
    download::JobInfo head(&url, false);
    return download::Fetch(&head) == download::kFailOk;
  } else {
    return FileExists(file);
  }
}

swissknife::ParameterList swissknife::CommandInfo::GetParams() {
  swissknife::ParameterList result;
  result.push_back(Parameter('r', "repository directory / url",
                             false, false));
  result.push_back(Parameter('l', "log level (0-4, default: 2)", true, false));
  result.push_back(Parameter('c', "show root catalog hash", true, true));
  result.push_back(Parameter('n', "show fully qualified repository name", true, true));
  result.push_back(Parameter('t', "show time stamp", true, true));
  result.push_back(Parameter('m', "check if repository is marked as replication master copy", true, true));
  result.push_back(Parameter('h', "print results in human readable form", true, true));
  // to be extended...
  return result;
}


int swissknife::CommandInfo::Main(const swissknife::ArgumentList &args) {
  if (args.find('l') != args.end()) {
    unsigned log_level =
      1 << (kLogLevel0 + String2Uint64(*args.find('l')->second));
    if (log_level > kLogNone) {
      swissknife::Usage();
      return 1;
    }
    SetLogVerbosity(static_cast<LogLevels>(log_level));
  }
  const string repository = MakeCanonicalPath(*args.find('r')->second);

  // Load manifest file
  // Repository can be HTTP address or on local file system
  // TODO: do this using Manifest::Fetch
  //       currently this is not possible, since Manifest::Fetch asks for the
  //       repository name... Which we want to figure out with the tool at hand.
  //       Possible Fix: Allow for a Manifest::Fetch with an empty name.
  manifest::Manifest *manifest = NULL;
  if (IsRemote(repository)) {
    download::Init(1);

    const string url = repository + "/.cvmfspublished";
    download::JobInfo download_manifest(&url, false, false, NULL);
    download::Failures retval = download::Fetch(&download_manifest);
    if (retval != download::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to download manifest (%d)",
               retval);
      return 1;
    }
    char *buffer = download_manifest.destination_mem.data;
    const unsigned length = download_manifest.destination_mem.size;
    manifest = manifest::Manifest::LoadMem(
      reinterpret_cast<const unsigned char *>(buffer), length);
    free(download_manifest.destination_mem.data);
  } else {
    if (chdir(repository.c_str()) != 0) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to switch to directory %s",
               repository.c_str());
      return 1;
    }
    manifest = manifest::Manifest::LoadFile(".cvmfspublished");
  }

  if (!manifest) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load repository manifest");
    return 1;
  }

  // Validate Manifest
  const string certificate_path =
    "data" + manifest->certificate().MakePath(1, 2) + "X";
  if (!Exists(repository, certificate_path)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to find certificate (%s)",
             certificate_path.c_str());
    delete manifest;
    return 1;
  }

  // Check if we should be human readable
  const bool human_readable = (args.count('h') > 0);

  // Get information from the Manifest
  if (args.count('c') > 0) {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s%s",
             (human_readable) ? "Root Catalog Hash:               " : "",
             manifest->catalog_hash().ToString().c_str());
  }

  if (args.count('n') > 0) {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s%s",
             (human_readable) ? "Fully Qualified Repository Name: " : "",
             manifest->repository_name().c_str());
  }

  if (args.count('t') > 0) {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s%d",
             (human_readable) ? "Time Stamp:                      " : "",
             manifest->publish_timestamp());
  }

  if (args.count('m') > 0) {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s%s",
             (human_readable) ? "Replication Master Copy:         " : "",
             (Exists(repository, ".cvmfs_master_replica")) ? "true" : "false");
  }

  delete manifest;
  return 0;
}
