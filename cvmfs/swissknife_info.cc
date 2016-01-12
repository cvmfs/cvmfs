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

#include "download.h"
#include "hash.h"
#include "logging.h"
#include "manifest.h"
#include "util.h"

using namespace std;  // NOLINT

namespace swissknife {

/**
 * Checks if the given path looks like a remote path
 */
static bool IsRemote(const string &repository) {
  return repository.substr(0, 7) == "http://";
}

/**
 * Checks for existance of a file either locally or via HTTP head
 */
bool CommandInfo::Exists(const string &repository, const string &file) const {
  if (IsRemote(repository)) {
    const string url = repository + "/" + file;
    download::JobInfo head(&url, false);
    return download_manager()->Fetch(&head) == download::kFailOk;
  } else {
    return FileExists(file);
  }
}

ParameterList CommandInfo::GetParams() {
  swissknife::ParameterList r;
  r.push_back(Parameter::Mandatory('r', "repository directory / url"));
  r.push_back(Parameter::Optional('u', "repository mount point"));
  r.push_back(Parameter::Optional('l', "log level (0-4, default: 2)"));
  r.push_back(Parameter::Switch('c', "show root catalog hash"));
  r.push_back(Parameter::Switch('C', "show mounted root catalog hash"));
  r.push_back(Parameter::Switch('n', "show fully qualified repository name"));
  r.push_back(Parameter::Switch('t', "show time stamp"));
  r.push_back(Parameter::Switch('m', "check if repository is marked as "
                                        "replication master copy"));
  r.push_back(Parameter::Switch('v', "repository revision number"));
  r.push_back(Parameter::Switch('g', "check if repository is garbage "
                                        "collectable"));
  r.push_back(Parameter::Switch('h', "print results in human readable form"));
  r.push_back(Parameter::Switch('L', "follow HTTP redirects"));
  r.push_back(Parameter::Switch('X', "show whether external data is supported "
                                        "in the root catalog."));
  r.push_back(Parameter::Switch('M', "print repository meta info."));
  r.push_back(Parameter::Switch('R', "print raw manifest."));
  return r;
}


int swissknife::CommandInfo::Main(const swissknife::ArgumentList &args) {
  if (args.find('l') != args.end()) {
    unsigned log_level =
      1 << (kLogLevel0 + String2Uint64(*args.find('l')->second));
    if (log_level > kLogNone) {
      LogCvmfs(kLogCvmfs, kLogStderr, "invalid log level");
      return 1;
    }
    SetLogVerbosity(static_cast<LogLevels>(log_level));
  }
  const string mount_point = (args.find('u') != args.end())
                           ?  *args.find('u')->second
                           :  "";
  const string repository = MakeCanonicalPath(*args.find('r')->second);

  // sanity check
  if (args.count('C') > 0 && mount_point.empty()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "need a CernVM-FS mountpoint (-u) for -C");
    return 1;
  }

  // Load manifest file
  // Repository can be HTTP address or on local file system
  // TODO(jblomer): do this using Manifest::Fetch
  //       currently this is not possible, since Manifest::Fetch asks for the
  //       repository name... Which we want to figure out with the tool at hand.
  //       Possible Fix: Allow for a Manifest::Fetch with an empty name.
  manifest::Manifest *manifest = NULL;
  if (IsRemote(repository)) {
    const bool follow_redirects = args.count('L') > 0;
    if (!this->InitDownloadManager(follow_redirects)) {
      return 1;
    }

    const string url = repository + "/.cvmfspublished";
    download::JobInfo download_manifest(&url, false, false, NULL);
    download::Failures retval = download_manager()->Fetch(&download_manifest);
    if (retval != download::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to download manifest (%d - %s)",
               retval, download::Code2Ascii(retval));
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
  const string certificate_path = "data/" + manifest->certificate().MakePath();
  if (!Exists(repository, certificate_path)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to find certificate (%s)",
             certificate_path.c_str());
    delete manifest;
    return 1;
  }

  // Check if we should be human readable
  const bool human_readable = (args.count('h') > 0);

  // Get information from the mount point
  if (args.count('C') > 0) {
    assert(!mount_point.empty());
    const std::string root_hash_xattr = "user.root_hash";
    std::string root_hash;
    const bool success = platform_getxattr(mount_point,
                                           root_hash_xattr,
                                           &root_hash);
    if (!success) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to retrieve extended attribute "
                                      " '%s' from '%s' (errno: %d)",
                                      root_hash_xattr.c_str(),
                                      mount_point.c_str(),
                                      errno);
      return 1;
    }
    LogCvmfs(kLogCvmfs, kLogStdout, "%s%s",
             (human_readable) ? "Mounted Root Hash:               " : "",
             root_hash.c_str());
  }

  // Get information about external data
  if (args.count('X') > 0) {
    assert(!mount_point.empty());
    const std::string external_data_xattr = "user.external_data";
    std::string external_data;
    const bool success = platform_getxattr(mount_point,
                                           external_data_xattr,
                                           &external_data);
    if (!success) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to retrieve extended attribute "
                                      " '%s' from '%s' (errno: %d)",
                                      external_data_xattr.c_str(),
                                      mount_point.c_str(),
                                      errno);
      return 1;
    }
    LogCvmfs(kLogCvmfs, kLogStdout, "%s%s",
             (human_readable) ? "External data enabled:               " : "",
             external_data.c_str());
  }

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

  if (args.count('v') > 0) {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s%s",
             (human_readable) ? "Revision:                        " : "",
             (StringifyInt(manifest->revision())).c_str());
  }

  if (args.count('g') > 0) {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s%s",
             (human_readable) ? "Garbage Collectable:             " : "",
             (StringifyBool(manifest->garbage_collectable())).c_str());
  }

  if (args.count('M') > 0) {
    shash::Any meta_info(manifest->meta_info());
    if (meta_info.IsNull()) {
      if (human_readable)
        LogCvmfs(kLogCvmfs, kLogStderr, "no meta info available");
      return 0;
    }
    const string url = repository + "/data/" + meta_info.MakePath();
    download::JobInfo download_metainfo(&url, true, false, &meta_info);
    download::Failures retval = download_manager()->Fetch(&download_metainfo);
    if (retval != download::kFailOk) {
      if (human_readable)
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "failed to download meta info (%d - %s)",
                 retval, download::Code2Ascii(retval));
      return 1;
    }
    string info(download_metainfo.destination_mem.data,
                download_metainfo.destination_mem.size);
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "%s", info.c_str());
  }

  if (args.count('R') > 0) {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "%s",
             manifest->ExportString().c_str());
  }

  delete manifest;
  return 0;
}


//------------------------------------------------------------------------------


int CommandVersion::Main(const ArgumentList &args) {
  LogCvmfs(kLogCvmfs, kLogStdout, "%s", PACKAGE_VERSION);
  return 0;
}

}  // namespace swissknife
