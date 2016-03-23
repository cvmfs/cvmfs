/**
 * This file is part of the CernVM File System.
 */

#include "swissknife_reflog.h"

#include <string>

#include "object_fetcher.h"
#include "manifest.h"
#include "upload_facility.h"


namespace swissknife {

typedef HttpObjectFetcher<> ObjectFetcher;

CommandBootstrapReflog::CommandBootstrapReflog() {

}


CommandBootstrapReflog::~CommandBootstrapReflog() {

}


ParameterList CommandBootstrapReflog::GetParams() {
  ParameterList r;
  r.push_back(Parameter::Mandatory('r', "repository url"));
  r.push_back(Parameter::Mandatory('u', "spooler definition string"));
  r.push_back(Parameter::Mandatory('n', "fully qualified repository name"));
  r.push_back(Parameter::Mandatory('t', "temporary directory"));
  r.push_back(Parameter::Mandatory('k', "repository keychain"));
  return r;
}


int CommandBootstrapReflog::Main(const ArgumentList &args) {
  const std::string &repo_url  = *args.find('r')->second;
  const std::string &spooler   = *args.find('u')->second;
  const std::string &repo_name = *args.find('n')->second;
  const std::string &tmp_dir   = *args.find('n')->second;
  const std::string &repo_keys = *args.find('k')->second;

  const bool follow_redirects = false;
  if (!this->InitDownloadManager(follow_redirects) ||
      !this->InitVerifyingSignatureManager(repo_keys)) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to init repo connection");
    return 1;
  }

  ObjectFetcher object_fetcher(repo_name,
                               repo_url,
                               tmp_dir,
                               download_manager(),
                               signature_manager());

  UniquePtr<manifest::Manifest> manifest;
  ObjectFetcher::Failures retval = object_fetcher.FetchManifest(&manifest);
  if (retval != ObjectFetcher::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to load repository manifest "
                                    "(%d - %s)",
                                    retval, Code2Ascii(retval));
    return 1;
  }

  const upload::SpoolerDefinition spooler_definition(spooler, shash::kAny);
  UniquePtr<upload::AbstractUploader> uploader(
                       upload::AbstractUploader::Construct(spooler_definition));

  if (!uploader.IsValid()) {
    LogCvmfs(kLogCvmfs, kLogStderr, "failed to initialize spooler for '%s'",
             spooler.c_str());
    return 1;
  }

  return 0;
}

}  // namespace swissknife
