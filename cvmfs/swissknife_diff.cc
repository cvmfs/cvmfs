/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "swissknife_diff.h"

#include <cassert>
#include <string>

#include "download.h"
#include "util/pointer.h"
#include "util/posix.h"

using namespace std;  // NOLINT

namespace swissknife {

/**
 * Checks if the given path looks like a remote path
 */
static bool IsRemote(const string &repository) {
  return repository.substr(0, 7) == "http://";
}

ParameterList CommandDiff::GetParams() const {
  swissknife::ParameterList r;
  r.push_back(Parameter::Mandatory('r', "repository directory / url"));
  r.push_back(Parameter::Mandatory('n', "repository name"));
  r.push_back(Parameter::Switch('L', "follow HTTP redirects"));
  return r;
}


int swissknife::CommandDiff::Main(const swissknife::ArgumentList &args) {
  const string fqrn = MakeCanonicalPath(*args.find('n')->second);
  const string repository = MakeCanonicalPath(*args.find('r')->second);
  if (IsRemote(repository)) {
    const bool follow_redirects = args.count('L') > 0;
    if (!this->InitDownloadManager(follow_redirects)) {
      return 1;
    }
  }

  InitVerifyingSignatureManager("/etc/cvmfs/keys/cern.ch/cern.ch.pub");

  UniquePtr<manifest::Manifest> manifest(FetchRemoteManifest(repository, fqrn));
  assert(manifest.IsValid());
  printf("%s\n", manifest->catalog_hash().ToString().c_str());

  return 0;
}

}  // namespace swissknife
