/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_H_
#define CVMFS_SWISSKNIFE_H_

#include <cassert>
#include <map>
#include <string>
#include <vector>

#include "download.h"
#include "manifest_fetch.h"
#include "signature.h"
#include "statistics.h"
#include "util.h"

namespace download {
class DownloadManager;
}
namespace signature {
class SignatureManager;
}
namespace perf {
class Statistics;
}
namespace manifest {
class Manifest;
}

namespace swissknife {

class Parameter {
 public:
  static Parameter Mandatory(const char key, const std::string &desc) {
    return Parameter(key, desc, false, false);
  }
  static Parameter Optional(const char key, const std::string &desc) {
    return Parameter(key, desc, true, false);
  }
  static Parameter Switch(const char key, const std::string &desc) {
    return Parameter(key, desc, true, true);
  }

  char key() const { return key_; }
  const std::string& description() const { return description_; }
  bool optional() const { return optional_; }
  bool mandatory() const { return !optional_; }
  bool switch_only() const { return switch_only_; }

 protected:
  Parameter(const char          key,
            const std::string  &desc,
            const bool          opt,
            const bool          switch_only) :
    key_(key),
    description_(desc),
    optional_(opt),
    switch_only_(switch_only)
  {
    assert(!switch_only_ || optional_);  // switches are optional by definition
  }

 private:
  char key_;
  std::string description_;
  bool optional_;
  bool switch_only_;
};

typedef std::vector<Parameter> ParameterList;
typedef std::map<char, std::string *> ArgumentList;

class Command {
 public:
  Command() { }
  virtual ~Command();
  virtual std::string GetName() = 0;
  virtual std::string GetDescription() = 0;
  virtual ParameterList GetParams() = 0;
  virtual int Main(const ArgumentList &args) = 0;

 protected:
  bool InitDownloadManager(const bool     follow_redirects,
                           const unsigned max_pool_handles = 1,
                           const bool     use_system_proxy = true);
  bool InitVerifyingSignatureManager(const std::string &pubkey_path,
                                     const std::string &trusted_certs = "");
  bool InitSigningSignatureManager(const std::string &certificate_path,
                                   const std::string &private_key_path,
                                   const std::string &private_key_password);

  manifest::Manifest* OpenLocalManifest(const std::string path) const;
  manifest::Failures  FetchRemoteManifestEnsemble(
                              const std::string &repository_url,
                              const std::string &repository_name,
                                    manifest::ManifestEnsemble *ensemble) const;
  manifest::Manifest* FetchRemoteManifest(
                             const std::string &repository_url,
                             const std::string &repository_name,
                             const shash::Any  &base_hash = shash::Any()) const;

  download::DownloadManager*   download_manager()  const;
  signature::SignatureManager* signature_manager() const;
  perf::Statistics*            statistics() { return &statistics_; }

 private:
  UniquePtr<download::DownloadManager>    download_manager_;
  UniquePtr<signature::SignatureManager>  signature_manager_;
  perf::Statistics                        statistics_;
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_H_
