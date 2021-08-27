/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_MKFS_H_
#define CVMFS_PUBLISH_CMD_MKFS_H_

#include <string>

#include "publish/command.h"

namespace publish {

class CmdMkfs : public Command {
 public:
  virtual std::string GetName() const { return "mkfs"; }
  virtual std::string GetBrief() const {
    return "Create a new, empty repository";
  }
  virtual ParameterList GetParams() const {
    ParameterList p;
    p.push_back(Parameter::Optional("stratum0", 'w', "stratum0 url",
      "HTTP endpoint of the authoritative storage if not localhost"));

    p.push_back(Parameter::Optional("storage", 'u', "upstream storage",
      "Upstream storage definition if other than local file system"));

    p.push_back(Parameter::Optional("owner", 'o', "user name",
      "User account that should own the published files"));

    p.push_back(Parameter::Switch("replicable", 'm',
      "Enable to mark the repository as source for stratum 1 copies"));

    p.push_back(Parameter::Optional("unionfs", 'f', "aufs | overlayfs",
      "Enforce aufs or overlayfs if both are available"));

    p.push_back(Parameter::Optional("s3config", 's', "s3 config file",
      "Settings for an S3 bucket, use with -w and -u flags"));

    p.push_back(Parameter::Switch("no-autotags", 'g',
      "Disable automatic creation of timestamp tags, useful with --gc"));

    p.push_back(Parameter::Optional("autotag-span", 'G', "timespan",
      "Speficy a `date` compatible time windows for keeping auto tags"));

    p.push_back(Parameter::Optional("hash", 'a', "algorithm",
      "Select a secure hash algorithm: sha1 (default) or rmd160 or shake128"));

    p.push_back(Parameter::Switch("gc", 'z',
      "Make the repository garbage-collectable"));

    p.push_back(Parameter::Switch("volatile", 'v',
      "Mark the repository contents as volatile for client caches"));

    p.push_back(Parameter::Optional("compression", 'Z', "algorithm",
      "Select the compression algorithm: zlib (default) or none"));

    p.push_back(Parameter::Optional("import-keychain", 'k', "directory",
      "Use existing keys instead of creating new ones"));

    p.push_back(Parameter::Optional("export-keychain", 'K', "directory",
      "Write repository keychain to directory different from /etc/cvmfs/keys"));

    p.push_back(Parameter::Switch("no-apache", 'p',
      "Disable creation of Apache config files, e.g. for S3 backend"));

    p.push_back(Parameter::Switch("no-publisher", 'P',
      "Only create repository but do not configure node as a publisher"));

    p.push_back(Parameter::Switch("keycard", 'R',
      "Require a master key card for the whitelist signature"));

    p.push_back(Parameter::Optional("auth", 'V', "auth spec",
      "Set the membership requirement to access the repository"));

    p.push_back(Parameter::Switch("external", 'X',
      "Enable to use this repository for external data"));
    return p;
  }
  virtual std::string GetUsage() const {
    return "[options] <fully qualified repository name>";
  }
  virtual unsigned GetMinPlainArgs() const { return 1; }

  virtual int Main(const Options &options);
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_MKFS_H_
