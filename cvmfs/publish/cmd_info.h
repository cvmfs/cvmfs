/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_INFO_H_
#define CVMFS_PUBLISH_CMD_INFO_H_

#include <string>

#include "publish/command.h"

namespace publish {

class CmdInfo : public Command {
 public:
  virtual std::string GetName() const { return "info"; }
  virtual std::string GetBrief() const {
    return "Show summary information about a repository";
  }
  virtual std::string GetDescription() const {
    return "Shows high-level data about a repository, such as its name, "
      "whitelist expiry, etc. For stratum 0/1 repositories managed on the "
      "machine, additionally shows the main configuration settings.";
  }
  virtual std::string GetUsage() const {
    return "[options] <repository name / URL>";
  }
  virtual ParameterList GetParams() const {
    ParameterList p;
    p.push_back(Parameter::Optional("keychain", 'k', "directory",
      "Path to the directory containing the repository public key"));
    p.push_back(Parameter::Switch("meta-info", 'm',
      "Print the repository global meta information"));
    p.push_back(Parameter::Optional("stats", 's', "file",
      "Path to sqlite statistics output file"));
    return p;
  }

  virtual int Main(const Options &options);
};  // class CmdInfo

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_INFO_H_
