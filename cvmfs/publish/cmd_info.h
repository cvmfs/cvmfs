/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_STAT_H_
#define CVMFS_PUBLISH_CMD_STAT_H_

#include "publish/command.h"

namespace publish {

class CmdInfo : public Command {
 public:
  virtual std::string GetName() const { return "info"; }
  virtual std::string GetBrief() const {
    return "Retrieves global repository information";
  }
  virtual std::string GetUsage() const {
    return "[options] <repository URL>";
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
  virtual unsigned GetMinPlainArgs() const { return 1; }

  virtual int Main(const Options &options);
};  // class CmdInfo

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_STAT_H_
