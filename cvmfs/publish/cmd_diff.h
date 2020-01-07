/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_DIFF_H_
#define CVMFS_PUBLISH_CMD_DIFF_H_

#include <string>

#include "publish/command.h"

namespace publish {

class CmdDiff : public Command {
 public:
  virtual std::string GetName() const { return "diff"; }
  virtual std::string GetBrief() const {
    return "Shows the change set between two repository revisions";
  }
  virtual std::string GetUsage() const {
    return "[options] <repository URL>";
  }
  virtual ParameterList GetParams() const {
    ParameterList p;
    p.push_back(Parameter::Optional("keychain", 'k', "directory",
      "Path to the directory containing the repository public key"));
    return p;
  }
  virtual unsigned GetMinPlainArgs() const { return 1; }

  virtual int Main(const Options &options);
};  // class CmdDiff

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_DIFF_H_
