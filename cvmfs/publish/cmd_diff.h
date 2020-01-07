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
  virtual std::string GetDescription() const {
    return "Shows the added, removed, and modified files between any two "
     "repository revisions. By default, shows the difference between the "
     "current and the previous revision. The repository revision can be given "
     "by a tag name or by the root hash. In the latter case, the hash needs to "
     "start with a '@' symbol to distinguish it from a name.";
  }
  virtual std::string GetUsage() const {
    return "[options] <repository URL>";
  }
  virtual ParameterList GetParams() const {
    ParameterList p;
    p.push_back(Parameter::Optional("keychain", 'k', "directory",
      "Path to the directory containing the repository public key"));
    p.push_back(Parameter::Switch("machine-readable", 'm',
      "Produce machine readble output"));
    p.push_back(Parameter::Optional("from", 's', "repository tag",
      "The source tag name [default='trunk-previous']"));
    p.push_back(Parameter::Optional("to", 'd', "repository tag",
      "The destination tag name [default='trunk']"));
    p.push_back(Parameter::Switch("header", 'h', "Show the header line"));
    p.push_back(Parameter::Switch("ignore-timestamp", 'i',
      "Ignore changes that only differ in their timestamps"));
    return p;
  }
  virtual unsigned GetMinPlainArgs() const { return 1; }

  virtual int Main(const Options &options);
};  // class CmdDiff

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_DIFF_H_
