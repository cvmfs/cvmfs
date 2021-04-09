/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_DIFF_H_
#define CVMFS_PUBLISH_CMD_DIFF_H_

#include <string>
#include <vector>

#include "publish/command.h"

namespace publish {

class CmdDiff : public Command {
 public:
  virtual std::string GetName() const { return "diff"; }
  virtual std::string GetBrief() const {
    return "Show the change set between two repository revisions";
  }
  virtual std::string GetDescription() const {
    return "Shows the added, removed, and modified files between any two "
     "repository revisions. By default, shows the difference between the "
     "current and the previous revision. The repository revision can be given "
     "by a tag name or by the root hash. In the latter case, the hash needs to "
     "start with a '@' symbol to distinguish it from a name.";
  }
  virtual std::string GetUsage() const {
    return "[options] <repository name / url>";
  }
  virtual std::vector<std::string> DoGetExamples() const {
    std::vector<std::string> e;
    e.push_back("-k /etc/cvmfs/keys/cern.ch "
      "http://cvmfs-stratum-one.cern.ch/cvmfs/grid.cern.ch "
      " # use with any repository for which public keys are available");
    e.push_back("myrepo.cvmfs.io "
      "# use with a local stratum 0 or stratum 1 copy");
    e.push_back("--from version1 --to version2 "
      "# compare tags for the one and only stratum 0 or stratum 1 copy "
      "available on this node");
    return e;
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
    p.push_back(Parameter::Switch("worktree", 'w',
      "Show the diff of the unpublished changes in the open transaction"));
    p.push_back(Parameter::Switch("header", 'h', "Show the header line"));
    p.push_back(Parameter::Switch("ignore-timediff", 'i',
      "Ignore changes that only differ in their timestamps"));
    return p;
  }

  virtual int Main(const Options &options);
};  // class CmdDiff

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_DIFF_H_
