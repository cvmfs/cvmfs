/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_COMMIT_H_
#define CVMFS_PUBLISH_CMD_COMMIT_H_

#include <string>
#include <vector>

#include "publish/command.h"

namespace publish {

class CmdCommit : public Command {
 public:
  virtual std::string GetName() const { return "commit"; }

  virtual std::string GetBrief() const {
    return "Commit changes made inside the ephemeral shell";
  }

  virtual std::string GetDescription() const {
    return "Commit new content from the ephemeral shell";
  }

  virtual std::string GetUsage() const { return "[options] <repository name>"; }

  virtual ParameterList GetParams() const {
    ParameterList p;
    p.push_back(Parameter::Optional(
        "repo-config", 'x', "repository configuration",
        "Path to the configuration of the repository gateway"));
    return p;
  }

  virtual std::vector<std::string> GetExamples() const {
    std::vector<std::string> e;
    e.push_back("commit myrepo.cvmfs.io "
                "# commit changes to myrepo.cvmfs.io");
    return e;
  }

  virtual int Main(const Options &options);
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_COMMIT_H_
