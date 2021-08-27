/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_TRANSACTION_H_
#define CVMFS_PUBLISH_CMD_TRANSACTION_H_

#include <string>
#include <vector>

#include "publish/command.h"

namespace publish {

class CmdTransaction : public Command {
 public:
  virtual std::string GetName() const { return "transaction"; }
  virtual std::string GetBrief() const {
    return "Open a managed repository for writing";
  }
  virtual std::string GetDescription() const {
    return "A transaction gets a lease for a repository in order to write "
      "new content. The content is not visible to clients until publication. "
      "Transactions can be aborted in order to revert to the original state. "
      "Repositories that are configured to use a CernVM-FS gateway can lock "
      "certain sub paths, such that other publisher nodes can publish "
      "concurrently to other sub trees.";
  }
  virtual std::string GetUsage() const {
    return "[options] <repository name>[path]";
  }
  virtual std::vector<std::string> DoGetExamples() const {
    std::vector<std::string> e;
    e.push_back("example.cvmfs.io "
      "# normal use with a locally managed repository");
    e.push_back("example.cvmfs.io/64bit/v42 "
      "# locks only the given sub tree, use with gateway "
      "(no space between the repository name and the path)");
    e.push_back("example.cvmfs.io/popular/path -t 500 "
      "# retries for a maximum of 5 minutes to lock /popular/path");
    return e;
  }
  virtual ParameterList GetParams() const {
    ParameterList p;
    p.push_back(Parameter::Optional("retry-timeout", 't', "seconds",
      "Retry for a maximum number of given seconds if repository is busy "
      "(0 for infinite)"));
    p.push_back(Parameter::Optional("template", 'T', "from-dir=to-dir",
      "Clone directory 'from-dir' to 'to-dir' as part of opening the "
      "transaction"));
    p.push_back(Parameter::Optional("template-from", 'U', "from-dir",
      "Use -U and -V as an alternative to the -T parmeter"));
    p.push_back(Parameter::Optional("template-to", 'V', "to-dir",
      "Use -U and -V as an alternative to the -T parmeter"));
    return p;
  }

  virtual int Main(const Options &options);
};  // class CmdTransaction

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_TRANSACTION_H_
