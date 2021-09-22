/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_ABORT_H_
#define CVMFS_PUBLISH_CMD_ABORT_H_

#include <string>
#include <vector>

#include "publish/command.h"

namespace publish {

class CmdAbort : public Command {
 public:
  virtual std::string GetName() const { return "abort"; }
  virtual std::string GetBrief() const {
    return "Abort the currently open transaction";
  }
  virtual std::string GetDescription() const {
    return "Returns to the state before the current transaction was opened. "
      "All changes staged in the current transaction are discarded. "
      "Repositories attached to a gateway return their lease.";
  }
  virtual std::string GetUsage() const {
    return "[options] <repository name>";
  }
  virtual std::vector<std::string> DoGetExamples() const {
    std::vector<std::string> e;
    e.push_back("-f myrepo.cvmfs.io  "
      "# abort without asking for confirmation");
    return e;
  }
  virtual ParameterList GetParams() const {
    ParameterList p;
    p.push_back(Parameter::Switch("force", 'f',
      "Do not ask for confirmation"));
    return p;
  }

  virtual int Main(const Options &options);
};  // class CmdAbort

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_ABORT_H_
