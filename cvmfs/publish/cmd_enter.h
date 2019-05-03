/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_ENTER_H_
#define CVMFS_PUBLISH_CMD_ENTER_H_

#include <string>

#include "publish/command.h"

namespace publish {

class CmdEnter : public Command {
 public:
  explicit CmdEnter() { }
  virtual std::string GetName() const { return "enter"; }
  virtual std::string GetBrief() const {
    return "Opens an ephemeral namespace to publish content";
  }
  virtual std::string GetUsage() const {
    return "[options] <fully qualified repository name>";
  }
  virtual ParameterList GetParams() const { return ParameterList(); }
  virtual unsigned GetMinPlainArgs() const { return 1; }

  virtual int Main(const Options &options);
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_ENTER_H_
