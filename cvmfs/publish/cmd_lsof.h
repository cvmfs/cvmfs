/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_LSOF_H_
#define CVMFS_PUBLISH_CMD_LSOF_H_

#include <string>

#include "publish/command.h"

namespace publish {

class CmdLsof : public Command {
 public:
  virtual std::string GetName() const { return "lsof"; }
  virtual std::string GetBrief() const { return "Internal helper"; }
  virtual std::string GetDescription() const {
    return "Internal helper. Will be removed. Don't use.";
  }
  virtual std::string GetUsage() const { return "<path>"; }
  virtual unsigned GetMinPlainArgs() const { return 1; }
  virtual ParameterList GetParams() const { return ParameterList(); }
  virtual bool IsHidden() const { return true; }

  virtual int Main(const Options &options);
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_LSOF_H_
