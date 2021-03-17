/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_HELP_H_
#define CVMFS_PUBLISH_CMD_HELP_H_

#include <string>

#include "publish/command.h"

namespace publish {

class CmdHelp : public Command {
 public:
  explicit CmdHelp(CommandList *commands) : commands_(commands) { }
  virtual std::string GetName() const { return "help"; }
  virtual std::string GetBrief() const {
    return "Print information about a command";
  }
  virtual std::string GetUsage() const { return "<command>"; }
  virtual unsigned GetMinPlainArgs() const { return 1; }
  virtual ParameterList GetParams() const { return ParameterList(); }

  virtual int Main(const Options &options);

 private:
  CommandList *commands_;
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_HELP_H_
