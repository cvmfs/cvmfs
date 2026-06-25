/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_TAG_H_
#define CVMFS_PUBLISH_CMD_TAG_H_

#include <string>

#include "publish/command.h"

namespace publish {

class CmdTag : public Command {
 public:
  virtual std::string GetName() const { return "tag"; }
  virtual std::string GetBrief() const {
    return "Add or remove named snapshots (tags) of a gateway repository";
  }
  virtual std::string GetDescription() const {
    return "Edits the tag database of a repository connected to a CernVM-FS "
           "gateway. As the publisher has no direct write access to the "
           "storage, a lease is acquired and the change is applied by the "
           "gateway receiver in a single commit. The repository catalog is "
           "left unchanged. This command is used internally by "
           "'cvmfs_server tag' for gateway repositories.";
  }
  virtual std::string GetUsage() const {
    return "[options] <repository name>";
  }
  virtual ParameterList GetParams() const {
    ParameterList p;
    p.push_back(Parameter::Optional("add", 'a', "tag name",
                                    "Name of the tag to add for the current "
                                    "repository revision"));
    p.push_back(Parameter::Optional("description", 'D', "description",
                                    "Description of the tag to add"));
    p.push_back(Parameter::Optional("remove", 'd', "tag names",
                                    "Space-separated list of tags to remove"));
    return p;
  }
  virtual unsigned GetMinPlainArgs() const { return 1; }

  virtual int Main(const Options &options);
};  // class CmdTag

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_TAG_H_
