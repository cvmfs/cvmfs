/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_COMMAND_H_
#define CVMFS_PUBLISH_COMMAND_H_

#include <string>
#include <vector>

namespace publish {

class Command {
 public:
  typedef std::vector<std::string> ArgList;
  virtual std::string GetName() = 0;
  virtual std::string GetDescription() = 0;

  virtual int Main(ArgList args) = 0;
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_COMMAND_H_
