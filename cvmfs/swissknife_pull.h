/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_PULL_H_
#define CVMFS_SWISSKNIFE_PULL_H_

#include "swissknife.h"

namespace swissknife {

class CommandPull : public Command {
 public:
  ~CommandPull() { };
  std::string GetName() { return "pull"; };
  std::string GetDescription() {
    return "Makes a Stratum 1 replica of a Stratum 0 repository.";
  };
  ParameterList GetParams() {
    ParameterList result;
    return result;
  }
  int Main(const ArgumentList &args);
};

}

#endif  // CVMFS_SWISSKNIFE_PULL_H_
