/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_INFO_H_
#define CVMFS_SWISSKNIFE_INFO_H_

#include "swissknife.h"

namespace swissknife {

class CommandInfo : public Command {
 public:
  ~CommandInfo() { };
  std::string GetName() { return "info"; };
  std::string GetDescription() {
    return "CernVM File System repository information retrieval\n"
      "This command reads the content of a .cvmfspublished file and exposes it "
      "to the user.";
  };
  ParameterList GetParams() {
    ParameterList result;
    result.push_back(Parameter('r', "repository directory / url",
                               false, false));
    result.push_back(Parameter('l', "log level (0-4, default: 2)", true, false));
    result.push_back(Parameter('c', "show root catalog hash", true, true));
    result.push_back(Parameter('n', "show fully qualified repository name", true, true));
    result.push_back(Parameter('t', "show time stamp", true, true));
    result.push_back(Parameter('h', "print results in human readable form", true, true));
    // to be extended...
    return result;
  }
  int Main(const ArgumentList &args);
};

}

#endif  // CVMFS_SWISSKNIFE_INFO_H_
