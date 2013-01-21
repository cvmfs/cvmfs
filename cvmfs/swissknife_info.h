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
  ParameterList GetParams();
  int Main(const ArgumentList &args);
};

}

#endif  // CVMFS_SWISSKNIFE_INFO_H_
