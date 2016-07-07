/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_INFO_H_
#define CVMFS_SWISSKNIFE_INFO_H_

#include <string>

#include "swissknife.h"

namespace swissknife {

class CommandInfo : public Command {
 public:
  ~CommandInfo() { }
  std::string GetName() { return "info"; }
  std::string GetDescription() {
    return "CernVM File System repository information retrieval\n"
      "This command reads the content of a .cvmfspublished file and exposes it "
      "to the user.";
  }
  ParameterList GetParams();
  int Main(const ArgumentList &args);

 protected:
  bool Exists(const std::string &repository, const std::string &file) const;
};

class CommandVersion : public Command {
 public:
  ~CommandVersion() { }
  std::string GetName()        { return "version";                         }
  std::string GetDescription() { return "Prints the version of CernVM-FS"; }
  ParameterList GetParams()    { return ParameterList();                   }
  int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_INFO_H_
