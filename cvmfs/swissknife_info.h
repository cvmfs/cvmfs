/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_INFO_H_
#define CVMFS_SWISSKNIFE_INFO_H_

#include "swissknife.h"

namespace swissknife {

class CommandInfo : public Command<CommandInfo> {
 public:
  CommandInfo(const std::string &param) : Command(param) {}
  ~CommandInfo() { };
  static std::string GetName() { return "info"; }
  static std::string GetDescription() {
    return "CernVM File System repository information retrieval\n"
      "This command reads the content of a .cvmfspublished file and exposes it "
      "to the user.";
  };
  static ParameterList GetParameters();
  int Run(const ArgumentList &args);
};

class CommandVersion : public Command<CommandVersion> {
 public:
  CommandVersion(const std::string &param) : Command(param) {}
  ~CommandVersion() {};
  static std::string   GetName()        { return "version";                     }
  static std::string   GetDescription() { return "Prints version of CernVM-FS"; }
  static ParameterList GetParameters()  { return ParameterList();               }
  int Run(const ArgumentList &args);
};

}

#endif  // CVMFS_SWISSKNIFE_INFO_H_
