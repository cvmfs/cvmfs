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
  virtual std::string GetName() const { return "info"; }
  virtual std::string GetDescription() const {
    return "CernVM File System repository information retrieval\n"
           "This command reads the content of a .cvmfspublished file and "
           "exposes it "
           "to the user.";
  }
  ParameterList GetParams() const;
  int Main(const ArgumentList &args);

 protected:
  bool Exists(const std::string &repository, const std::string &file) const;
};

class CommandVersion : public Command {
 public:
  ~CommandVersion() { }
  virtual std::string GetName() const { return "version"; }
  virtual std::string GetDescription() const {
    return "Prints the version of CernVM-FS";
  }
  ParameterList GetParams() const { return ParameterList(); }
  int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_INFO_H_
