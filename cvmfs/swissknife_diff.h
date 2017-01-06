/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_DIFF_H_
#define CVMFS_SWISSKNIFE_DIFF_H_

#include "swissknife.h"

namespace swissknife {

class CommandDiff : public Command {
 public:
  ~CommandDiff() { }
  virtual std::string GetName() const { return "diff"; }
  virtual std::string GetDescription() const {
    return "Show changes between two revisions";
  }
  ParameterList GetParams() const;
  int Main(const ArgumentList &args);

 protected:
  bool Exists(const std::string &repository, const std::string &file) const;
};  // class CommandDiff

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_DIFF_H_