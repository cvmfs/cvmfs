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
    ParameterList r;
    r.push_back(Parameter::Mandatory('u', "repository url"));
    r.push_back(Parameter::Mandatory('m', "repository name"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    r.push_back(Parameter::Mandatory('k', "repository master key(s)"));
    r.push_back(Parameter::Optional ('y', "trusted certificate directories"));
    r.push_back(Parameter::Mandatory('x', "directory for temporary files"));
    r.push_back(Parameter::Optional ('n', "number of download threads"));
    r.push_back(Parameter::Optional ('l', "log level (0-4, default: 2)"));
    r.push_back(Parameter::Optional ('t', "timeout (s)"));
    r.push_back(Parameter::Optional ('a', "number of retries"));
    r.push_back(Parameter::Switch   ('p', "pull catalog history, too"));
    r.push_back(Parameter::Switch   ('c', "preload cache instead of stratum 1"));
    return r;
  }
  int Main(const ArgumentList &args);
};

}

#endif  // CVMFS_SWISSKNIFE_PULL_H_
