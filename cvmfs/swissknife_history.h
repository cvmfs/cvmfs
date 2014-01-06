/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_HISTORY_H_
#define CVMFS_SWISSKNIFE_HISTORY_H_

#include <string>
#include "swissknife.h"

namespace swissknife {

class CommandTag : public Command {
 public:
  ~CommandTag() { };
  std::string GetName() { return "tag"; };
  std::string GetDescription() {
    return "Tags a snapshot.";
  };
  ParameterList GetParams() {
    ParameterList result;
    result.push_back(Parameter('r', "repository directory / url",
                               false, false));
    result.push_back(Parameter('b', "base hash", false, false));
    result.push_back(Parameter('t', "trunk hash", false, false));
    result.push_back(Parameter('s', "trunk catalog size", false, false));
    result.push_back(Parameter('i', "trunk revision", false, false));
    result.push_back(Parameter('n', "repository name", false, false));
    result.push_back(Parameter('k', "repository public key", false, false));
    result.push_back(Parameter('o', "history db output file",
                               false, false));
    result.push_back(Parameter('d', "delete a tag", true, false));
    result.push_back(Parameter('a', "add a tag (format: \"name@channel@desc\")",
                               true, false));
    result.push_back(Parameter('h', "tag hash (if different from trunk)",
                               true, false));
    result.push_back(Parameter('l', "list tags", true, true));
    result.push_back(Parameter('z', "trusted certificate dir(s)", true, false));

    return result;
  }
  int Main(const ArgumentList &args);
};


class CommandRollback : public Command {
 public:
  ~CommandRollback() { };
  std::string GetName() { return "rollback"; };
  std::string GetDescription() {
    return "Re-publishes a previous tagged snapshot.  All intermediate "
           "snapshots become inaccessible.";
  };
  ParameterList GetParams() {
    ParameterList result;
    result.push_back(Parameter('r', "spooler definition", false, false));
    result.push_back(Parameter('u', "repository directory / url",
                               false, false));
    result.push_back(Parameter('b', "base hash", false, false));
    result.push_back(Parameter('n', "repository name", false, false));
    result.push_back(Parameter('k', "repository public key", false, false));
    result.push_back(Parameter('o', "history db output file",
                               false, false));
    result.push_back(Parameter('m', "manifest output file", false, false));
    result.push_back(Parameter('t', "revert to this tag", false, false));
    result.push_back(Parameter('d', "temp directory", false, false));
    result.push_back(Parameter('z', "trusted certificate dir(s)", true, false));
    return result;
  }
  int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_HISTORY_H_
