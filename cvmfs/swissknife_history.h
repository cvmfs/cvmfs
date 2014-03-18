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
    ParameterList r;
    r.push_back(Parameter::Mandatory('r', "repository directory / url"));
    r.push_back(Parameter::Mandatory('b', "base hash"));
    r.push_back(Parameter::Mandatory('t', "trunk hash"));
    r.push_back(Parameter::Mandatory('s', "trunk catalog size"));
    r.push_back(Parameter::Mandatory('i', "trunk revision"));
    r.push_back(Parameter::Mandatory('n', "repository name"));
    r.push_back(Parameter::Mandatory('k', "repository public key"));
    r.push_back(Parameter::Mandatory('o', "history db output file"));
    r.push_back(Parameter::Optional ('d', "delete a tag"));
    r.push_back(Parameter::Optional ('a', "add a tag (format: \"name@channel@desc\")"));
    r.push_back(Parameter::Optional ('h', "tag hash (if different from trunk)"));
    r.push_back(Parameter::Switch   ('l', "list tags"));
    r.push_back(Parameter::Optional ('z', "trusted certificate dir(s)"));

    return r;
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
    ParameterList r;
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    r.push_back(Parameter::Mandatory('u', "repository directory / url"));
    r.push_back(Parameter::Mandatory('b', "base hash"));
    r.push_back(Parameter::Mandatory('n', "repository name"));
    r.push_back(Parameter::Mandatory('k', "repository public key"));
    r.push_back(Parameter::Mandatory('o', "history db output file"));
    r.push_back(Parameter::Mandatory('m', "manifest output file"));
    r.push_back(Parameter::Mandatory('t', "revert to this tag"));
    r.push_back(Parameter::Mandatory('d', "temp directory"));
    r.push_back(Parameter::Optional ('z', "trusted certificate dir(s)"));
    return r;
  }
  int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_HISTORY_H_
