/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_HASH_H_
#define CVMFS_SWISSKNIFE_HASH_H_

#include <string>

#include "swissknife.h"

namespace swissknife {

class CommandHash : public Command {
 public:
  ~CommandHash() { }
  std::string GetName() { return "hash"; }
  std::string GetDescription() {
    return "Hashes stdin and prints out the result on stdout.";
  }
  ParameterList GetParams() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('a', "hash algorithm"));
    r.push_back(Parameter::Switch('f', "print in fingerprint representation"));
    return r;
  }
  int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_HASH_H_
