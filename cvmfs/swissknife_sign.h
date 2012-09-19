/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_SIGN_H_
#define CVMFS_SWISSKNIFE_SIGN_H_

#include "swissknife.h"

namespace swissknife {

class CommandSign : public Command {
 public:
  ~CommandSign() { };
  std::string GetName() { return "sign"; };
  std::string GetDescription() {
    return "Adds a signature to the repository manifest.";
  };
  ParameterList GetParams() {
    ParameterList result;
    result.push_back(Parameter('m', "manifest file", false, false));
    result.push_back(Parameter('r', "spooler definition", false, false));
    result.push_back(Parameter('t', "directory for temporary files",
                               false, false));
    result.push_back(Parameter('c', "x509 certificate", true, false));
    result.push_back(Parameter('k', "private key of the certificate",
                               true, false));
    result.push_back(Parameter('s', "password for the private key",
                               true, false));
    result.push_back(Parameter('n', "repository name", true, false));
    return result;
  }
  int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_SIGN_H_
