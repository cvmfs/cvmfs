/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_LETTER_H_
#define CVMFS_SWISSKNIFE_LETTER_H_

#include "swissknife.h"

namespace swissknife {

class CommandLetter : public Command {
 public:
  ~CommandLetter() { };
  std::string GetName() { return "letter"; };
  std::string GetDescription() {
    return "Signs aribtrary text with the repository certificate.";
  };
  ParameterList GetParams() {
    ParameterList result;
    result.push_back(Parameter('s', "sign text", true, true));
    result.push_back(Parameter('a', "hash algorithm", true, false));
    result.push_back(Parameter('c', "x509 certificate", true, false));
    result.push_back(Parameter('k', "private key of the certificate "
                               "or public master key", true, false));
    result.push_back(Parameter('p', "password for the private key",
                               true, false));
    result.push_back(Parameter('v', "verify text", true, true));
    result.push_back(Parameter('m', "max age (seconds)", true, false));
    result.push_back(Parameter('z', "trusted certificate dir(s)", true, false));
    result.push_back(Parameter('r', "repository url", true, false));
    result.push_back(Parameter('l', "verify in a loop until EOF", true, true));
    result.push_back(Parameter('t', "text to sign or verify", true, false));
    result.push_back(Parameter('f', "fully qualified repository name",
                     false, false));
    return result;
  }
  int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_LETTER_H_
