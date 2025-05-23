/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_LETTER_H_
#define CVMFS_SWISSKNIFE_LETTER_H_

#include <string>

#include "swissknife.h"

namespace swissknife {

class CommandLetter : public Command {
 public:
  ~CommandLetter() { }
  virtual std::string GetName() const { return "letter"; }
  virtual std::string GetDescription() const {
    return "Signs arbitrary text with the repository certificate.";
  }
  virtual ParameterList GetParams() const {
    ParameterList r;
    r.push_back(Parameter::Switch('s', "sign text"));
    r.push_back(Parameter::Optional('a', "hash algorithm"));
    r.push_back(Parameter::Optional('c', "x509 certificate"));
    r.push_back(Parameter::Optional('k', "private key of the certificate "
                                         "or public master key"));
    r.push_back(Parameter::Optional('p', "password for the private key"));
    r.push_back(Parameter::Switch('v', "verify text"));
    r.push_back(Parameter::Optional('m', "max age (seconds)"));
    r.push_back(Parameter::Optional('r', "repository url"));
    r.push_back(Parameter::Switch('e', "Erlang mode (stay active)"));
    r.push_back(Parameter::Optional('t', "text to sign or verify"));
    r.push_back(Parameter::Optional('@', "proxy url"));
    r.push_back(Parameter::Mandatory('f', "fully qualified repository name"));
    return r;
  }
  int Main(const ArgumentList &args);

 private:
  static const unsigned kDefaultMaxAge = 300;  // 5 minutes
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_LETTER_H_
