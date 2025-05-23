/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_HASH_H_
#define CVMFS_PUBLISH_CMD_HASH_H_

#include <string>

#include "publish/command.h"

namespace publish {

class CmdHash : public Command {
 public:
  virtual std::string GetName() const { return "hash"; }
  virtual std::string GetBrief() const { return "CernVM-FS hash functions"; }
  virtual std::string GetDescription() const {
    return "Hash over a string or STDIN with one of the hash functions in "
           "CernVM-FS";
  }
  virtual ParameterList GetParams() const {
    ParameterList p;
    p.push_back(
        Parameter::Mandatory("algorithm", 'a', "algorithm name",
                             "hash algorithm to use (e.g. shake128, sha1)"));
    p.push_back(Parameter::Optional("input", 'i', "string",
                                    "data to hash over (instead of STDIN)"));
    p.push_back(Parameter::Switch("fingerprint", 'f',
                                  "print in fingerprint representation"));
    p.push_back(Parameter::Switch(
        "split", 's', "additionally print the hash as 64bit integer tuples"));
    return p;
  }
  virtual bool IsHidden() const { return true; }

  virtual int Main(const Options &options);
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_HASH_H_
