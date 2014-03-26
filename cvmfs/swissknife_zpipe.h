/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_ZPIPE_H_
#define CVMFS_SWISSKNIFE_ZPIPE_H_

#include "swissknife.h"

namespace swissknife {

class CommandZpipe : public Command<CommandZpipe> {
 public:
  CommandZpipe(const std::string &param) : Command(param) {}
  ~CommandZpipe() { };
  static std::string GetName() { return "zpipe"; };
  static std::string GetDescription() {
    return "Compresses or decompresses a file using the DEFLATE algorithm.\n"
      "Input comes on stdin, output goes to stdout.";
  };
  static ParameterList GetParameters() {
    ParameterList r;
    r.push_back(Parameter::Switch('d', "decompress file"));
    return r;
  }
  int Run(const ArgumentList &args);
};

}

#endif  // CVMFS_SWISSKNIFE_ZPIPE_H_
