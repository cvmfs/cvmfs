/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_ZPIPE_H_
#define CVMFS_SWISSKNIFE_ZPIPE_H_

#include <string>

#include "swissknife.h"

namespace swissknife {

class CommandZpipe : public Command {
 public:
  ~CommandZpipe() { }
  virtual std::string GetName() const { return "zpipe"; }
  virtual std::string GetDescription() const {
    return "Compresses or decompresses a file using the DEFLATE algorithm.\n"
           "Input comes on stdin, output goes to stdout.";
  }
  virtual ParameterList GetParams() const {
    ParameterList r;
    r.push_back(Parameter::Switch('d', "decompress file"));
    return r;
  }
  virtual int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_ZPIPE_H_
