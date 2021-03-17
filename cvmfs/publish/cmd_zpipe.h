/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_CMD_ZPIPE_H_
#define CVMFS_PUBLISH_CMD_ZPIPE_H_

#include <string>

#include "publish/command.h"

namespace publish {

class CmdZpipe : public Command {
 public:
  virtual std::string GetName() const { return "zpipe"; }
  virtual std::string GetBrief() const {
    return "Compress and decompress data with zlib";
  }
  virtual std::string GetDescription() const {
    return "Compresses or decompresses a file using the DEFLATE algorithm.\n"
      "Input comes on stdin, output goes to stdout.";
  }
  virtual ParameterList GetParams() const {
    ParameterList p;
    p.push_back(Parameter::Switch("decompress", 'd',
      "Decompress input data (default is compression)"));
    p.push_back(Parameter::Optional("input", 'i', "file",
      "Path to input file"));
    return p;
  }
  virtual bool IsHidden() const { return true; }

  virtual int Main(const Options &options);
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_CMD_ZPIPE_H_
