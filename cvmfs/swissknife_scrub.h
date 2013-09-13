/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_SCRUB_H_
#define CVMFS_SWISSKNIFE_SCRUB_H_

#include "swissknife.h"

namespace swissknife {

class CommandScrub : public Command {
 public:
  ~CommandScrub() { };
  std::string GetName() { return "info"; }
  std::string GetDescription() {
    return "CernVM File System repository file storage checker. Finds silent "
           "disk corruptions by recomputing all file content checksums in the "
           "backend file storage.";
  };
  ParameterList GetParams();
  int Main(const ArgumentList &args);
};

}

#endif  // CVMFS_SWISSKNIFE_SCRUB_H_
