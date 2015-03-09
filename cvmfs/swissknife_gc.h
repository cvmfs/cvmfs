/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_GC_H_
#define CVMFS_SWISSKNIFE_GC_H_

#include <string>

#include "catalog_traversal.h"
#include "swissknife.h"

namespace swissknife {

class CommandGc : public Command {
 public:
  ~CommandGc() { }
  std::string GetName() { return "gc"; }
  std::string GetDescription() {
    return "Garbage Collect a CernVM-FS repository.";
  }
  ParameterList GetParams();
  int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_GC_H_
