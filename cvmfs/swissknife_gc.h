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
  virtual std::string GetName() const { return "gc"; }
  virtual std::string GetDescription() const {
    return "Garbage Collect a CernVM-FS repository.";
  }
  virtual ParameterList GetParams() const;
  int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_GC_H_
