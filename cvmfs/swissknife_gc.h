/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_GC_H_
#define CVMFS_SWISSKNIFE_GC_H_

#include "swissknife.h"
#include "catalog_traversal.h"

namespace swissknife {

class CommandGC : public Command {
 public:
  ~CommandGC() { };
  std::string GetName() { return "gc"; }
  std::string GetDescription() {
    return "Garbage Collect a CernVM-FS repository.";
  };
  ParameterList GetParams();
  int Main(const ArgumentList &args);

 protected:
  void CatalogCallback(const ReadonlyCatalogTraversal::CallbackData &data);
};

}

#endif  // CVMFS_SWISSKNIFE_GC_H_
