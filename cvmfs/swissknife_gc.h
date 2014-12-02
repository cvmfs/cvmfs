/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_GC_H_
#define CVMFS_SWISSKNIFE_GC_H_

#include "swissknife.h"
#include "catalog_traversal.h"

namespace swissknife {

class CommandGc : public Command {
 public:
  ~CommandGc() { };
  std::string GetName() { return "gc"; }
  std::string GetDescription() {
    return "Garbage Collect a CernVM-FS repository.";
  };
  ParameterList GetParams();
  int Main(const ArgumentList &args);

 protected:
  bool CheckGarbageCollectability(const std::string &repository_url,
                                  const std::string &repository_name,
                                  const std::string &pubkey_path) const;
};

}

#endif  // CVMFS_SWISSKNIFE_GC_H_
