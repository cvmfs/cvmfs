/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_REFLOG_H_
#define CVMFS_SWISSKNIFE_REFLOG_H_

#include "swissknife.h"

#include <string>

namespace manifest {
class Reflog;
class Manifest;
}

namespace swissknife {

class CommandReconstructReflog : public Command {
 public:
  std::string GetName() { return "reconstruct_reflog"; }
  std::string GetDescription() {
    return "Bootstraps a Reference Log from Catalog and History chains. This "
           "is used for both legacy repository migration and repairs.";
  }
  ParameterList GetParams();
  int Main(const ArgumentList &args);

 protected:
  void AddStaticManifestObjects(manifest::Reflog    *reflog,
                                manifest::Manifest  *manifest) const;
};

};  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_REFLOG_H_
