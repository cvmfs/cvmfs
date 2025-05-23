/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_REFLOG_H_
#define CVMFS_SWISSKNIFE_REFLOG_H_

#include <string>

#include "swissknife.h"

namespace manifest {
class Reflog;
class Manifest;
}  // namespace manifest

namespace swissknife {

class CommandReconstructReflog : public Command {
 public:
  virtual std::string GetName() const { return "reconstruct_reflog"; }
  virtual std::string GetDescription() const {
    return "Bootstraps a Reference Log from Catalog and History chains. This "
           "is used for both legacy repository migration and repairs.";
  }
  virtual ParameterList GetParams() const;
  int Main(const ArgumentList &args);

 protected:
  void AddStaticManifestObjects(manifest::Reflog *reflog,
                                manifest::Manifest *manifest) const;
};

};  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_REFLOG_H_
