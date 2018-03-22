/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_TARBALL_H_
#define CVMFS_SWISSKNIFE_TARBALL_H_

#include <string>

#include "swissknife.h"
#include "swissknife_sync.h"

namespace swissknife {
class IngestTarball : public Command {
 public:
  ~IngestTarball() {}
  virtual std::string GetName() const { return "ingest_tarball"; }
  virtual std::string GetDescription() const {
    return "Pushes the content of the tarball to the repository";
  }
  virtual ParameterList GetParams() const {
    ParameterList r;
    r.push_back(Parameter::Mandatory('b', "base hash"));
    r.push_back(Parameter::Mandatory('c', "r/o volume"));
    r.push_back(Parameter::Mandatory('o', "manifest output file"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    r.push_back(Parameter::Mandatory('s', "scratch directory"));
    r.push_back(Parameter::Mandatory('t', "directory for tee"));
    r.push_back(Parameter::Mandatory('u', "union volume"));
    r.push_back(Parameter::Mandatory('w', "stratum 0 base url"));
    r.push_back(Parameter::Mandatory('K', "public key(s) for repo"));
    r.push_back(Parameter::Mandatory('N', "fully qualified repository name"));

    r.push_back(Parameter::Mandatory('T', "tar file to extract"));
    r.push_back(Parameter::Mandatory(
        'B', "base directory where to extract the tarfile"));
    r.push_back(Parameter::Optional(
        'D', "entity to delete before to extract the tar"));

    return r;
  }
  int Main(const ArgumentList &args);
};
}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_TARBALL_H_
