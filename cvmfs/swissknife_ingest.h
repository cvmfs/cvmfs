/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_INGEST_H_
#define CVMFS_SWISSKNIFE_INGEST_H_

#include <string>

#include "swissknife.h"
#include "swissknife_sync.h"

namespace swissknife {
class Ingest : public Command {
 public:
  ~Ingest() { }
  virtual std::string GetName() const { return "ingest"; }
  virtual std::string GetDescription() const {
    return "Pushes the content of the tarball to the repository";
  }
  virtual ParameterList GetParams() const {
    ParameterList r;
    r.push_back(Parameter::Mandatory('b', "base hash"));
    r.push_back(Parameter::Mandatory('c', "r/o volume"));
    r.push_back(Parameter::Mandatory('o', "manifest output file"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    r.push_back(Parameter::Mandatory('t', "directory for tee"));
    r.push_back(Parameter::Mandatory('u', "union volume"));
    r.push_back(Parameter::Mandatory('w', "stratum 0 base url"));
    r.push_back(Parameter::Mandatory('K', "public key(s) for repo"));
    r.push_back(Parameter::Mandatory('N', "fully qualified repository name"));

    r.push_back(Parameter::Optional('T', "tar file to extract"));
    r.push_back(Parameter::Optional(
        'B', "base directory where to extract the tarfile"));
    r.push_back(
        Parameter::Optional('D', "entity to delete before to extract the tar"));
    r.push_back(Parameter::Optional(
        'C', "create a new catalog where the tar file is extracted"));

    r.push_back(Parameter::Optional('P', "session_token_file"));
    r.push_back(Parameter::Optional('H', "key file for HTTP API"));
    r.push_back(Parameter::Optional('@', "proxy url"));
    r.push_back(Parameter::Switch('I', "upload updated statistics DB file"));
    r.push_back(Parameter::Optional(
        'U',
        "uid of new owner of the ingested data (-1 for keep tarball owner)"));
    r.push_back(Parameter::Optional(
        'G',
        "gid of new owner of the ingested data (-1 for keep tarball owner)"));
    r.push_back(Parameter::Switch('j', "enable nanosecond timestamps"));

    return r;
  }
  int Main(const ArgumentList &args);
};
}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_INGEST_H_
