/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_PULL_H_
#define CVMFS_SWISSKNIFE_PULL_H_

#include <string>

#include "swissknife.h"

namespace catalog {
class Catalog;
}

namespace shash {
class Any;
}

namespace swissknife {

class CommandPull : public Command {
 public:
  ~CommandPull() { }
  std::string GetName() { return "pull"; }
  std::string GetDescription() {
    return "Makes a Stratum 1 replica of a Stratum 0 repository.";
  }
  ParameterList GetParams() {
    ParameterList r;
    r.push_back(Parameter::Mandatory('u', "repository url"));
    r.push_back(Parameter::Mandatory('m', "repository name"));
    r.push_back(Parameter::Mandatory('r', "spooler definition"));
    r.push_back(Parameter::Mandatory('k', "repository master key(s)"));
    r.push_back(Parameter::Optional('y', "trusted certificate directories"));
    r.push_back(Parameter::Mandatory('x', "directory for temporary files"));
    r.push_back(Parameter::Optional('n', "number of download threads"));
    r.push_back(Parameter::Optional('l', "log level (0-4, default: 2)"));
    r.push_back(Parameter::Optional('t', "timeout (s)"));
    r.push_back(Parameter::Optional('a', "number of retries"));
    r.push_back(Parameter::Optional('d', "directory for path specification"));
    r.push_back(Parameter::Switch('p', "pull catalog history, too"));
    r.push_back(Parameter::Switch('c', "preload cache instead of stratum 1"));
    // Required for preloading client cache with a dirtab.  If the dirtab
    // changes, the existence of a catalog does not anymore indicate if
    // everything in the corresponding subtree is already fetched, too.
    r.push_back(
      Parameter::Switch('z', "look into all catalogs even if already present"));
    return r;
  }
  int Main(const ArgumentList &args);

 protected:
  bool PullRecursion(catalog::Catalog *catalog, const std::string &path);
  bool Pull(const shash::Any &catalog_hash, const std::string &path);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_PULL_H_
