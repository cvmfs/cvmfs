/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_LEASE_H_
#define CVMFS_SWISSKNIFE_LEASE_H_

#include <string>

#include "swissknife.h"

namespace swissknife {

class CommandLease : public Command {
 public:
  virtual ~CommandLease();

  virtual std::string GetName() const { return "lease"; }
  virtual std::string GetDescription() const {
    return "Acquire a lease on a repository sub-path";
  }

  virtual ParameterList GetParams() const;

  virtual int Main(const ArgumentList &args);

  struct Parameters {
    Parameters()
        : repo_service_url(""), action(""), key_file(""), lease_path("") { }

    std::string repo_service_url;
    std::string action;
    std::string key_file;
    std::string lease_path;
  };
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_LEASE_H_
