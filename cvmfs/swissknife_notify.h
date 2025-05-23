/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_NOTIFY_H_
#define CVMFS_SWISSKNIFE_NOTIFY_H_

#include <string>

#include "swissknife.h"

namespace swissknife {

class CommandNotify : public Command {
 public:
  virtual ~CommandNotify();

  virtual std::string GetName() const { return "notify"; }
  virtual std::string GetDescription() const {
    return "Publish/subscribe client for the CVMFS notification system";
  }

  virtual ParameterList GetParams() const;

  virtual int Main(const ArgumentList &args);
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_NOTIFY_H_
