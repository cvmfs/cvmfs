/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_H_
#define CVMFS_SWISSKNIFE_H_

#include <cassert>
#include <map>
#include <string>
#include <vector>

#include "download.h"
#include "manifest_fetch.h"
#include "reflog.h"
#include "server_tool.h"
#include "signature.h"
#include "statistics.h"

namespace download {
class DownloadManager;
}
namespace manifest {
class Manifest;
}
namespace perf {
class Statistics;
}
namespace signature {
class SignatureManager;
}

namespace swissknife {

class Parameter {
 public:
  static Parameter Mandatory(const char key, const std::string &desc) {
    return Parameter(key, desc, false, false);
  }
  static Parameter Optional(const char key, const std::string &desc) {
    return Parameter(key, desc, true, false);
  }
  static Parameter Switch(const char key, const std::string &desc) {
    return Parameter(key, desc, true, true);
  }

  char key() const { return key_; }
  const std::string &description() const { return description_; }
  bool optional() const { return optional_; }
  bool mandatory() const { return !optional_; }
  bool switch_only() const { return switch_only_; }

 protected:
  Parameter(const char key, const std::string &desc, const bool opt,
            const bool switch_only)
      : key_(key),
        description_(desc),
        optional_(opt),
        switch_only_(switch_only) {
    assert(!switch_only_ || optional_);  // switches are optional by definition
  }

 private:
  char key_;
  std::string description_;
  bool optional_;
  bool switch_only_;
};

typedef std::vector<Parameter> ParameterList;
typedef std::map<char, std::string *> ArgumentList;

class Command : public ServerTool {
 public:
  Command();
  virtual ~Command();
  virtual std::string GetName() const = 0;
  virtual std::string GetDescription() const = 0;
  virtual ParameterList GetParams() const = 0;
  virtual int Main(const ArgumentList &args) = 0;
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_H_
