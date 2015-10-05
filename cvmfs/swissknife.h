/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_H_
#define CVMFS_SWISSKNIFE_H_

#include <cassert>
#include <map>
#include <string>
#include <vector>

namespace download {
class DownloadManager;
}
namespace signature {
class SignatureManager;
}
namespace perf {
class Statistics;
}

namespace swissknife {

extern download::DownloadManager *g_download_manager;
extern signature::SignatureManager *g_signature_manager;
extern perf::Statistics *g_statistics;

void Usage();

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
  const std::string& description() const { return description_; }
  bool optional() const { return optional_; }
  bool mandatory() const { return !optional_; }
  bool switch_only() const { return switch_only_; }

 protected:
  Parameter(const char          key,
            const std::string  &desc,
            const bool          opt,
            const bool          switch_only) :
    key_(key),
    description_(desc),
    optional_(opt),
    switch_only_(switch_only)
  {
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

class Command {
 public:
  Command() { }
  virtual ~Command() { }
  virtual std::string GetName() = 0;
  virtual std::string GetDescription() = 0;
  virtual ParameterList GetParams() = 0;
  virtual int Main(const ArgumentList &args) = 0;
};

}  // namespace swissknife

#endif  // CVMFS_SWISSKNIFE_H_
