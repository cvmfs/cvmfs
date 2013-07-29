/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_H_
#define CVMFS_SWISSKNIFE_H_

#include <string>
#include <vector>
#include <map>

namespace download {
class DownloadManager;
}
namespace signature {
class SignatureManager;
}

namespace swissknife {

extern download::DownloadManager *g_download_manager;
extern signature::SignatureManager *g_signature_manager;

void Usage();

class Parameter {
 public:
  Parameter(const char key, const std::string &desc, const bool opt,
            const bool switch_only)
  {
    key_ = key;
    description_ = desc;
    optional_ = opt;
    switch_only_ = switch_only;
  }

  char key() const { return key_; }
  std::string description() const { return description_; }
  bool optional() const { return optional_; }
  bool switch_only() const { return switch_only_; }
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
