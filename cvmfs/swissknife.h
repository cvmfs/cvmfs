/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SWISSKNIFE_H_
#define CVMFS_SWISSKNIFE_H_

#include <string>
#include <vector>
#include <map>
#include <cassert>

#include "util.h"

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
  static Parameter Mandatory(const char key, const std::string &desc) {
    return Parameter(key, desc, false, false, false);
  }
  static Parameter Optional(const char key, const std::string &desc) {
    return Parameter(key, desc, true, false, false);
  }
  static Parameter Switch(const char key, const std::string &desc) {
    return Parameter(key, desc, true, true, false);
  }
  static Parameter HelpSwitch(const char key, const std::string &desc) {
    return Parameter(key, desc, true, true, true);
  }

  char key() const { return key_; }
  const std::string& description() const { return description_; }
  bool optional() const { return optional_; }
  bool mandatory() const { return !optional_; }
  bool switch_only() const { return switch_only_; }
  bool help_switch() const { return help_switch_; }

 protected:
  Parameter(const char          key,
            const std::string  &desc,
            const bool          opt,
            const bool          switch_only,
            const bool          help_switch) :
    key_(key),
    description_(desc),
    optional_(opt),
    switch_only_(switch_only),
    help_switch_(help_switch)
  {
    assert (! switch_only_ || optional_); // switches are optional by definition
    assert (! help_switch_ || switch_only_); // help switches _are_ switches
  }

 private:
  char key_;
  std::string description_;
  bool optional_;
  bool switch_only_;
  bool help_switch_;
};

typedef std::vector<Parameter> ParameterList;
typedef std::map<char, std::string *> ArgumentList;

struct CommandIntrospection {
  CommandIntrospection(const std::string    &name,
                       const std::string    &description,
                       const ParameterList  &parameters) :
    name(name), description(description), parameters(parameters) {}

  std::string    name;
  std::string    description;
  ParameterList  parameters;
};

class AbstractCommand : public PolymorphicConstruction<AbstractCommand,
                                                       std::string,
                                                       CommandIntrospection> {
 public:
  static void RegisterPlugins();

 public:
  // this should be overridden and up-called for each Command implementation
  AbstractCommand(const std::string &param) {}
  virtual ~AbstractCommand() { }
  virtual int Main(int argc, char** argv) = 0;
  virtual int Run(const ArgumentList &args) = 0;

 protected:
  virtual ArgumentList ReadArguments(int argc, char **argv) const = 0;

  ArgumentList ParseArguments(int argc, char **argv, ParameterList &params) const;
};


// CRTP - Curiously Recurring Template Pattern for static polymorphism
template <class DerivedT>
class Command : public AbstractCommand {
 public:
  Command(const std::string &param) : AbstractCommand(param) {}

  static bool WillHandle(const std::string &param) {
    return (param == DerivedT::GetName());
  }

  static CommandIntrospection GetInfo() {
    return CommandIntrospection(DerivedT::GetName(),
                                DerivedT::GetDescription(),
                                DerivedT::GetParameters());
  }

  int Main(int argc, char **argv);

 protected:
  ArgumentList ReadArguments(int argc, char **argv) const;
};

}  // namespace swissknife

#include "swissknife_impl.h"

#endif  // CVMFS_SWISSKNIFE_H_
