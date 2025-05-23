/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PUBLISH_COMMAND_H_
#define CVMFS_PUBLISH_COMMAND_H_

#include <stdint.h>

#include <cassert>
#include <map>
#include <string>
#include <vector>

#include "util/single_copy.h"
#include "util/string.h"

namespace publish {

class Command {
  friend class CmdHelp;  // to set the progname_

 public:
  /**
   * A parameter is information that can be passed by -$short_key or --$key to
   * a command.  Parameters can be boolean switches (they have no arguments).
   * If they have an argument, parameters can be required for the command or
   * optional.
   */
  struct Parameter {
    /**
     * This constructor should only be used for objects that are meant for
     * comparison with / search of existing parameters.
     */
    explicit Parameter(const std::string &key)
        : key(key)
        , short_key('?')
        , arg_name("")
        , description("")
        , is_switch(false)
        , is_optional(false) { }

    Parameter(const std::string &key,
              char short_key,
              const std::string &arg_name,
              const std::string &desc,
              bool is_switch,
              bool is_optional)
        : key(key)
        , short_key(short_key)
        , arg_name(arg_name)
        , description(desc)
        , is_switch(is_switch)
        , is_optional(is_optional) {
      assert(!key.empty());
      assert(!is_switch || is_optional);  // switches are always optional
      assert(is_optional || !arg_name.empty());
    }

    bool operator==(const Parameter &other) const { return key == other.key; }
    bool operator!=(const Parameter &other) const { return key != other.key; }
    bool operator<(const Parameter &other) const { return key < other.key; }

    static Parameter Mandatory(const std::string &key, char short_key,
                               const std::string &arg_name,
                               const std::string &desc) {
      return Parameter(key, short_key, arg_name, desc, false, false);
    }
    static Parameter Optional(const std::string &key, char short_key,
                              const std::string &arg_name,
                              const std::string &desc) {
      return Parameter(key, short_key, arg_name, desc, false, true);
    }
    static Parameter Switch(const std::string &key, char short_key,
                            const std::string &desc) {
      return Parameter(key, short_key, "", desc, true, true);
    }

    std::string key;
    char short_key;
    std::string arg_name;
    std::string description;
    bool is_switch;
    bool is_optional;
  };
  typedef std::vector<Parameter> ParameterList;

  /**
   * Encapsulates the value of parameters that are not switches. The conversion
   * to an int is done unconditionally, which doesn't hurt even for string
   * arguments.
   */
  struct Argument {
    Argument() : value_int(0) { }
    explicit Argument(const std::string &v)
        : value_str(v), value_int(String2Int64(v)) { }
    std::string value_str;
    int64_t value_int;
  };

  /**
   * Encapsulates parameter-argument pairs
   */
  class Options {
   public:
    void AppendPlain(const Argument &a) { plain_args_.push_back(a); }
    void Set(const Parameter &p, const Argument &a) { map_[p] = a; }
    bool Has(const std::string &key) const {
      return map_.count(Parameter(key)) > 0;
    }
    bool HasNot(const std::string &key) const { return !Has(key); }
    Argument Get(const std::string &key) const {
      return map_.find(Parameter(key))->second;
    }
    std::string GetString(const std::string &key) const {
      return map_.find(Parameter(key))->second.value_str;
    }
    std::string GetStringDefault(const std::string &key,
                                 const std::string &default_value) const {
      if (Has(key))
        return map_.find(Parameter(key))->second.value_str;
      return default_value;
    }
    int GetInt(const std::string &key) const {
      return map_.find(Parameter(key))->second.value_int;
    }
    unsigned GetSize() const { return map_.size(); }
    const std::vector<Argument> &plain_args() const { return plain_args_; }

   private:
    std::map<Parameter, Argument> map_;
    std::vector<Argument> plain_args_;
  };

  virtual ~Command() { }

  /**
   * Used to call the command as `cvmfs_server $GetName() ...`
   */
  virtual std::string GetName() const = 0;
  /**
   * A one-line explanation of what the command does
   */
  virtual std::string GetBrief() const = 0;
  /**
   * An extended description of the functionality
   */
  virtual std::string GetDescription() const { return GetBrief(); }
  /**
   * Returns the ordered array of possible parameters to this command
   */
  virtual ParameterList GetParams() const = 0;
  virtual std::string GetUsage() const { return "[options]"; }
  std::string GetExamples() const;
  /**
   * The command needs at least so many non-parameter arguments (e.g. fqrn)
   */
  virtual unsigned GetMinPlainArgs() const { return 0; }
  /**
   * Internal commands can be added that will be omitted from the printed list
   * of available commands.  By default, commands are visible though.
   */
  virtual bool IsHidden() const { return false; }

  Options ParseOptions(int argc, char **argv);

  /**
   * The dictionary of passed arguments can be obtained by a call to
   * ParseOptions()
   */
  virtual int Main(const Options &options) = 0;

  std::string progname() const { return progname_; }

 protected:
  /**
   * Example one-liners which get prepended by the command invocation
   */
  virtual std::vector<std::string> DoGetExamples() const {
    return std::vector<std::string>();
  }

 private:
  std::string progname_;
};


class CommandList : SingleCopy {
 public:
  ~CommandList();
  void TakeCommand(Command *command);
  Command *Find(const std::string &name);

  const std::vector<Command *> &commands() const { return commands_; }

 private:
  std::vector<Command *> commands_;
};

}  // namespace publish

#endif  // CVMFS_PUBLISH_COMMAND_H_
