/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_OPTIONS_H_
#define CVMFS_OPTIONS_H_

#include <stdint.h>

#include <cassert>
#include <map>
#include <memory>
#include <string>
#include <vector>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * Templating manager used for variable replacement in the config file
 */
class OptionsTemplateManager {
 public:
  void SetTemplate(std::string name, std::string val);
  std::string GetTemplate(std::string name);
  bool HasTemplate(std::string name);
  bool ParseString(std::string *input);

 private:
  std::map<std::string, std::string> templates_;
};

class DefaultOptionsTemplateManager : public OptionsTemplateManager {
 public:
  explicit DefaultOptionsTemplateManager(std::string fqrn);

 private:
  static const char *kTemplateIdentFqrn;
  static const char *kTemplateIdentOrg;
};

/**
 * In-memory copy of a config repository's /etc/cvmfs tree, fetched from the
 * server instead of read through the mounted repository.  Paths are relative
 * to the repository root, e.g. "etc/cvmfs/default.conf".
 */
struct ConfigRepoTree {
  static const unsigned kMaxEntries = 4096;
  static const uint64_t kMaxFileSize = 1024 * 1024;
  static const uint64_t kMaxTotalSize = 16 * 1024 * 1024;

  ConfigRepoTree() : revision(0) { }

  /**
   * Resolves "." / ".." and symbolic links inside the tree.  Fails if the
   * path is absent, a directory, or escapes the repository root.
   */
  bool Lookup(const std::string &path, std::string *content) const;
  bool HasDirectory(const std::string &path) const;
  /**
   * Names of regular files and symlinks directly below dir_path, sorted.
   */
  std::vector<std::string> List(const std::string &dir_path) const;
  /**
   * Repository-relative path for an absolute path below
   * <mount_dir>/<fqrn>/, or false if the path lies outside.
   */
  bool MapMountPath(const std::string &mount_dir, const std::string &abs_path,
                    std::string *rel_path) const;

  std::string Serialize() const;
  static bool Deserialize(const std::string &buf, ConfigRepoTree *tree);

  std::string fqrn;
  uint64_t revision;
  std::string root_hash;
  std::map<std::string, std::string> files;
  std::map<std::string, std::string> symlinks;  ///< path -> raw target

 private:
  bool Resolve(const std::string &path, unsigned depth,
               std::string *resolved) const;
};


/**
 * This is the abstract base class for the different option parsers. It parses
 * and stores the information contained in different config files, keeping
 * the last parsed file that changed each property. It stores the information
 * in a key-value map so that for each variable name the value and last source
 * are stored, and makes each property part of the program's environments
 */
class OptionsManager {
 public:
  explicit OptionsManager(OptionsTemplateManager *opt_templ_mgr_param)
      : taint_environment_(true) {
    if (opt_templ_mgr_param != NULL) {
      opt_templ_mgr_ = opt_templ_mgr_param;
    } else {
      opt_templ_mgr_ = new OptionsTemplateManager();
    }
  }

  OptionsManager(const OptionsManager &opt_mgr) {
    config_ = opt_mgr.config_;
    protected_parameters_ = opt_mgr.protected_parameters_;
    templatable_values_ = opt_mgr.templatable_values_;
    taint_environment_ = opt_mgr.taint_environment_;
    config_repo_tree_ = opt_mgr.config_repo_tree_;

    opt_templ_mgr_ = new OptionsTemplateManager(*(opt_mgr.opt_templ_mgr_));
  }

  virtual ~OptionsManager() { delete opt_templ_mgr_; }

  /**
   * Switches the Options Templating Manager and reparses the set options
   */
  void SwitchTemplateManager(OptionsTemplateManager *opt_templ_mgr_param);

  /**
   * Opens the config_file and extracts all contained variables and their
   * corresponding values. The new variables are set (and overwritten in case
   * they were previously defined) as environment variables
   *
   * @param config_file  absolute path to the configuration file
   * @param external     if true it indicates the configuration file is in the
   *                     repository. If false the configuration file is in /etc
   */
  virtual void ParsePath(const std::string &config_file,
                         const bool external) = 0;

  /**
   * Like ParsePath() but for a file that is already in memory.  The shell
   * evaluates shell_text; assignments are only extracted from the lines of
   * scan_text (the file itself, without inlined includes), as for ParsePath().
   *
   * @param label  recorded as the source of the parsed parameters
   */
  virtual void ParseBuffer(const std::string &shell_text,
                           const std::string &scan_text,
                           const std::string &label) = 0;

  /**
   * Parses the default config files for cvmfs
   */
  void ParseDefault(const std::string &fqrn);
  void SetupGlobalEnvironmentParams();

  /**
   * Cleans all information about the variables
   */
  void ClearConfig();

  /**
   * Checks if a concrete key (variable) is defined
   *
   * @param   key variable to be checked in the map
   * @return  true if there is a value for key, false otherwise
   */
  bool IsDefined(const std::string &key);

  /**
   * Gets the stored value for a concrete variable
   *
   * @param  key variable to be accessed in the map
   * @param  value container of the received value, if it exists
   * @return true if there was a value stored in the map for key
   */
  bool GetValue(const std::string &key, std::string *value) const;

  /**
   * Gets the stored value for a concrete variable. Panics if the value is
   * missing.
   *
   * @param  key variable to be accessed in the map
   * @param  value container of the received value, if it exists
   */
  std::string GetValueOrDie(const std::string &key);

  /**
   * Gets the stored last source of a concrete variable
   *
   * @param  key variable to be accessed in the map
   * @param  value container of the received value, if it exists
   * @return true if that variable was previously defined in
   *         at least one source
   */
  bool GetSource(const std::string &key, std::string *value);

  /**
   * Checks if a variable contains a boolean value
   *
   * @param   param_value variable to be accessed in the map
   * @return  true if param has as value "YES", "ON" or "1". False otherwise
   */
  bool IsOn(const std::string &param_value) const;

  /**
   * Checks if a variable contains a boolean value
   *
   * @param   param_value variable to be accessed in the map
   * @return  true if param has as value "NO", "OFF", "0", or "FALSE".
   */
  bool IsOff(const std::string &param_value) const;

  /**
   * Retrieves a vector containing all stored keys
   *
   * @return a vector with all keys contained in the map
   */
  std::vector<std::string> GetAllKeys();

  /**
   * Returns key=value strings from the options array for all keys that match
   * key_prefix.  Can be used to construct an environment pointer for execve.
   */
  std::vector<std::string> GetEnvironmentSubset(const std::string &key_prefix,
                                                bool strip_prefix);

  /**
   * Gets all stored key-values of the map in an string format. This format
   * follows the following pattern:
   *
   * "KEY=VALUE    # from SOURCE"
   *
   * @return a vector containing all key-values in a string format
   */
  std::string Dump();

  bool HasConfigRepository(const std::string &fqrn, std::string *config_path);

  /**
   * If set, ParseDefault() takes the config repository files from the tree
   * instead of the mount point.  The tree is shared with copies of this
   * object and stays available for consumers of key files (MountPoint).
   */
  void set_config_repo_tree(std::shared_ptr<const ConfigRepoTree> tree) {
    config_repo_tree_ = tree;
  }
  std::shared_ptr<const ConfigRepoTree> config_repo_tree() const {
    return config_repo_tree_;
  }

  /**
   * Replaces ". <path>" / "source <path>" lines whose literal path resolves
   * inside the tree by the content of the included file.  path is the
   * repository-relative path of content.  Other lines are kept as is.
   */
  static std::string InlineIncludes(const ConfigRepoTree &tree,
                                    const std::string &mount_dir,
                                    const std::string &path,
                                    const std::string &content);

  /**
   * Similar to a bash "read-only" parameter: the current value will be locked
   * and cannot be changed anymore by succeeding parsings of config files.
   */
  void ProtectParameter(const std::string &param);

  /**
   * Artificially inject values in the option manager.
   */
  void SetValue(const std::string &key, const std::string &value);

  /**
   * Artificially inject values in the option manager, marking
     cvmfs_talk as the source.
   */
  void SetValueFromTalk(const std::string &key, const std::string &value);

  /**
   * Purge a value from the parameter map.  Used in unit tests.
   */
  void UnsetValue(const std::string &key);

  void set_taint_environment(bool value) { taint_environment_ = value; }

 protected:
  /**
   * The ConfigValue structure contains a concrete value of a variable, as well
   * as the source (complete path) of the config file where it was obtained
   */
  struct ConfigValue {
    std::string value;
    std::string source;
  };

  std::string TrimParameter(const std::string &parameter);
  std::string SanitizeParameterAssignment(std::string *line,
                                          std::vector<std::string> *tokens);
  void PopulateParameter(const std::string &param, const ConfigValue val);
  void ParseValue(const std::string param, ConfigValue *val);
  void UpdateEnvironment(const std::string &param, ConfigValue val);
  /**
   * Parses one file of the config repository, from config_repo_tree_ if set,
   * otherwise from the mount point.  rel_path is relative to etc/cvmfs/.
   */
  void ParseConfigRepoFile(const std::string &config_path,
                           const std::string &rel_path);
  std::map<std::string, ConfigValue> config_;
  std::map<std::string, std::string> protected_parameters_;
  std::map<std::string, std::string> templatable_values_;

  OptionsTemplateManager *opt_templ_mgr_;
  /**
   * Whether to add environment variables to the process' environment or not.
   * In libcvmfs, we don't want a tainted environment.
   */
  bool taint_environment_;
  std::shared_ptr<const ConfigRepoTree> config_repo_tree_;

 private:
  static bool InlineIncludesRec(const ConfigRepoTree &tree,
                                const std::string &mount_dir,
                                const std::string &path,
                                const std::string &content,
                                std::vector<std::string> *stack,
                                std::string *result);
  // NOLINTNEXTLINE(bugprone-unhandled-self-assignment,cert-oop54-cpp)
  OptionsManager &operator=(const OptionsManager & /* other */) {
    assert(false);
  }
};  // class OptionManager


/**
 * Derived class from OptionsManager. This class provides a so-called fast
 * parsing procedure which does not create a fork of the process and only
 * retrieves those options of the configuration file which strictly are in the
 * following format:
 *
 *  "KEY=VALUE".
 *
 *  No comments (#) are allowed.
 *
 *  @note In order to use this parse it is necessary to execute the program
 *        with the "-o simple_options_parsing" flag
 *  @note If using IgProf profiling tool it is necessary to use this parser in
 *        order to avoid a fork
 */
class SimpleOptionsParser : public OptionsManager {
 public:
  explicit SimpleOptionsParser(
      OptionsTemplateManager *opt_templ_mgr_param = NULL)
      : OptionsManager(opt_templ_mgr_param) { }
  virtual void ParsePath(const std::string &config_file,
                         const bool external __attribute__((unused))) {
    (void)TryParsePath(config_file);
  }
  virtual void ParseBuffer(const std::string &shell_text,
                           const std::string &scan_text,
                           const std::string &label);
  // Libcvmfs returns success or failure, the fuse module fails silently
  bool TryParsePath(const std::string &config_file);

 private:
  void ParseLines(const std::string &text, const std::string &label);
};  // class SimpleOptionsManager


/**
 * Derived class from OptionsManager. This class provides the
 * complete parsing of the configuration files. In order to parse the
 * configuration files it retrieves the "KEY=VALUE" pairs and uses bash for
 * the rest, so that you can execute slightly complex scripts
 */
class BashOptionsManager : public OptionsManager {
 public:
  explicit BashOptionsManager(
      OptionsTemplateManager *opt_templ_mgr_param = NULL)
      : OptionsManager(opt_templ_mgr_param) { }
  void ParsePath(const std::string &config_file, const bool external);
  virtual void ParseBuffer(const std::string &shell_text,
                           const std::string &scan_text,
                           const std::string &label);

 private:
  void ParseWithShell(const std::string &cwd, const std::string &shell_text,
                      const std::string &scan_text, const std::string &label);
};  // class BashOptionsManager


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_OPTIONS_H_
