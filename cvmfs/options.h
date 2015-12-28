/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_OPTIONS_H_
#define CVMFS_OPTIONS_H_

#include <stdint.h>

#include <map>
#include <string>
#include <vector>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * This is the abstract base class for the different option parsers. It parses
 * and stores the information contained in different config files, keeping
 * the last parsed file that changed each property. It stores the information
 * in a key-value map so that for each variable name the value and last source
 * are stored, and makes each property part of the program's environments
 */
class OptionsManager {
 public:
  OptionsManager() {}
  virtual ~OptionsManager() {}

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
   * Parses the default config files for cvmfs
   */
  void ParseDefault(const std::string &fqrn);

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
  bool GetValue(const std::string &key, std::string *value);

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
  bool IsOn(const std::string &param_value);

  /**
   * Retrieves a vector containing all stored keys
   *
   * @return a vector with all keys contained in the map
   */
  std::vector<std::string> GetAllKeys();

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

 protected:
  /**
    * The ConfigValue structure contains a concrete value of a variable, as well
    * as the source (complete path) of the config file where it was obtained
    */
  struct ConfigValue {
    std::string value;
    std::string source;
  };

  std::map<std::string, ConfigValue> config_;
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
  void ParsePath(const std::string &config_file, const bool external);
};  // class SimpleOptionsManager


/**
 * Derived class from OptionsManager. This class provides the
 * complete parsing of the configuration files. In order to parse the
 * configuration files it retrieves the "KEY=VALUE" pairs and uses bash for
 * the rest, so that you can execute sightly complex scripts
 */
class BashOptionsManager : public OptionsManager {
 public:
  void ParsePath(const std::string &config_file, const bool external);
};  // class BashOptionsManager



#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_OPTIONS_H_
