/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_OPTIONS_H_
#define CVMFS_OPTIONS_H_

#include <stdint.h>
#include <map>
#include <string>
#include <vector>
#include <map>

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

struct ConfigValue {
  std::string value;
  std::string source;
};

class OptionsManager {

protected:
  bool HasConfigRepository(const std::string &fqrn, std::string *config_path);

public:
  OptionsManager() :
    config_(std::map<std::string, ConfigValue>()) {}
  virtual ~OptionsManager() {}

  virtual void ParsePath(const std::string &config_file, const bool external) = 0;
  void ParseDefault(const std::string &fqrn);
  void ClearConfig();
  bool IsDefined(const std::string &key);
  bool GetValue(const std::string &key, std::string *value);
  bool GetSource(const std::string &key, std::string *value);
  bool IsOn(const std::string &param_value);
  std::vector<std::string> GetAllKeys();
  std::string Dump();

  bool ParseUIntMap(const std::string &path, std::map<uint64_t, uint64_t> *map);

protected:
  std::map<std::string, ConfigValue> config_;

};  // class OptionManager



class FastOptionsManager : public OptionsManager {

public:
  void ParsePath(const std::string &config_file, const bool external);

};  // class FastOptionManager



class BashOptionsManager : public OptionsManager {

public:
  void ParsePath(const std::string &config_file, const bool external);

};  // class BashOptionManager



#ifdef CVMFS_NAMESPACE_GUARD
}
#endif

#endif  // CVMFS_OPTIONS_H_
