/**
 * This file is part of the CernVM File System.
 *
 * Fills an internal map of key-value pairs from ASCII files in key=value
 * style.  Parameters can be overwritten.  Used to read configuration from
 * /etc/cvmfs/...
 */

#include "cvmfs_config.h"
#include "options.h"

#include <unistd.h>

#include <cstdio>
#include <cassert>
#include <cstdlib>

#include <map>

#include "util.h"

using namespace std;  // NOLINT

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

namespace options {

struct ConfigValue {
  string value;
  string source;
};

map<string, ConfigValue> *config_ = NULL;


void Init() {
  config_ = new map<string, ConfigValue>();
}


void Fini() {
  delete config_;
  config_ = NULL;
}


static string EscapeShell(const std::string &raw) {
  for (unsigned i = 0, l = raw.length(); i < l; ++i) {
    if (not (((raw[i] >= '0') && (raw[i] <= '9')) ||
             ((raw[i] >= 'A') && (raw[i] <= 'Z')) ||
             ((raw[i] >= 'a') && (raw[i] <= 'z')) ||
             (raw[i] == '/') || (raw[i] == ':') || (raw[i] == '.') ||
             (raw[i] == '_') || (raw[i] == '-') || (raw[i] == ',')))
    {
      goto escape_shell_quote;
    }
  }
  return raw;

 escape_shell_quote:
  string result = "'";
  for (unsigned i = 0, l = raw.length(); i < l; ++i) {
    if (raw[i] == '\'')
      result += "\\";
    result += raw[i];
  }
  result += "'";
  return result;
}


void ParsePath(const string &config_file) {
  FILE *fconfig = fopen(config_file.c_str(), "r");
  if (!fconfig)
    return;

  int retval;

  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  retval = Shell(&fd_stdin, &fd_stdout, &fd_stderr);
  assert(retval);

  // Let the shell read the file
  string line;
  const string newline = "\n";
  while (GetLineFile(fconfig, &line)) {
    WritePipe(fd_stdin, line.data(), line.length());
    WritePipe(fd_stdin, newline.data(), newline.length());
  }
  rewind(fconfig);

  // Read line by line and extract parameters
  while (GetLineFile(fconfig, &line)) {
    line = Trim(line);
    if (line.empty() || line[0] == '#' || line.find("if ") == 0)
      continue;
    vector<string> tokens = SplitString(line, '=');
    if (tokens.size() < 2)
      continue;

    ConfigValue value;
    value.source = config_file;
    string parameter = tokens[0];
    // Strip "readonly"
    if (parameter.find("readonly") == 0) {
      parameter = parameter.substr(8);
      parameter = Trim(parameter);
    }
    // Strip export
    if (parameter.find("export") == 0) {
      parameter = parameter.substr(6);
      parameter = Trim(parameter);
    }
    // Strip eval
    if (parameter.find("eval") == 0) {
      parameter = parameter.substr(4);
      parameter = Trim(parameter);
    }

    const string sh_echo = "echo $" + parameter + "\n";
    WritePipe(fd_stdin, sh_echo.data(), sh_echo.length());
    GetLineFd(fd_stdout, &value.value);
    (*config_)[parameter] = value;
    retval = setenv(parameter.c_str(), value.value.c_str(), 1);
    assert(retval == 0);
  }

  close(fd_stderr);
  close(fd_stdout);
  close(fd_stdin);
  fclose(fconfig);
}


void ParseDefault(const string &repository_name) {
  ParsePath("/etc/cvmfs/default.conf");
  vector<string> dist_defaults = FindFiles("/etc/cvmfs/default.d", ".conf");
  for (unsigned i = 0; i < dist_defaults.size(); ++i) {
    ParsePath(dist_defaults[i]);
  }
  ParsePath("/etc/cernvm/default.conf");
  ParsePath("/etc/cvmfs/site.conf");
  ParsePath("/etc/cernvm/site.conf");
  ParsePath("/etc/cvmfs/default.local");

  if (repository_name != "") {
    string domain;
    vector<string> tokens = SplitString(repository_name, '.');
    if (tokens.size() > 1) {
      tokens.erase(tokens.begin());
      domain = JoinStrings(tokens, ".");
    } else {
      GetValue("CVMFS_DEFAULT_DOMAIN", &domain);
    }
    ParsePath("/etc/cvmfs/domain.d/" + domain + ".conf");
    ParsePath("/etc/cvmfs/domain.d/" + domain + ".local");

    ParsePath("/etc/cvmfs/config.d/" + repository_name + ".conf");
    ParsePath("/etc/cvmfs/config.d/" + repository_name + ".local");
  }
}


void ClearConfig() {
  config_->clear();
}


bool IsDefined(const std::string &key) {
  map<string, ConfigValue>::const_iterator iter = config_->find(key);
  return iter != config_->end();
}


bool GetValue(const string &key, string *value) {
  map<string, ConfigValue>::const_iterator iter = config_->find(key);
  if (iter != config_->end()) {
    *value = iter->second.value;
    return true;
  }
  *value = "";
  return false;
}


bool GetSource(const string &key, string *value) {
  map<string, ConfigValue>::const_iterator iter = config_->find(key);
  if (iter != config_->end()) {
    *value = iter->second.source;
    return true;
  }
  *value = "";
  return false;
}


bool IsOn(const std::string &param_value) {
  const string uppercase = ToUpper(param_value);
  return ((uppercase == "YES") || (uppercase == "ON") || (uppercase == "1"));
}


vector<string> GetAllKeys() {
  vector<string> result;
  for (map<string, ConfigValue>::const_iterator i = config_->begin(),
       iEnd = config_->end(); i != iEnd; ++i)
  {
    result.push_back(i->first);
  }
  return result;
}


string Dump() {
  string result;
  vector<string> keys = GetAllKeys();
  for (unsigned i = 0, l = keys.size(); i < l; ++i) {
    bool retval;
    string value;
    string source;

    retval = GetValue(keys[i], &value);
    assert(retval);
    retval = GetSource(keys[i], &source);
    assert(retval);
    result += keys[i] + "=" + EscapeShell(value) +
              "    # from " + source + "\n";
  }
  return result;
}


bool ParseUIntMap(const string &path, map<uint64_t, uint64_t> *map) {
  assert(map);

  FILE *fmap = fopen(path.c_str(), "r");
  if (!fmap)
    return false;

  string line;
  while (GetLineFile(fmap, &line)) {
    line = Trim(line);
    if (line.empty() || line[0] == '#') {
      continue;
    }
    vector<string> components = SplitString(line, ' ');
    if (components.size() != 2) {
      fclose(fmap);
      return false;
    }
    uint64_t from = String2Uint64(components[0]);
    uint64_t to = String2Uint64(components[1]);
    map->insert(pair<uint64_t, uint64_t>(from, to));
  }
  fclose(fmap);
  return true;
}

}  // namespace options

#ifdef CVMFS_NAMESPACE_GUARD
}
#endif
