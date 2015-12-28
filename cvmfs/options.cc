/**
 * This file is part of the CernVM File System.
 *
 * Fills an internal map of key-value pairs from ASCII files in key=value
 * style.  Parameters can be overwritten.  Used to read configuration from
 * /etc/cvmfs/...
 */

#include "cvmfs_config.h"
#include "options.h"

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <utility>

#include "logging.h"
#include "sanitizer.h"
#include "util.h"

using namespace std;  // NOLINT

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif


static string EscapeShell(const std::string &raw) {
  for (unsigned i = 0, l = raw.length(); i < l; ++i) {
    if (!(((raw[i] >= '0') && (raw[i] <= '9')) ||
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


void SimpleOptionsParser::ParsePath(const string &config_file,
                                 const bool external __attribute__((unused))) {
  LogCvmfs(kLogCvmfs, kLogDebug, "Fast-parsing config file %s",
      config_file.c_str());
  int retval;
  string line;
  FILE *fconfig = fopen(config_file.c_str(), "r");
  if (fconfig == NULL)
    return;

  // Read line by line and extract parameters
  while (GetLineFile(fconfig, &line)) {
    line = Trim(line);
    if (line.empty() || line[0] == '#' || line.find(" ") < string::npos)
      continue;
    vector<string> tokens = SplitString(line, '=');
    if (tokens.size() < 2 || tokens.size() > 2)
      continue;

    ConfigValue value;
    value.source = config_file;
    value.value = tokens[1];
    string parameter = tokens[0];
    config_[parameter] = value;
    retval = setenv(parameter.c_str(), value.value.c_str(), 1);
    assert(retval == 0);
  }
  fclose(fconfig);
}


void BashOptionsManager::ParsePath(const string &config_file,
                                   const bool external) {
  LogCvmfs(kLogCvmfs, kLogDebug, "Parsing config file %s", config_file.c_str());
  int retval;
  int pipe_open[2];
  int pipe_quit[2];
  pid_t pid_child = 0;
  if (external) {
    // cvmfs can run in the process group of automount in which case
    // autofs won't mount an additional config repository.  We create a
    // short-lived process that detaches from the process group and triggers
    // autofs to mount the config repository, if necessary.  It holds a file
    // handle to the config file until the main process opened the file, too.
    MakePipe(pipe_open);
    MakePipe(pipe_quit);
    switch (pid_child = fork()) {
      case -1:
        abort();
      case 0: {  // Child
        close(pipe_open[0]);
        close(pipe_quit[1]);
        // If this is not a process group leader, create a new session
        if (getpgrp() != getpid()) {
          pid_t new_session = setsid();
          assert(new_session != (pid_t)-1);
        }
        (void)open(config_file.c_str(), O_RDONLY);
        char ready = 'R';
        WritePipe(pipe_open[1], &ready, 1);
        read(pipe_quit[0], &ready, 1);
        _exit(0);  // Don't flush shared file descriptors
      }
    }
    // Parent
    close(pipe_open[1]);
    close(pipe_quit[0]);
    char ready = 0;
    ReadPipe(pipe_open[0], &ready, 1);
    assert(ready == 'R');
    close(pipe_open[0]);
  }
  const string config_path = GetParentPath(config_file);
  FILE *fconfig = fopen(config_file.c_str(), "r");
  if (pid_child > 0) {
    char c = 'C';
    WritePipe(pipe_quit[1], &c, 1);
    int statloc;
    waitpid(pid_child, &statloc, 0);
    close(pipe_quit[1]);
  }
  if (!fconfig) {
    if (external && !DirectoryExists(config_path)) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogWarn,
               "external location for configuration files does not exist: %s",
               config_path.c_str());
    }
    return;
  }

  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  retval = Shell(&fd_stdin, &fd_stdout, &fd_stderr);
  assert(retval);

  // Let the shell read the file
  string line;
  const string newline = "\n";
  const string cd = "cd \"" + ((config_path == "") ? "/" : config_path) + "\"" +
                    newline;
  WritePipe(fd_stdin, cd.data(), cd.length());
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
    config_[parameter] = value;
    retval = setenv(parameter.c_str(), value.value.c_str(), 1);
    assert(retval == 0);
  }

  close(fd_stderr);
  close(fd_stdout);
  close(fd_stdin);
  fclose(fconfig);
}


bool OptionsManager::HasConfigRepository(const string &fqrn,
                                         string *config_path) {
  string cvmfs_mount_dir;
  if (!GetValue("CVMFS_MOUNT_DIR", &cvmfs_mount_dir)) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr, "CVMFS_MOUNT_DIR missing");
    return false;
  }

  string config_repository;
  if (GetValue("CVMFS_CONFIG_REPOSITORY", &config_repository)) {
    if (config_repository.empty() || (config_repository == fqrn))
      return false;
    sanitizer::RepositorySanitizer repository_sanitizer;
    if (!repository_sanitizer.IsValid(config_repository)) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
               "invalid CVMFS_CONFIG_REPOSITORY: %s",
               config_repository.c_str());
      return false;
    }
    *config_path = cvmfs_mount_dir + "/" + config_repository + "/etc/cvmfs/";
    return true;
  }
  return false;
}


void OptionsManager::ParseDefault(const string &fqrn) {
  int retval = setenv("CVMFS_FQRN", fqrn.c_str(), 1);
  assert(retval == 0);

  ParsePath("/etc/cvmfs/default.conf", false);
  vector<string> dist_defaults = FindFiles("/etc/cvmfs/default.d", ".conf");
  for (unsigned i = 0; i < dist_defaults.size(); ++i) {
    ParsePath(dist_defaults[i], false);
  }
  ParsePath("/etc/cvmfs/default.local", false);

  if (fqrn != "") {
    string domain;
    vector<string> tokens = SplitString(fqrn, '.');
    assert(tokens.size() > 1);
    tokens.erase(tokens.begin());
    domain = JoinStrings(tokens, ".");

    string external_config_path;
    if (HasConfigRepository(fqrn, &external_config_path))
      ParsePath(external_config_path + "domain.d/" + domain + ".conf", true);
    ParsePath("/etc/cvmfs/domain.d/" + domain + ".conf", false);
    ParsePath("/etc/cvmfs/domain.d/" + domain + ".local", false);

    if (HasConfigRepository(fqrn, &external_config_path))
      ParsePath(external_config_path + "config.d/" + fqrn + ".conf", true);
    ParsePath("/etc/cvmfs/config.d/" + fqrn + ".conf", false);
    ParsePath("/etc/cvmfs/config.d/" + fqrn + ".local", false);
  }
}


void OptionsManager::ClearConfig() {
  config_.clear();
}


bool OptionsManager::IsDefined(const std::string &key) {
  map<string, ConfigValue>::const_iterator iter = config_.find(key);
  return iter != config_.end();
}


bool OptionsManager::GetValue(const string &key, string *value) {
  map<string, ConfigValue>::const_iterator iter = config_.find(key);
  if (iter != config_.end()) {
    *value = iter->second.value;
    return true;
  }
  *value = "";
  return false;
}


bool OptionsManager::GetSource(const string &key, string *value) {
  map<string, ConfigValue>::const_iterator iter = config_.find(key);
  if (iter != config_.end()) {
    *value = iter->second.source;
    return true;
  }
  *value = "";
  return false;
}


bool OptionsManager::IsOn(const std::string &param_value) {
  const string uppercase = ToUpper(param_value);
  return ((uppercase == "YES") || (uppercase == "ON") || (uppercase == "1"));
}


vector<string> OptionsManager::GetAllKeys() {
  vector<string> result;
  for (map<string, ConfigValue>::const_iterator i = config_.begin(),
       iEnd = config_.end(); i != iEnd; ++i)
  {
    result.push_back(i->first);
  }
  return result;
}


string OptionsManager::Dump() {
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

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
