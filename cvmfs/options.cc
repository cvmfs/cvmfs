/**
 * This file is part of the CernVM File System.
 *
 * Fills an internal map of key-value pairs from ASCII files in key=value
 * style.  Parameters can be overwritten.  Used to read configuration from
 * /etc/cvmfs/...
 */


#include "options.h"

#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <utility>

#include "sanitizer.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

using namespace std;  // NOLINT

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif


static string EscapeShell(const std::string &raw) {
  for (unsigned i = 0, l = raw.length(); i < l; ++i) {
    if (!(((raw[i] >= '0') && (raw[i] <= '9'))
          || ((raw[i] >= 'A') && (raw[i] <= 'Z'))
          || ((raw[i] >= 'a') && (raw[i] <= 'z')) || (raw[i] == '/')
          || (raw[i] == ':') || (raw[i] == '.') || (raw[i] == '_')
          || (raw[i] == '-') || (raw[i] == ','))) {
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


string OptionsManager::TrimParameter(const string &parameter) {
  string result = Trim(parameter);
  // Strip "readonly"
  if (result.find("readonly ") == 0) {
    result = result.substr(9);
    result = Trim(result);
  } else if (result.find("export ") == 0) {
    result = result.substr(7);
    result = Trim(result);
    // This enables a hack to ignore a variable setting in cvmfs versions
    // prior to 2.14.2 which also stopped setting variables to blank when
    // they are defined only in false conditionals.  The hack is to use
    // 'export X VAR=VAL', where VAR will be recognized in 2.14.2 and later
    // and ignored prior to 2.14.2.
    const size_t last_space = result.find_last_of(" ");
    if (last_space != string::npos) {
      result = result.substr(last_space + 1);
    }
  } else if (result.find("eval ") == 0) {
    result = result.substr(5);
    result = Trim(result);
  }
  return result;
}

string OptionsManager::SanitizeParameterAssignment(string *line,
                                                   vector<string> *tokens) {
  const size_t comment_idx = line->find("#");
  if (comment_idx != string::npos)
    *line = line->substr(0, comment_idx);
  *line = Trim(*line);
  if (line->empty())
    return "";
  *tokens = SplitString(*line, '=');
  if (tokens->size() < 2)
    return "";
  string parameter = TrimParameter((*tokens)[0]);
  if (parameter.find(" ") != string::npos)
    return "";
  return parameter;
}

void OptionsManager::SwitchTemplateManager(
    OptionsTemplateManager *opt_templ_mgr_param) {
  delete opt_templ_mgr_;
  if (opt_templ_mgr_param != NULL) {
    opt_templ_mgr_ = opt_templ_mgr_param;
  } else {
    opt_templ_mgr_ = new OptionsTemplateManager();
  }
  for (std::map<std::string, std::string>::iterator it =
           templatable_values_.begin();
       it != templatable_values_.end();
       it++) {
    config_[it->first].value = it->second;
    opt_templ_mgr_->ParseString(&(config_[it->first].value));
    UpdateEnvironment(it->first, config_[it->first]);
  }
}

static string ReadLines(FILE *f) {
  string text;
  string line;
  bool first = true;
  while (GetLineFile(f, &line)) {
    if (!first)
      text.push_back('\n');
    text += line;
    first = false;
  }
  return text;
}

bool SimpleOptionsParser::TryParsePath(const string &config_file) {
  LogCvmfs(kLogCvmfs, kLogDebug, "Fast-parsing config file %s",
           config_file.c_str());
  FILE *fconfig = fopen(config_file.c_str(), "r");
  if (fconfig == NULL)
    return false;
  const string text = ReadLines(fconfig);
  fclose(fconfig);
  ParseLines(text, config_file);
  return true;
}

void SimpleOptionsParser::ParseBuffer(const string & /* shell_text */,
                                      const string &scan_text,
                                      const string &label) {
  LogCvmfs(kLogCvmfs, kLogDebug, "Fast-parsing config buffer %s",
           label.c_str());
  ParseLines(scan_text, label);
}

void SimpleOptionsParser::ParseLines(const string &text, const string &label) {
  const vector<string> lines = SplitString(text, '\n');
  for (unsigned i = 0; i < lines.size(); ++i) {
    string line = lines[i];
    vector<string> tokens;
    const string parameter = SanitizeParameterAssignment(&line, &tokens);
    if (parameter.empty())
      continue;

    // Strip quotes from value
    tokens.erase(tokens.begin());
    string value = Trim(JoinStrings(tokens, "="));
    const unsigned value_length = value.length();
    if (value_length > 2) {
      if (((value[0] == '"') && ((value[value_length - 1] == '"')))
          || ((value[0] == '\'') && ((value[value_length - 1] == '\'')))) {
        value = value.substr(1, value_length - 2);
      }
    }

    ConfigValue config_value;
    config_value.source = label;
    config_value.value = value;
    PopulateParameter(parameter, config_value);
  }
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
        PANIC(NULL);
      case 0: {  // Child
        close(pipe_open[0]);
        close(pipe_quit[1]);
        // If this is not a process group leader, create a new session
        if (getpgrp() != getpid()) {
          const pid_t new_session = setsid();
          assert(new_session != (pid_t)-1);
        }
        (void)open(config_file.c_str(), O_RDONLY);
        char ready = 'R';
        WritePipe(pipe_open[1], &ready, 1);
        retval = read(pipe_quit[0], &ready, 1);
        _exit(retval);  // Don't flush shared file descriptors
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
      string repo_required;
      if (GetValue("CVMFS_CONFIG_REPO_REQUIRED", &repo_required)
          && IsOn(repo_required)) {
        LogCvmfs(
            kLogCvmfs, kLogStderr | kLogSyslogErr,
            "required configuration repository directory does not exist: %s",
            config_path.c_str());
        // Do not crash as in abort(), which can trigger core file creation
        // from the mount helper
        exit(1);
      }

      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogWarn,
               "configuration repository directory does not exist: %s",
               config_path.c_str());
    }
    return;
  }

  const string text = ReadLines(fconfig);
  fclose(fconfig);
  ParseWithShell(config_path, text, text, config_file);
}


void BashOptionsManager::ParseBuffer(const string &shell_text,
                                     const string &scan_text,
                                     const string &label) {
  LogCvmfs(kLogCvmfs, kLogDebug, "Parsing config buffer %s", label.c_str());
  // No local directory to cd into; the caller inlines relative includes
  ParseWithShell("", shell_text, scan_text, label);
}


void BashOptionsManager::ParseWithShell(const string &cwd,
                                        const string &shell_text,
                                        const string &scan_text,
                                        const string &label) {
  int fd_stdin;
  int fd_stdout;
  int fd_stderr;
  const int retval = Shell(&fd_stdin, &fd_stdout, &fd_stderr);
  assert(retval);

  // Let the shell read the file
  const string newline = "\n";
  const string cd = "cd \"" + ((cwd == "") ? "/" : cwd) + "\"" + newline;
  WritePipe(fd_stdin, cd.data(), cd.length());
  const vector<string> shell_lines = SplitString(shell_text, '\n');
  for (unsigned i = 0; i < shell_lines.size(); ++i) {
    WritePipe(fd_stdin, shell_lines[i].data(), shell_lines[i].length());
    WritePipe(fd_stdin, newline.data(), newline.length());
  }

  // Read line by line and extract parameters
  const vector<string> scan_lines = SplitString(scan_text, '\n');
  for (unsigned i = 0; i < scan_lines.size(); ++i) {
    string line = scan_lines[i];
    vector<string> tokens;
    const string parameter = SanitizeParameterAssignment(&line, &tokens);
    if (parameter.empty())
      continue;

    ConfigValue value;
    value.source = label;
    const string sh_echo = "echo ${" + parameter + "-xNOTxSETx}\n";
    WritePipe(fd_stdin, sh_echo.data(), sh_echo.length());
    GetLineFd(fd_stdout, &value.value);
    if (value.value != "xNOTxSETx") {
      PopulateParameter(parameter, value);
    }
  }

  close(fd_stderr);
  close(fd_stdout);
  close(fd_stdin);
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
    const sanitizer::RepositorySanitizer repository_sanitizer;
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

void OptionsManager::SetupGlobalEnvironmentParams() {
  SetValue("CVMFS_ARCH", GetArch());

  // Set CVMFS_VERSION environment variable
  SetValue("CVMFS_VERSION", CVMFS_VERSION);

  // Calculate and set CVMFS_VERSION_NUMERIC
  // Format: major * 10000 + minor * 100 + patch
  // Example: 2.13.2 becomes 21302
  const int version_numeric = CVMFS_VERSION_MAJOR * 10000
                              + CVMFS_VERSION_MINOR * 100 + CVMFS_VERSION_PATCH;
  char version_numeric_str[16];
  snprintf(version_numeric_str, sizeof(version_numeric_str), "%d",
           version_numeric);
  SetValue("CVMFS_VERSION_NUMERIC", version_numeric_str);
}

void OptionsManager::ParseDefault(const string &fqrn) {
  if (taint_environment_) {
    const int retval = setenv("CVMFS_FQRN", fqrn.c_str(), 1);
    assert(retval == 0);
  }

  SetupGlobalEnvironmentParams();
  protected_parameters_.clear();
  ParsePath("/etc/cvmfs/default.conf", false);
  vector<string> dist_defaults = FindFilesBySuffix("/etc/cvmfs/default.d",
                                                   ".conf");
  for (unsigned i = 0; i < dist_defaults.size(); ++i) {
    ParsePath(dist_defaults[i], false);
  }
  ProtectParameter("CVMFS_CONFIG_REPOSITORY");
  string external_config_path;
  if ((fqrn != "") && HasConfigRepository(fqrn, &external_config_path))
    ParseConfigRepoFile(external_config_path, "default.conf");
  ParsePath("/etc/cvmfs/default.local", false);

  if (fqrn != "") {
    string domain;
    vector<string> tokens = SplitString(fqrn, '.');
    assert(tokens.size() > 1);
    tokens.erase(tokens.begin());
    domain = JoinStrings(tokens, ".");

    if (HasConfigRepository(fqrn, &external_config_path))
      ParseConfigRepoFile(external_config_path, "domain.d/" + domain + ".conf");
    ParsePath("/etc/cvmfs/domain.d/" + domain + ".conf", false);
    ParsePath("/etc/cvmfs/domain.d/" + domain + ".local", false);

    if (HasConfigRepository(fqrn, &external_config_path))
      ParseConfigRepoFile(external_config_path, "config.d/" + fqrn + ".conf");
    ParsePath("/etc/cvmfs/config.d/" + fqrn + ".conf", false);
    ParsePath("/etc/cvmfs/config.d/" + fqrn + ".local", false);
  }
}


void OptionsManager::ParseConfigRepoFile(const string &config_path,
                                         const string &rel_path) {
  const string mount_path = config_path + rel_path;
  string config_repository;
  GetValue("CVMFS_CONFIG_REPOSITORY", &config_repository);
  if (!config_repo_tree_ || (config_repo_tree_->fqrn != config_repository)) {
    ParsePath(mount_path, true);
    return;
  }

  if (!config_repo_tree_->HasDirectory("etc/cvmfs")) {
    string repo_required;
    if (GetValue("CVMFS_CONFIG_REPO_REQUIRED", &repo_required)
        && IsOn(repo_required)) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
               "required configuration repository %s has no /etc/cvmfs",
               config_repository.c_str());
      exit(1);
    }
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogWarn,
             "configuration repository %s has no /etc/cvmfs",
             config_repository.c_str());
    return;
  }

  const string tree_path = "etc/cvmfs/" + rel_path;
  string content;
  if (!config_repo_tree_->Lookup(tree_path, &content)) {
    LogCvmfs(kLogCvmfs, kLogDebug, "%s not in fetched config repository",
             tree_path.c_str());
    return;
  }
  string mount_dir;
  GetValue("CVMFS_MOUNT_DIR", &mount_dir);
  const string shell_text = InlineIncludes(*config_repo_tree_, mount_dir,
                                           tree_path, content);
  ParseBuffer(shell_text, content, mount_path);
}


void OptionsManager::PopulateParameter(const string &param, ConfigValue val) {
  const map<string, string>::const_iterator iter = protected_parameters_.find(
      param);
  if ((iter != protected_parameters_.end()) && (iter->second != val.value)) {
    LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
             "error in cvmfs configuration: attempt to change protected %s "
             "from %s to %s",
             param.c_str(), iter->second.c_str(), val.value.c_str());
    return;
  }
  ParseValue(param, &val);
  config_[param] = val;
  UpdateEnvironment(param, val);
}

void OptionsManager::UpdateEnvironment(const string &param, ConfigValue val) {
  if (taint_environment_) {
    const int retval = setenv(param.c_str(), val.value.c_str(), 1);
    assert(retval == 0);
  }
}

void OptionsManager::ParseValue(std::string param, ConfigValue *val) {
  const string orig = val->value;
  const bool has_templ = opt_templ_mgr_->ParseString(&(val->value));
  if (has_templ) {
    templatable_values_[param] = orig;
  }
}


void OptionsManager::ProtectParameter(const string &param) {
  string value;
  // We don't care about the result.  If param does not yet exists, we lock it
  // to the empty string.
  (void)GetValue(param, &value);
  protected_parameters_[param] = value;
}


void OptionsManager::ClearConfig() { config_.clear(); }


bool OptionsManager::IsDefined(const std::string &key) {
  const map<string, ConfigValue>::const_iterator iter = config_.find(key);
  return iter != config_.end();
}


bool OptionsManager::GetValue(const string &key, string *value) const {
  const map<string, ConfigValue>::const_iterator iter = config_.find(key);
  if (iter != config_.end()) {
    *value = iter->second.value;
    return true;
  }
  *value = "";
  return false;
}


std::string OptionsManager::GetValueOrDie(const string &key) {
  std::string value;
  const bool retval = GetValue(key, &value);
  if (!retval) {
    PANIC(kLogStderr | kLogDebug, "%s configuration parameter missing",
          key.c_str());
  }
  return value;
}


bool OptionsManager::GetSource(const string &key, string *value) {
  const map<string, ConfigValue>::const_iterator iter = config_.find(key);
  if (iter != config_.end()) {
    *value = iter->second.source;
    return true;
  }
  *value = "";
  return false;
}


bool OptionsManager::IsOn(const std::string &param_value) const {
  const string uppercase = ToUpper(param_value);
  return ((uppercase == "YES") || (uppercase == "ON") || (uppercase == "1")
          || (uppercase == "TRUE"));
}


bool OptionsManager::IsOff(const std::string &param_value) const {
  const string uppercase = ToUpper(param_value);
  return ((uppercase == "NO") || (uppercase == "OFF") || (uppercase == "0")
          || (uppercase == "FALSE"));
}


vector<string> OptionsManager::GetAllKeys() {
  vector<string> result;
  for (map<string, ConfigValue>::const_iterator i = config_.begin(),
                                                iEnd = config_.end();
       i != iEnd;
       ++i) {
    result.push_back(i->first);
  }
  return result;
}


vector<string> OptionsManager::GetEnvironmentSubset(const string &key_prefix,
                                                    bool strip_prefix) {
  vector<string> result;
  for (map<string, ConfigValue>::const_iterator i = config_.begin(),
                                                iEnd = config_.end();
       i != iEnd;
       ++i) {
    const bool ignore_prefix = false;
    if (HasPrefix(i->first, key_prefix, ignore_prefix)) {
      const string output_key = strip_prefix
                                    ? i->first.substr(key_prefix.length())
                                    : i->first;
      result.push_back(output_key + "=" + i->second.value);
    }
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
    result += keys[i] + "=" + EscapeShell(value) + "    # from " + source
              + "\n";
  }
  return result;
}

void OptionsManager::SetValueFromTalk(const string &key, const string &value) {
  ConfigValue config_value;
  config_value.source = "cvmfs_talk";
  config_value.value = value;
  PopulateParameter(key, config_value);
}

void OptionsManager::SetValue(const string &key, const string &value) {
  ConfigValue config_value;
  config_value.source = "@INTERNAL@";
  config_value.value = value;
  PopulateParameter(key, config_value);
}


void OptionsManager::UnsetValue(const string &key) {
  protected_parameters_.erase(key);
  config_.erase(key);
  if (taint_environment_)
    unsetenv(key.c_str());
}

const char *DefaultOptionsTemplateManager ::kTemplateIdentFqrn = "fqrn";

const char *DefaultOptionsTemplateManager ::kTemplateIdentOrg = "org";

DefaultOptionsTemplateManager::DefaultOptionsTemplateManager(std::string fqrn) {
  SetTemplate(kTemplateIdentFqrn, fqrn);
  vector<string> fqrn_parts = SplitString(fqrn, '.');
  SetTemplate(kTemplateIdentOrg, fqrn_parts[0]);
}

void OptionsTemplateManager::SetTemplate(std::string name, std::string val) {
  templates_[name] = val;
}

std::string OptionsTemplateManager::GetTemplate(std::string name) {
  if (templates_.count(name)) {
    return templates_[name];
  } else {
    std::string var_name = "@" + name + "@";
    LogCvmfs(kLogCvmfs, kLogDebug, "Undeclared variable: %s", var_name.c_str());
    return var_name;
  }
}

bool OptionsTemplateManager::ParseString(std::string *input) {
  std::string result;
  std::string in = *input;
  bool has_vars = false;
  int mode = 0;
  std::string stock;
  for (std::string::size_type i = 0; i < in.size(); i++) {
    switch (mode) {
      case 0:
        if (in[i] == '@') {
          mode = 1;
        } else {
          result += in[i];
        }
        break;
      case 1:
        if (in[i] == '@') {
          mode = 0;
          result += GetTemplate(stock);
          stock = "";
          has_vars = true;
        } else {
          stock += in[i];
        }
        break;
    }
  }
  if (mode == 1) {
    result += "@" + stock;
  }
  *input = result;
  return has_vars;
}

bool OptionsTemplateManager::HasTemplate(std::string name) {
  return templates_.count(name);
}

//------------------------------------------------------------------------------


/**
 * Collapses "." and ".." in a relative path.  Fails if ".." leaves the root.
 */
static bool NormalizeRelPath(const string &path, vector<string> *components) {
  components->clear();
  const vector<string> parts = SplitString(path, '/');
  for (unsigned i = 0; i < parts.size(); ++i) {
    if (parts[i].empty() || (parts[i] == "."))
      continue;
    if (parts[i] == "..") {
      if (components->empty())
        return false;
      components->pop_back();
      continue;
    }
    components->push_back(parts[i]);
  }
  return true;
}


bool ConfigRepoTree::Resolve(const string &path, unsigned depth,
                             string *resolved) const {
  static const unsigned kMaxSymlinkDepth = 8;
  vector<string> components;
  if (!NormalizeRelPath(path, &components))
    return false;

  string current;
  for (unsigned i = 0; i < components.size(); ++i) {
    const string next = current.empty() ? components[i]
                                        : current + "/" + components[i];
    const map<string, string>::const_iterator link = symlinks.find(next);
    if (link == symlinks.end()) {
      current = next;
      continue;
    }
    // Absolute targets would point outside of the fetched tree
    if ((depth >= kMaxSymlinkDepth) || link->second.empty()
        || (link->second[0] == '/')) {
      return false;
    }
    string rest = current.empty() ? link->second : current + "/" + link->second;
    for (unsigned j = i + 1; j < components.size(); ++j)
      rest += "/" + components[j];
    return Resolve(rest, depth + 1, resolved);
  }
  *resolved = current;
  return true;
}


bool ConfigRepoTree::Lookup(const string &path, string *content) const {
  string resolved;
  if (!Resolve(path, 0, &resolved))
    return false;
  const map<string, string>::const_iterator file = files.find(resolved);
  if (file == files.end())
    return false;
  *content = file->second;
  return true;
}


bool ConfigRepoTree::HasDirectory(const string &path) const {
  string resolved;
  if (!Resolve(path, 0, &resolved))
    return false;
  const string prefix = resolved.empty() ? "" : resolved + "/";
  map<string, string>::const_iterator i = files.lower_bound(prefix);
  if ((i != files.end()) && HasPrefix(i->first, prefix, false))
    return true;
  i = symlinks.lower_bound(prefix);
  return (i != symlinks.end()) && HasPrefix(i->first, prefix, false);
}


vector<string> ConfigRepoTree::List(const string &dir_path) const {
  vector<string> result;
  string resolved;
  if (!Resolve(dir_path, 0, &resolved))
    return result;
  const string prefix = resolved.empty() ? "" : resolved + "/";
  const map<string, string> *maps[] = {&files, &symlinks};
  for (unsigned m = 0; m < 2; ++m) {
    for (map<string, string>::const_iterator i = maps[m]->lower_bound(prefix);
         (i != maps[m]->end()) && HasPrefix(i->first, prefix, false);
         ++i) {
      const string name = i->first.substr(prefix.length());
      if (name.find('/') == string::npos)
        result.push_back(name);
    }
  }
  sort(result.begin(), result.end());
  return result;
}


bool ConfigRepoTree::MapMountPath(const string &mount_dir,
                                  const string &abs_path,
                                  string *rel_path) const {
  if (mount_dir.empty() || fqrn.empty())
    return false;
  const string root = MakeCanonicalPath(mount_dir) + "/" + fqrn;
  const string path = MakeCanonicalPath(abs_path);
  if (path == root) {
    *rel_path = "";
    return true;
  }
  if (!HasPrefix(path, root + "/", false))
    return false;
  *rel_path = path.substr(root.length() + 1);
  return true;
}


static const char kTreeMagic[] = "CVMFSCFGTREE1";

static void AppendU64(uint64_t value, string *buf) {
  for (unsigned i = 0; i < 8; ++i)
    buf->push_back(static_cast<char>((value >> (8 * i)) & 0xff));
}

static void AppendBlob(const string &blob, string *buf) {
  AppendU64(blob.size(), buf);
  buf->append(blob);
}

static bool ConsumeU64(const string &buf, size_t *pos, uint64_t *value) {
  if (buf.size() - *pos < 8)
    return false;
  *value = 0;
  for (unsigned i = 0; i < 8; ++i) {
    *value |= static_cast<uint64_t>(static_cast<unsigned char>(buf[*pos + i]))
              << (8 * i);
  }
  *pos += 8;
  return true;
}

static bool ConsumeBlob(const string &buf, size_t *pos, uint64_t max_size,
                        string *blob) {
  uint64_t size;
  if (!ConsumeU64(buf, pos, &size) || (size > max_size)
      || (size > buf.size() - *pos)) {
    return false;
  }
  blob->assign(buf, *pos, size);
  *pos += size;
  return true;
}


string ConfigRepoTree::Serialize() const {
  string buf(kTreeMagic, sizeof(kTreeMagic));
  AppendBlob(fqrn, &buf);
  AppendU64(revision, &buf);
  AppendBlob(root_hash, &buf);
  AppendU64(files.size(), &buf);
  AppendU64(symlinks.size(), &buf);
  for (map<string, string>::const_iterator i = files.begin(); i != files.end();
       ++i) {
    AppendBlob(i->first, &buf);
    AppendBlob(i->second, &buf);
  }
  for (map<string, string>::const_iterator i = symlinks.begin();
       i != symlinks.end();
       ++i) {
    AppendBlob(i->first, &buf);
    AppendBlob(i->second, &buf);
  }
  return buf;
}


bool ConfigRepoTree::Deserialize(const string &buf, ConfigRepoTree *tree) {
  static const uint64_t kMaxNameSize = 4096;
  if ((buf.size() < sizeof(kTreeMagic))
      || (memcmp(buf.data(), kTreeMagic, sizeof(kTreeMagic)) != 0)) {
    return false;
  }
  size_t pos = sizeof(kTreeMagic);
  uint64_t num_files;
  uint64_t num_symlinks;
  if (!ConsumeBlob(buf, &pos, kMaxNameSize, &tree->fqrn)
      || !ConsumeU64(buf, &pos, &tree->revision)
      || !ConsumeBlob(buf, &pos, kMaxNameSize, &tree->root_hash)
      || !ConsumeU64(buf, &pos, &num_files)
      || !ConsumeU64(buf, &pos, &num_symlinks) || (num_files > kMaxEntries)
      || (num_symlinks > kMaxEntries - num_files)) {
    return false;
  }
  uint64_t total_size = 0;
  for (uint64_t i = 0; i < num_files + num_symlinks; ++i) {
    string path;
    string data;
    const uint64_t max_data = (i < num_files) ? kMaxFileSize : kMaxNameSize;
    if (!ConsumeBlob(buf, &pos, kMaxNameSize, &path)
        || !ConsumeBlob(buf, &pos, max_data, &data)) {
      return false;
    }
    total_size += data.size();
    if (total_size > kMaxTotalSize)
      return false;
    if (i < num_files)
      tree->files[path] = data;
    else
      tree->symlinks[path] = data;
  }
  return pos == buf.size();
}


/**
 * Returns the literal path of a ". path" or "source path" line, or the empty
 * string if the line is no include or its argument needs shell expansion
 * (other than the well-known config repository prefix).
 */
static string GetIncludePath(const string &line) {
  string rest = Trim(line);
  if (HasPrefix(rest, ". ", false) || HasPrefix(rest, ".\t", false)) {
    rest = rest.substr(2);
  } else if (HasPrefix(rest, "source ", false)
             || HasPrefix(rest, "source\t", false)) {
    rest = rest.substr(7);
  } else {
    return "";
  }
  rest = Trim(rest);
  const size_t comment = rest.find(" #");
  if (comment != string::npos)
    rest = Trim(rest.substr(0, comment));
  if ((rest.length() > 2)
      && (((rest[0] == '"') && (rest[rest.length() - 1] == '"'))
          || ((rest[0] == '\'') && (rest[rest.length() - 1] == '\'')))) {
    rest = rest.substr(1, rest.length() - 2);
  }

  static const char *kRepoPrefixes[] = {
      "$CVMFS_MOUNT_DIR/$CVMFS_CONFIG_REPOSITORY/",
      "${CVMFS_MOUNT_DIR}/${CVMFS_CONFIG_REPOSITORY}/"};
  string prefix;
  for (unsigned i = 0; i < 2; ++i) {
    if (HasPrefix(rest, kRepoPrefixes[i], false)) {
      prefix = kRepoPrefixes[i];
      rest = rest.substr(prefix.length());
      break;
    }
  }
  if (rest.empty())
    return "";
  for (unsigned i = 0; i < rest.length(); ++i) {
    const char c = rest[i];
    if (!(((c >= '0') && (c <= '9')) || ((c >= 'A') && (c <= 'Z'))
          || ((c >= 'a') && (c <= 'z')) || (c == '/') || (c == '.')
          || (c == '_') || (c == '-') || (c == '+'))) {
      return "";
    }
  }
  // Marks a path relative to the repository root
  return prefix.empty() ? rest : "//" + rest;
}


bool OptionsManager::InlineIncludesRec(const ConfigRepoTree &tree,
                                       const string &mount_dir,
                                       const string &path,
                                       const string &content,
                                       vector<string> *stack,
                                       string *result) {
  static const unsigned kMaxIncludeDepth = 4;
  const vector<string> lines = SplitString(content, '\n');
  for (unsigned i = 0; i < lines.size(); ++i) {
    if (i > 0)
      result->push_back('\n');
    const string include = GetIncludePath(lines[i]);
    if (include.empty()) {
      *result += lines[i];
      continue;
    }

    string include_rel;
    bool in_tree = true;
    if (HasPrefix(include, "//", false)) {
      include_rel = include.substr(2);
    } else if (include[0] == '/') {
      in_tree = tree.MapMountPath(mount_dir, include, &include_rel);
    } else {
      include_rel = GetParentPath(path) + "/" + include;
    }
    vector<string> components;
    string included;
    if (!in_tree || !NormalizeRelPath(include_rel, &components)) {
      LogCvmfs(kLogCvmfs, kLogDebug, "%s: include %s not in config repository",
               path.c_str(), include.c_str());
      *result += lines[i];
      continue;
    }
    include_rel = JoinStrings(components, "/");
    if (!tree.Lookup(include_rel, &included)) {
      LogCvmfs(kLogCvmfs, kLogDebug, "%s: included %s missing", path.c_str(),
               include_rel.c_str());
      *result += lines[i];
      continue;
    }
    if ((stack->size() >= kMaxIncludeDepth)
        || (find(stack->begin(), stack->end(), include_rel) != stack->end())) {
      LogCvmfs(kLogCvmfs, kLogDebug | kLogSyslogErr,
               "%s: include loop or too deep nesting at %s", path.c_str(),
               include_rel.c_str());
      return false;
    }
    stack->push_back(include_rel);
    const bool retval = InlineIncludesRec(tree, mount_dir, include_rel,
                                          included, stack, result);
    stack->pop_back();
    if (!retval)
      return false;
  }
  return true;
}


string OptionsManager::InlineIncludes(const ConfigRepoTree &tree,
                                      const string &mount_dir,
                                      const string &path,
                                      const string &content) {
  vector<string> stack;
  stack.push_back(path);
  string result;
  if (!InlineIncludesRec(tree, mount_dir, path, content, &stack, &result))
    return "";
  return result;
}


#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif
