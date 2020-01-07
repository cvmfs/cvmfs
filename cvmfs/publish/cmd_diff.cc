/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_diff.h"

#include <cassert>
#include <string>

#include "directory_entry.h"
#include "logging.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "publish/settings.h"
#include "sanitizer.h"

namespace {

class DiffReporter : public publish::DiffListener {
 public:
  virtual ~DiffReporter() {}
  virtual void OnAdd(const std::string &path,
                     const catalog::DirectoryEntry &entry)
  {
    LogCvmfs(kLogCvmfs, kLogStdout, "Add: %s", path.c_str());
  }

  virtual void OnRemove(const std::string &path,
                        const catalog::DirectoryEntry &entry)
  {
    LogCvmfs(kLogCvmfs, kLogStdout, "Remove: %s", path.c_str());
  }

  virtual void OnModify(const std::string &path,
                        const catalog::DirectoryEntry &entry_from,
                        const catalog::DirectoryEntry &entry_to)
  {
    LogCvmfs(kLogCvmfs, kLogStdout, "Modify: %s", path.c_str());
  }
};

}  // anonymous namespace


namespace publish {

int CmdDiff::Main(const Options &options) {
  std::string url = options.plain_args()[0].value_str;
  std::string fqrn = Repository::GetFqrnFromUrl(url);
  sanitizer::RepositorySanitizer sanitizer;
  if (!sanitizer.IsValid(fqrn)) {
    throw EPublish("malformed repository name: " + fqrn);
  }

  std::string from = options.GetStringDefault("from", "trunk-previous");
  std::string to = options.GetStringDefault("to", "trunk");

  SettingsRepository settings_repository(fqrn);
  settings_repository.SetUrl(url);
  if (options.Has("keychain")) {
    settings_repository.GetKeychain()->SetKeychainDir(
      options.GetString("keychain"));
  }
  Repository repository(settings_repository);

  DiffReporter diff_reporter;
  repository.Diff(from, to, &diff_reporter);

  return 0;
}

}  // namespace publish
