/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_info.h"

#include "logging.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "publish/settings.h"
#include "sanitizer.h"

namespace publish {

int CmdInfo::Main(const Options &options) {
  std::string url = options.plain_args()[0].value_str;
  std::string fqrn = Repository::GetFqrnFromUrl(url);
  sanitizer::RepositorySanitizer sanitizer;
  if (!sanitizer.IsValid(fqrn)) {
    throw EPublish("malformed repository name: " + fqrn);
  }

  SettingsRepository settings_repository(fqrn);
  settings_repository.SetUrl(url);
  if (options.Has("keychain")) {
    settings_repository.GetKeychain()->SetKeychainDir(
      options.GetString("keychain"));
  }
  Repository repository(settings_repository);

  LogCvmfs(kLogCvmfs, kLogStdout, "Connected to %s", fqrn.c_str());

  if (options.Has("meta-info")) {
    LogCvmfs(kLogCvmfs, kLogStdout, "%s", repository.GetMetainfo().c_str());
  }

  return 0;
}

}  // namespace publish
