/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_mkfs.h"

#include <unistd.h>

#include <string>

#include "manifest.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "publish/settings.h"
#include "sanitizer.h"
#include "signature.h"
#include "upload_spooler_definition.h"
#include "util/pointer.h"
#include "util/posix.h"

namespace publish {

int CmdMkfs::Main(const Options &options) {
  std::string fqrn = options.plain_args()[0].value_str;
  sanitizer::RepositorySanitizer sanitizer;
  if (!sanitizer.IsValid(fqrn)) {
    throw EPublish("malformed repository name: " + fqrn);
  }
  SettingsPublisher settings(fqrn);

  std::string user_name = GetUserName();
  if (options.HasNot("owner")) {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak, "Owner of %s [%s]: ",
             fqrn.c_str(), user_name.c_str());
    std::string input;
    int c;
    while ((c = getchar()) != EOF) {
      if (c == '\n') break;
      input.push_back(c);
    }
    if (!input.empty()) user_name = input;
  }
  settings.SetOwner(user_name);

  // Sanity checks
  if (options.Has("no-autotags") && options.Has("autotag-span")) {
    throw EPublish(
        "options 'no-autotags' and 'autotag-span' are mutually exclusive",
        EPublish::kFailInvocation);
  }
  if (options.HasNot("no-autotags") && options.HasNot("autotag-span") &&
      options.Has("gc"))
  {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "Note: Autotagging all revisions impedes garbage collection");
  }

  // Needs to be done before the storage and its temp dir is configured
  if (options.Has("no-publisher")) {
    settings.GetTransaction()->GetSpoolArea()->UseSystemTempDir();
    if (options.Has("export-keychain")) {
      settings.GetKeychain()->SetKeychainDir(
        options.GetString("export-keychain"));
    } else {
      settings.GetKeychain()->SetKeychainDir(".");
    }
  }

  // Storage configuration
  if (options.Has("storage")) {
    if (options.Has("storage-s3")) {
      throw EPublish(
        "options 'storage' and 'storage-s3' are mutually exclusive",
        EPublish::kFailInvocation);
    }
    if (options.Has("storage-local")) {
      throw EPublish(
        "options 'storage' and 'storage-local' are mutually exclusive",
        EPublish::kFailInvocation);
    }
    settings.GetStorage()->SetLocator(options.GetString("storage"));
  } else if (options.Has("storage-s3")) {
    if (options.Has("storage-local")) {
      throw EPublish(
        "options 'storage-s3' and 'storage-local' are mutually exclusive",
        EPublish::kFailInvocation);
    }
    settings.GetStorage()->MakeS3(
      options.GetString("storage-s3"),
      settings.transaction().spool_area().tmp_dir());
  } else if (options.Has("storage-local")) {
    settings.GetStorage()->MakeLocal(options.GetString("storage-local"));
  }
  bool configure_apache =
    (settings.storage().type() == upload::SpoolerDefinition::Local) &&
    options.HasNot("no-apache");

  // Permission check
  if (geteuid() != 0) {
    bool can_unprivileged =
      options.Has("no-publisher") && !configure_apache &&
      (user_name == GetUserName());
    if (!can_unprivileged) {
      throw EPublish("root privileges required", EPublish::kFailPermission);
    }
  }

  // Stratum 0 URL
  if (options.Has("stratum0")) {
    settings.SetUrl(options.GetString("stratum0"));
  } else {
    bool need_stratum0 =
      (settings.storage().type() != upload::SpoolerDefinition::Local) &&
      options.HasNot("no-publisher");
    if (need_stratum0) {
      throw EPublish("missing repository stratum 0 URL for non-local storage "
                     "(add option -w)", EPublish::kFailInvocation);
    }
  }

  // Union file system
  if (options.HasNot("no-publisher")) {
    if (options.Has("unionfs")) {
      settings.GetTransaction()->SetUnionFsType(options.GetString("unionfs"));
    } else {
      settings.GetTransaction()->DetectUnionFsType();
    }
  } else {
    if (options.Has("unionfs")) {
      throw EPublish(
        "options 'no-publisher' and 'unionfs' are mutually exclusive",
        EPublish::kFailInvocation);
    }
  }

  if (configure_apache) {
    // TODO(jblomer): Apache configuration
  }

  // TODO(jblomer): for local backend we need to create the path as root and
  // then hand it over
  Publisher::Bootstrap(settings);
  // if (options.Has("no-apache"))

  return 0;
}

}  // namespace publish
