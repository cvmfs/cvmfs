/**
 * This file is part of the CernVM File System.
 */


#include "cmd_mkfs.h"

#include <unistd.h>

#include <string>

#include "crypto/signature.h"
#include "manifest.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "publish/settings.h"
#include "sanitizer.h"
#include "upload_spooler_definition.h"
#include "util/pointer.h"
#include "util/posix.h"

namespace publish {

int CmdMkfs::Main(const Options &options) {
  const std::string fqrn = options.plain_args()[0].value_str;
  const sanitizer::RepositorySanitizer sanitizer;
  if (!sanitizer.IsValid(fqrn)) {
    throw EPublish("malformed repository name: " + fqrn);
  }
  SettingsPublisher settings(fqrn);

  std::string user_name = GetUserName();
  if (options.HasNot("owner")) {
    LogCvmfs(kLogCvmfs, kLogStdout | kLogNoLinebreak,
             "Owner of %s [%s]: ", fqrn.c_str(), user_name.c_str());
    std::string input;
    int c;
    while ((c = getchar()) != EOF) {
      if (c == '\n')
        break;
      input.push_back(c);
    }
    if (!input.empty())
      user_name = input;
  }
  settings.SetOwner(user_name);

  // Sanity checks
  if (options.Has("no-autotags") && options.Has("autotag-span")) {
    throw EPublish(
        "options 'no-autotags' and 'autotag-span' are mutually exclusive");
  }
  if (options.HasNot("no-autotags") && options.HasNot("autotag-span")
      && options.Has("gc")) {
    LogCvmfs(kLogCvmfs, kLogStdout,
             "Note: Autotagging all revisions impedes garbage collection");
  }

  // Needs to be done before the storage and its temp dir is configured
  if (options.Has("no-publisher")) {
    settings.GetTransaction()->GetSpoolArea()->UseSystemTempDir();
    settings.GetKeychain()->SetKeychainDir(".");
  }

  // Storage configuration
  if (options.Has("storage")) {
    if (options.Has("s3config")) {
      throw EPublish("options 'storage' and 's3config' are mutually exclusive");
    }
    settings.GetStorage()->SetLocator(options.GetString("storage"));
  } else if (options.Has("s3config")) {
    settings.GetStorage()->MakeS3(
        options.GetString("s3config"),
        settings.transaction().spool_area().tmp_dir());
  }
  const bool configure_apache =
      (settings.storage().type() == upload::SpoolerDefinition::Local) &&
      options.HasNot("no-apache");

  // Permission check
  if (geteuid() != 0) {
    const bool can_unprivileged = options.Has("no-publisher") &&
                                  !configure_apache &&
                                  (user_name == GetUserName());
    if (!can_unprivileged)
      throw EPublish("root privileges required");
  }

  // Stratum 0 URL
  if (options.Has("stratum0")) {
    settings.SetUrl(options.GetString("stratum0"));
  } else {
    const bool need_stratum0 =
        (settings.storage().type() != upload::SpoolerDefinition::Local) &&
        options.HasNot("no-publisher");
    if (need_stratum0) {
      throw EPublish("repository stratum 0 URL for non-local storage "
                     "(add option -w)");
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
          "options 'no-publisher' and 'unionfs' are mutually exclusive");
    }
  }

  if (configure_apache) {
    // TODO(jblomer): Apache configuration
  }

  // TODO(jblomer): for local backend we need to create the path as root and
  // then hand it over
  const UniquePtr<Publisher> publisher(Publisher::Create(settings));
  // if (options.Has("no-apache"))

  LogCvmfs(kLogCvmfs, kLogStdout, "PUBLIC MASTER KEY:\n%s",
           publisher->signature_mgr()->GetActivePubkeys().c_str());
  LogCvmfs(kLogCvmfs, kLogStdout, "CERTIFICATE:\n%s",
           publisher->signature_mgr()->GetCertificate().c_str());

  LogCvmfs(kLogCvmfs, kLogStdout, "MANIFEST:\n%s",
           publisher->manifest()->ExportString().c_str());

  return 0;
}

}  // namespace publish
