/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cmd_mkfs.h"

#include <unistd.h>

#include "publish/except.h"
#include "publish/repository.h"
#include "publish/settings.h"
#include "sanitizer.h"
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

  // Sanity checks
  if (geteuid() != 0) {
    bool can_unprivileged =
      options.Has("no-publisher") && options.Has("no-apache") &&
      (options.HasNot("owner") || options.GetString("owner") == GetUserName());
    if (!can_unprivileged) throw EPublish("root privileges required");
  }

  // Storage configuration
  if (options.Has("storage")) {
    if (options.Has("s3config")) {
      throw EPublish(
        "options 'storage' and 's3config' are mutually exclusive");
    }
    settings.GetStorage()->SetLocator(options.GetString("storage"));
  } else if (options.Has("s3config")) {
    settings.GetStorage()->MakeS3(options.GetString("s3config"),
                                  settings.transaction().spool_area());
  }
  bool configure_apache =
    (settings.storage().type() == upload::SpoolerDefinition::Local) &&
    options.HasNot("no-apache");

  // Stratum 0 URL
  if (options.Has("stratum0")) {
    settings.SetUrl(options.GetString("stratum0"));
  } else {
    bool need_stratum0 =
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

  }

  // TODO: for local backend we need to create the path as root and then
  // hand it over
  UniquePtr<Publisher> publisher(Publisher::Create(settings));
  //if (options.Has("no-apache"))

  return 0;
}

}  // namespace publish
