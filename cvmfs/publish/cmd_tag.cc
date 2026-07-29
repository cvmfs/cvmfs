/**
 * This file is part of the CernVM File System.
 */


#include "cmd_tag.h"

#include <errno.h>

#include <string>
#include <vector>

#include "history.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "publish/settings.h"
#include "upload_spooler_definition.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"
#include "whitelist.h"

namespace publish {

int CmdTag::Main(const Options &options) {
  const std::string fqrn = options.plain_args()[0].value_str;

  const bool has_add = options.Has("add");
  const bool has_remove = options.Has("remove");
  if (!has_add && !has_remove)
    throw EPublish("neither tags to add (-a) nor to remove (-d) given",
                   EPublish::kFailInvocation);

  SettingsBuilder builder;
  UniquePtr<SettingsPublisher> settings;
  try {
    settings = builder.CreateSettingsPublisher(fqrn, true /* needs_managed */);
  } catch (const EPublish &e) {
    if (e.failure() == EPublish::kFailRepositoryNotFound) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr, "CernVM-FS error: %s",
               e.msg().c_str());
      return 1;
    }
    throw;
  }

  if (settings->storage().type() != upload::SpoolerDefinition::Gateway) {
    throw EPublish("'cvmfs_publish tag' only supports gateway repositories",
                   EPublish::kFailInvocation);
  }

  if (!SwitchCredentials(settings->owner_uid(), settings->owner_gid(),
                         false /* temporarily */)) {
    throw EPublish("No write permission to repository",
                   EPublish::kFailPermission);
  }

  UniquePtr<Publisher> publisher;
  try {
    publisher = new Publisher(*settings);
    if (publisher->whitelist()->IsExpired()) {
      throw EPublish("Repository whitelist for " + fqrn + " is expired",
                     EPublish::kFailWhitelistExpired);
    }
  } catch (const EPublish &e) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr, "%s", e.msg().c_str());
    if (e.failure() == EPublish::kFailLayoutRevision
        || e.failure() == EPublish::kFailWhitelistExpired) {
      return EINVAL;
    }
    return EIO;
  }

  std::vector<history::History::Tag> add_tags;
  if (has_add) {
    history::History::Tag tag;
    tag.name = options.GetString("add");
    if (options.Has("description"))
      tag.description = options.GetString("description");
    add_tags.push_back(tag);
  }

  std::vector<std::string> rm_tags;
  if (has_remove)
    rm_tags = SplitString(options.GetString("remove"), ' ');

  try {
    publisher->EditTags(add_tags, rm_tags);
  } catch (const EPublish &e) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr,
             "CernVM-FS tag error: %s", e.msg().c_str());
    if (e.failure() == EPublish::kFailLeaseBusy)
      return EBUSY;
    return EIO;
  }

  return 0;
}

}  // namespace publish
