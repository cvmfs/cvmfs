/**
 * This file is part of the CernVM File System.
 */


#include <unistd.h>

#include <vector>

#include "history_sqlite.h"
#include "manifest.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "repository_tag.h"
#include "sanitizer.h"
#include "upload.h"
#include "upload_spooler_definition.h"
#include "util/logging.h"
#include "util/string.h"

namespace publish {

void Publisher::CheckTagName(const std::string &name) {
  if (name.empty())
    throw EPublish("the empty string is not a valid tag name");
  if (name == "trunk")
    throw EPublish("'trunk' is not allowed as a custom tag name");
  if (name == "trunk-previous")
    throw EPublish("'trunk-previous' is not allowed as a custom tag name");
  if (!sanitizer::TagSanitizer().IsValid(name))
    throw EPublish("invalid tag name: " + name);
}


void Publisher::EditTags(const std::vector<history::History::Tag> &add_tags,
                         const std::vector<std::string> &rm_tags) {
  if (settings_.storage().type() == upload::SpoolerDefinition::Gateway) {
    EditTagsGateway(add_tags, rm_tags);
    return;
  }

  if (!in_transaction_.IsSet())
    throw EPublish("cannot edit tags outside transaction");

  for (unsigned i = 0; i < add_tags.size(); ++i) {
    const std::string name = add_tags[i].name;
    CheckTagName(name);
    history_->Insert(add_tags[i]);
  }

  for (unsigned i = 0; i < rm_tags.size(); ++i) {
    const std::string name = rm_tags[i];
    CheckTagName(name);
    if (history_->Exists(name)) {
      const bool retval = history_->Remove(name);
      if (!retval)
        throw EPublish("cannot remove tag " + name);
    }
  }

  PushHistory();

  // TODO(jblomer): virtual catalog
}


void Publisher::EditTagsGateway(
    const std::vector<history::History::Tag> &add_tags,
    const std::vector<std::string> &rm_tags) {
  // Validate everything up front so we never acquire a lease for a request that
  // is bound to be rejected.
  RepositoryTag repo_tag;
  if (!rm_tags.empty()) {
    for (unsigned i = 0; i < rm_tags.size(); ++i)
      CheckTagName(rm_tags[i]);
    repo_tag.SetDeleteTags(JoinStrings(rm_tags, " "));
  }
  if (!add_tags.empty()) {
    // The commit protocol carries a single tag to add (tagging the new HEAD).
    if (add_tags.size() > 1) {
      throw EPublish("adding more than one tag in a single gateway operation "
                     "is not supported");
    }
    CheckTagName(add_tags[0].name);
    if (!add_tags[0].root_hash.IsNull()) {
      throw EPublish("tagging a specific root hash is not supported on gateway "
                     "repositories");
    }
    repo_tag.SetName(add_tags[0].name);
    repo_tag.SetDescription(add_tags[0].description);
  }

  // The tag database is edited by the receiver; the catalog itself does not
  // change, so the commit carries the current root hash as both the old and
  // the new hash (an empty catalog diff).
  const std::string root_hash = manifest_->catalog_hash().ToString(
      true /* with_suffix */);

  // Acquire a gateway lease and write the session token before constructing the
  // spoolers, which read it.
  session_->Acquire();
  ConstructSpoolers();

  spooler_files_->FinalizeSession(false /* commit */);
  const bool rvb = spooler_catalogs_->FinalizeSession(
      true /* commit */, root_hash, root_hash, repo_tag);

  if (!rvb) {
    // Leave the lease in place so the Session destructor releases it.
    throw EPublish("failed to commit tag changes to the gateway");
  }

  // A successful commit makes the gateway drop the lease; remove the local
  // session token and keep the Session object from trying to drop it again.
  session_->SetKeepAlive(true);
  unlink(session_->token_path().c_str());

  LogCvmfs(kLogCvmfs, kLogStdout, "Tags updated");
}

}  // namespace publish
