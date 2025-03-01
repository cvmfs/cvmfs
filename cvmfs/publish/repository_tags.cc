/**
 * This file is part of the CernVM File System.
 */


#include "publish/repository.h"

#include <vector>

#include "history_sqlite.h"
#include "publish/except.h"
#include "sanitizer.h"

namespace publish {

void Publisher::CheckTagName(const std::string &name) {
  if (name.empty()) throw EPublish("the empty string is not a valid tag name");
  if (name == "trunk")
    throw EPublish("'trunk' is not allowed as a custom tag name");
  if (name == "trunk-previous")
    throw EPublish("'trunk-previous' is not allowed as a custom tag name");
  if (!sanitizer::TagSanitizer().IsValid(name))
    throw EPublish("invalid tag name: " + name);
}


void Publisher::EditTags(const std::vector<history::History::Tag> &add_tags,
                         const std::vector<std::string> &rm_tags)
{
  if (!in_transaction_.IsSet())
    throw EPublish("cannot edit tags outside transaction");

  for (unsigned i = 0; i < add_tags.size(); ++i) {
    std::string name = add_tags[i].name;
    CheckTagName(name);
    history_->Insert(add_tags[i]);
  }

  for (unsigned i = 0; i < rm_tags.size(); ++i) {
    std::string name = rm_tags[i];
    CheckTagName(name);
    if (history_->Exists(name)) {
      bool retval = history_->Remove(name);
      if (!retval) throw EPublish("cannot remove tag " + name);
    }
  }

  PushHistory();

  // TODO(jblomer): virtual catalog
}

}  // namespace publish
