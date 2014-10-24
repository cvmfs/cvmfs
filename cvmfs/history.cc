/**
 * This file is part of the CernVM File System.
 */

#include "history.h"

#include <cstdlib>
#include <cassert>

#include <algorithm>

#include "logging.h"
#include "util.h"

using namespace std;  // NOLINT

namespace history {

const float HistoryDatabase::kLatestSchema = 1.0;
const float HistoryDatabase::kLatestSupportedSchema = 1.0;
const unsigned HistoryDatabase::kLatestSchemaRevision = 1;


/**
 * This method creates a new database file and initializes the database schema.
 */
bool HistoryDatabase::CreateEmptyDatabase() {
  assert (read_write());
  return sqlite::Sql(sqlite_db(),
    "CREATE TABLE tags (name TEXT, hash TEXT, revision INTEGER, "
    "  timestamp INTEGER, channel INTEGER, description TEXT, size INTEGER, "
    "  CONSTRAINT pk_tags PRIMARY KEY (name))").Execute();
}


bool HistoryDatabase::InsertInitialValues(const std::string &repository_name) {
  assert (read_write());
  sqlite::Sql add_fqrn(sqlite_db(), "INSERT INTO properties "
                                    "(key, value) VALUES ('fqrn', :name);");
  return add_fqrn.BindText(1, repository_name) &&
         add_fqrn.Execute();
}


bool HistoryDatabase::CheckSchemaCompatibility() {
  return ! ((schema_version() < kLatestSupportedSchema - kSchemaEpsilon) ||
            (schema_version() > kLatestSchema          + kSchemaEpsilon));
}


bool HistoryDatabase::LiveSchemaUpgradeIfNecessary() {
  assert (read_write());

  if (schema_revision() == 0) {
    LogCvmfs(kLogHistory, kLogDebug, "upgrading schema revision");

    sqlite::Sql sql_upgrade(sqlite_db(), "ALTER TABLE tags ADD size INTEGER;");
    if (!sql_upgrade.Execute()) {
      LogCvmfs(kLogHistory, kLogDebug, "failed to upgrade tags table");
      return false;
    }

    set_schema_revision(1);
    if (! StoreSchemaRevision()) {
      LogCvmfs(kLogHistory, kLogDebug, "failed tp upgrade schema revision");
      return false;
    }
  }

  return true;
}


//------------------------------------------------------------------------------


bool SqlTag::BindTag(const Tag &tag) {
  return (
    BindText(1, tag.name) &&
    BindTextTransient(2, tag.root_hash.ToString()) && // temporary from ToString
    BindInt64(3, tag.revision) &&
    BindInt64(4, tag.timestamp) &&
    BindInt64(5, tag.channel) &&
    BindText(6, tag.description) &&
    BindInt64(7, tag.size)
  );
}


Tag SqlTag::RetrieveTag() {
  Tag result;
  result.name = string(reinterpret_cast<const char *>(RetrieveText(0)));
  const string hash_str(reinterpret_cast<const char *>(RetrieveText(1)));
  result.root_hash = shash::MkFromHexPtr(shash::HexPtr(hash_str));
  result.revision = RetrieveInt64(2);
  result.timestamp = RetrieveInt64(3);
  result.channel = static_cast<UpdateChannel>(RetrieveInt64(4));
  result.description = string(reinterpret_cast<const char *>(RetrieveText(5)));
  result.size = RetrieveInt64(6);
  return result;
}


//------------------------------------------------------------------------------


bool TagList::Load(Database *database) {
  assert(database);
  string size_field = "0";
  if (database->schema_revision() >= 1)
    size_field = "size";
  SqlTag sql_load(*database,
    // NULL for size automatically converted to 0
    "SELECT name, hash, revision, timestamp, channel, description, " +
    size_field +
    " FROM tags ORDER BY revision;");
  while (sql_load.FetchRow())
    list_.push_back(sql_load.RetrieveTag());

  return true;
}


bool TagList::Store(Database *database) {
  assert(database);
  SqlTag sql_erase(*database, "DELETE FROM tags;");
  bool retval = sql_erase.Execute();
  assert(retval);

  SqlTag sql_store(*database,
    "INSERT INTO tags "
    "(name, hash, revision, timestamp, channel, description, size) VALUES "
    "(:n, :h, :r, :t, :c, :d, :s);");
  for (unsigned i = 0; i < list_.size(); ++i) {
    retval = sql_store.BindTag(list_[i]);
    assert(retval);
    retval = sql_store.Execute();
    if (!retval) {
      LogCvmfs(kLogHistory, kLogStderr, "failed to store taglist (%s)",
               database->GetLastErrorMsg().c_str());
      abort();
    }
    sql_store.Reset();
  }

  return true;
}


string TagList::List() {
  string result =
    "NAME | HASH | SIZE | REVISION | TIMESTAMP | CHANNEL | DESCRIPTION\n";
  for (unsigned i = 0; i < list_.size(); ++i) {
    Tag tag(list_[i]);
    string tag_size = "n/a";
    if (tag.size > 0 && tag.size < 1024)
      tag_size = StringifyInt(tag.size);
    else if (tag.size >= 1024 && tag.size < 1024*1024)
      tag_size = StringifyInt(tag.size/1024) + "kB";
    else if (tag.size >= 1024*1024)
      tag_size = StringifyInt(tag.size/(1024*1024)) + "MB";
    result += tag.name + " | " + tag.root_hash.ToString() + " | " +
              tag_size + " | " +
              StringifyInt(tag.revision) + " | " +
              StringifyTime(tag.timestamp, true) + " | " +
              StringifyInt(tag.channel) + " | " + tag.description + "\n";
  }
  return result;
}


bool TagList::FindTag(const string &name, Tag *tag) {
  assert(tag);
  for (unsigned i = 0; i < list_.size(); ++i) {
    if (list_[i].name == name) {
      *tag = list_[i];
      return true;
    }
  }
  return false;
}


bool TagList::FindTagByDate(const time_t seconds_utc, Tag *tag) {
  assert(tag);
  bool result = false;
  for (unsigned i = 0; i < list_.size(); ++i) {
    if (list_[i].timestamp > seconds_utc)
      break;
    *tag = list_[i];
    result = true;
  }
  return result;
}


bool TagList::FindRevision(const unsigned revision, Tag *tag) {
  assert(tag);
  for (unsigned i = 0; i < list_.size(); ++i) {
    if (list_[i].revision == revision) {
      *tag = list_[i];
      return true;
    }
  }
  return false;
}


bool TagList::FindHash(const shash::Any &hash, Tag *tag) {
  assert(tag);
  for (unsigned i = 0; i < list_.size(); ++i) {
    if (list_[i].root_hash == hash) {
      *tag = list_[i];
      return true;
    }
  }
  return false;
}


void TagList::Remove(const string &name) {
  for (vector<Tag>::iterator i = list_.begin(); i < list_.end(); ++i) {
    if (i->name == name) {
      list_.erase(i);
      return;
    }
  }
}


TagList::Failures TagList::Insert(const Tag &tag) {
  Tag existing_tag;
  if (FindTag(tag.name, &existing_tag))
    return kFailTagExists;

  list_.push_back(tag);
  return kFailOk;
}


map<string, shash::Any> TagList::GetAllHashes() {
  map<string, shash::Any> result;
  for (unsigned i = 0; i < list_.size(); ++i) {
    result[list_[i].name] = list_[i].root_hash;
  }
  return result;
}


struct revision_comparator {
  bool operator() (const Tag &tag1, const Tag &tag2) {
    return tag1.revision < tag2.revision;
  }
};

struct hash_extractor {
  const shash::Any& operator() (const Tag &tag) {
    return tag.root_hash;
  }
};

std::vector<shash::Any> TagList::GetReferencedHashes() const {
  // copy the internal tag list and sort it by decending revision
  std::vector<Tag> tags = list_;
  std::sort(tags.begin(), tags.end(), revision_comparator());

  // extract the root catalog hashes from it
  std::vector<shash::Any> hashes(tags.size());
  std::transform(tags.begin(), tags.end(), hashes.begin(), hash_extractor());
  return hashes;
}


// Ordered list, newest releases first
vector<TagList::ChannelTag> TagList::GetChannelTops() {
  vector<ChannelTag> result;
  if (list_.size() == 0)
    return result;

  vector<Tag> sorted_tag_list(list_);
  sort(sorted_tag_list.begin(), sorted_tag_list.end());

  set<UpdateChannel> processed_channels;
  for (int i = sorted_tag_list.size()-1; i >= 0; --i) {
    UpdateChannel channel = sorted_tag_list[i].channel;
    if (channel == kChannelTrunk)
      continue;
    if (processed_channels.find(channel) == processed_channels.end()) {
      result.push_back(ChannelTag(channel, sorted_tag_list[i].root_hash));
      processed_channels.insert(channel);
    }
  }
  return result;
}


void TagList::Rollback(const unsigned until_revision) {
  for (vector<Tag>::iterator i = list_.begin(); i < list_.end(); ) {
    if (i->revision >= until_revision)
      i = list_.erase(i);
    else
      ++i;
  }
}

}  // namespace history
