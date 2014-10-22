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

const float Database::kLatestSchema = 1.0;
const float Database::kLatestSupportedSchema = 1.0;
const float Database::kSchemaEpsilon = 0.0005;
const unsigned Database::kLatestSchemaRevision = 1;

bool Database::Open(const std::string filename,
                    const sqlite::DbOpenMode open_mode)
{
  filename_ = filename;
  ready_ = false;
  schema_version_ = 0.0;
  schema_revision_ = 0;
  sqlite_db_ = NULL;

  int flags = SQLITE_OPEN_NOMUTEX;
  switch (open_mode) {
    case sqlite::kDbOpenReadOnly:
      flags |= SQLITE_OPEN_READONLY;
      read_write_ = false;
      break;
    case sqlite::kDbOpenReadWrite:
      flags |= SQLITE_OPEN_READWRITE;
      read_write_ = true;
      break;
    default:
      abort();
  }

  // Open database file (depending on the flags read-only or read-write)
  LogCvmfs(kLogHistory, kLogDebug, "opening database file %s",
           filename_.c_str());
  if (SQLITE_OK != sqlite3_open_v2(filename_.c_str(), &sqlite_db_, flags, NULL))
  {
    LogCvmfs(kLogHistory, kLogDebug, "cannot open catalog database file %s",
             filename_.c_str());
    return false;
  }
  sqlite3_extended_result_codes(sqlite_db_, 1);

  {  // Get schema version and revision
    sqlite::Sql sql_schema(sqlite_db_,
                           "SELECT value FROM properties WHERE key='schema';");
    if (sql_schema.FetchRow()) {
      schema_version_ = sql_schema.RetrieveDouble(0);
    } else {
      LogCvmfs(kLogHistory, kLogDebug, "failed to retrieve schema in %s",
               filename_.c_str());
      goto database_failure;
    }
    sqlite::Sql sql_schema_revision(sqlite_db_,
      "SELECT value FROM properties WHERE key='schema_revision';");
    if (sql_schema_revision.FetchRow()) {
      schema_revision_ = sql_schema_revision.RetrieveInt(0);
    }
  }
  LogCvmfs(kLogCatalog, kLogDebug, "open db with schema version %f revision %d",
           schema_version_, schema_revision_);
  if ((schema_version_ < kLatestSupportedSchema-kSchemaEpsilon) ||
      (schema_version_ > kLatestSchema+kSchemaEpsilon))
  {
    LogCvmfs(kLogCatalog, kLogDebug, "schema version %f not supported (%s)",
             schema_version_, filename.c_str());
    goto database_failure;
  }
  // Live schema upgrade
  if (open_mode == sqlite::kDbOpenReadWrite) {
    if (schema_revision_ == 0) {
      LogCvmfs(kLogCatalog, kLogDebug, "upgrading schema revision");
      sqlite::Sql sql_upgrade(sqlite_db_, "ALTER TABLE tags ADD size INTEGER;");
      if (!sql_upgrade.Execute()) {
        LogCvmfs(kLogCatalog, kLogDebug, "failed tp upgrade nested_catalogs");
        goto database_failure;
      }
      sqlite::Sql sql_revision(sqlite_db_,
                               "INSERT INTO properties (key, value) VALUES "
                               "('schema_revision', 1);");
      if (!sql_revision.Execute()) {
        LogCvmfs(kLogCatalog, kLogDebug, "failed tp upgrade schema revision");
        goto database_failure;
      }

      schema_revision_ = 1;
    }
  }

  ready_ = true;
  return true;

 database_failure:
  sqlite3_close(sqlite_db_);
  sqlite_db_ = NULL;
  return false;
}


/**
 * Private constructor.  Used to create a new sqlite database.
 */
Database::Database(sqlite3 *sqlite_db, const float schema,
                   const unsigned schema_revision, const bool rw)
{
  sqlite_db_ = sqlite_db;
  filename_ = "TMP";
  schema_version_ = schema;
  schema_revision_ = schema_revision;
  read_write_ = rw;
  ready_ = false;  // Don't close on delete
}


Database::~Database() {
  if (ready_)
    sqlite3_close(sqlite_db_);
}


/**
 * This method creates a new database file and initializes the database schema.
 */
bool Database::Create(const string &filename, const string &repository_name)
{
  sqlite3 *sqlite_db;
  sqlite::Sql *sql_schema;
  sqlite::Sql *sql_schema_revision;
  sqlite::Sql *sql_fqrn;
  int open_flags = SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE |
                   SQLITE_OPEN_CREATE;

  // Create the new catalog file and open it
  LogCvmfs(kLogCatalog, kLogVerboseMsg, "creating new history db at '%s'",
           filename.c_str());
  if (sqlite3_open_v2(filename.c_str(), &sqlite_db, open_flags, NULL) !=
      SQLITE_OK)
  {
    LogCvmfs(kLogCatalog, kLogStderr,
             "Cannot create and open history database file '%s'",
             filename.c_str());
    return false;
  }
  sqlite3_extended_result_codes(sqlite_db, 1);
  Database database(sqlite_db, kLatestSchema, kLatestSchemaRevision, true);

  bool retval;
  string sql_create =
    "CREATE TABLE tags (name TEXT, hash TEXT, revision INTEGER, "
    "  timestamp INTEGER, channel INTEGER, description TEXT, size INTEGER, "
    "  CONSTRAINT pk_tags PRIMARY KEY (name))";
  retval = sqlite::Sql(sqlite_db, sql_create).Execute();
  if (!retval)
    goto create_schema_fail;

  sql_create = "CREATE TABLE properties (key TEXT, value TEXT, "
               "CONSTRAINT pk_properties PRIMARY KEY (key));";
  retval = sqlite::Sql(sqlite_db, sql_create).Execute();
  if (!retval)
    goto create_schema_fail;

  sql_schema = new sqlite::Sql(sqlite_db, "INSERT INTO properties "
                               "(key, value) VALUES ('schema', :schema);");
  retval = sql_schema->BindDouble(1, kLatestSchema) && sql_schema->Execute();
  delete sql_schema;
  if (!retval)
    goto create_schema_fail;

  sql_schema_revision =
    new sqlite::Sql(sqlite_db, "INSERT INTO properties "
                               "(key, value) VALUES ('schema_revision', :r);");
  retval = sql_schema_revision->BindInt(1, kLatestSchemaRevision) &&
           sql_schema_revision->Execute();
  delete sql_schema_revision;
  if (!retval)
    goto create_schema_fail;

  sql_fqrn = new sqlite::Sql(sqlite_db, "INSERT INTO properties "
                             "(key, value) VALUES ('fqrn', :name);");
  retval = sql_fqrn->BindText(1, repository_name) && sql_fqrn->Execute();
  delete sql_fqrn;
  if (!retval)
    goto create_schema_fail;

  sqlite3_close(sqlite_db);
  return true;

 create_schema_fail:
  LogCvmfs(kLogSql, kLogVerboseMsg, "sql failure %s",
           sqlite3_errmsg(sqlite_db));
  sqlite3_close(sqlite_db);
  return false;
}


std::string Database::GetLastErrorMsg() const {
  std::string msg = sqlite3_errmsg(sqlite_db_);
  return msg;
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
  result.root_hash = shash::MkFromHexPtr(shash::HexPtr(hash_str),
                                         shash::kSuffixCatalog);
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
