/**
 * This file is part of the CernVM File System.
 */

#include "history_sql.h"


namespace history {

const float    HistoryDatabase::kLatestSchema          = 1.0;
const float    HistoryDatabase::kLatestSupportedSchema = 1.0;
const unsigned HistoryDatabase::kLatestSchemaRevision  = 1;

const std::string HistoryDatabase::kFqrnKey = "fqrn";


/**
 * This method creates a new database file and initializes the database schema.
 */
bool HistoryDatabase::CreateEmptyDatabase() {
  assert (read_write());

  return CreateTagsTable();
}


bool HistoryDatabase::CreateTagsTable() {
  assert (read_write());
  return sqlite::Sql(sqlite_db(),
    "CREATE TABLE tags (name TEXT, hash TEXT, revision INTEGER, "
    "  timestamp INTEGER, channel INTEGER, description TEXT, size INTEGER, "
    "  CONSTRAINT pk_tags PRIMARY KEY (name))").Execute();
}


bool HistoryDatabase::InsertInitialValues(const std::string &repository_name) {
  assert (read_write());
  return this->SetProperty(kFqrnKey, repository_name);
}


bool HistoryDatabase::CheckSchemaCompatibility() {
  return ! ((schema_version() < kLatestSupportedSchema - kSchemaEpsilon) ||
            (schema_version() > kLatestSchema          + kSchemaEpsilon));
}


bool HistoryDatabase::LiveSchemaUpgradeIfNecessary() {
  assert (read_write());
  assert (IsEqualSchema(schema_version(), 1.0));

  if (schema_revision() == kLatestSchemaRevision) {
    return true;
  }

  LogCvmfs(kLogHistory, kLogDebug, "upgrading history schema revision "
                                   "%.2f (Rev: %d) to %.2f (Rev: %d)",
           schema_version(), schema_revision(),
           kLatestSchema, kLatestSchemaRevision);

  const bool success = UpgradeSchemaRevision_10_1();

  return success && StoreSchemaRevision();
}


bool HistoryDatabase::UpgradeSchemaRevision_10_1() {
  if (schema_revision() > 0) {
    return true;
  }

  sqlite::Sql sql_upgrade(sqlite_db(), "ALTER TABLE tags ADD size INTEGER;");
  if (!sql_upgrade.Execute()) {
    LogCvmfs(kLogHistory, kLogStderr, "failed to upgrade tags table");
    return false;
  }

  set_schema_revision(1);
  return true;
}


//------------------------------------------------------------------------------


const std::string SqlRetrieveTag::tag_database_fields =
  "name, hash, revision, timestamp, channel, description, size";

const std::string& SqlRetrieveTag::GetDatabaseFields() const {
  return SqlRetrieveTag::tag_database_fields;
}

History::Tag SqlRetrieveTag::RetrieveTag() const {
  History::Tag result;
  result.name        = RetrieveString(0);
  result.root_hash   = shash::MkFromHexPtr(shash::HexPtr(RetrieveString(1)),
                                           shash::kSuffixCatalog);
  result.revision    = RetrieveInt64(2);
  result.timestamp   = RetrieveInt64(3);
  result.channel     = static_cast<History::UpdateChannel>(RetrieveInt64(4));
  result.description = RetrieveString(5);
  result.size        = RetrieveInt64(6);
  return result;
}


//------------------------------------------------------------------------------


SqlInsertTag::SqlInsertTag(const HistoryDatabase *database) {
  const std::string stmt =
    "INSERT INTO tags (" + GetDatabaseFields() + ")"
    "VALUES (" + GetDatabasePlaceholders() + ");";
  const bool success = Init(database->sqlite_db(), stmt);
  assert (success);
}

const std::string SqlInsertTag::tag_database_placeholders =
  ":name, :hash, :revision, :timestamp, :channel, :description, :size";

const std::string& SqlInsertTag::GetDatabasePlaceholders() const {
  return SqlInsertTag::tag_database_placeholders;
}

bool SqlInsertTag::BindTag(const History::Tag &tag) {
  return (
    BindText         (1, tag.name)                 &&
    BindTextTransient(2, tag.root_hash.ToString()) && // temporary from ToString
    BindInt64        (3, tag.revision)             &&
    BindInt64        (4, tag.timestamp)            &&
    BindInt64        (5, tag.channel)              &&
    BindText         (6, tag.description)          &&
    BindInt64        (7, tag.size)
  );
}


//------------------------------------------------------------------------------


SqlRemoveTag::SqlRemoveTag(const HistoryDatabase *database) {
  const std::string stmt = "DELETE FROM tags WHERE name = :name;";
  const bool success = Init(database->sqlite_db(), stmt);
  assert (success);
}

bool SqlRemoveTag::BindName(const std::string &name) {
  return BindText(1, name);
}


//------------------------------------------------------------------------------


SqlFindTag::SqlFindTag(const HistoryDatabase *database) {
  const std::string stmt =
    "SELECT " + GetDatabaseFields() + " FROM tags "
    "WHERE name = :name LIMIT 1;";
  const bool success = Init(database->sqlite_db(), stmt);
  assert (success);
}

bool SqlFindTag::BindName(const std::string &name) {
  return BindText(1, name);
}


//------------------------------------------------------------------------------


SqlFindTagByDate::SqlFindTagByDate(const HistoryDatabase *database) {
  // figure out the tag that was HEAD to a given point in time
  //
  // conceptually goes back in the revision history  |  ORDER BY revision DESC
  // and picks the first tag                         |  LIMIT 1
  // that is older than the given timestamp          |  WHICH timestamp <= :ts
  const bool success = Init(database->sqlite_db(),
                            "SELECT " + GetDatabaseFields() + " FROM tags "
                            "WHERE timestamp <= :timestamp "
                            "ORDER BY revision DESC LIMIT 1;");
  assert (success);
}

bool SqlFindTagByDate::BindTimestamp(const time_t timestamp) {
  return BindInt64(1, timestamp);
}


//------------------------------------------------------------------------------


SqlCountTags::SqlCountTags(const HistoryDatabase *database) {
  const bool success = Init(database->sqlite_db(),
                            "SELECT count(*) FROM tags;");
  assert (success);
}

int SqlCountTags::RetrieveCount() const {
  return RetrieveInt64(0);
}


//------------------------------------------------------------------------------


SqlListTags::SqlListTags(const HistoryDatabase *database) {
  const bool success = Init(database->sqlite_db(),
                            "SELECT " + GetDatabaseFields() + " FROM tags "
                            "ORDER BY revision DESC;");
  assert (success);
}


//------------------------------------------------------------------------------


SqlGetChannelTips::SqlGetChannelTips(const HistoryDatabase *database) {
  const bool success = Init(database->sqlite_db(),
                            "SELECT " + GetDatabaseFields() + ", "
                            "  MAX(revision) AS max_rev "
                            "FROM tags "
                            "GROUP BY channel;");
  assert (success);
}

SqlGetHashes::SqlGetHashes(const HistoryDatabase *database) {
  const bool success = Init(database->sqlite_db(),
                            "SELECT DISTINCT hash FROM tags "
                            "ORDER BY revision ASC");
  assert (success);
}

shash::Any SqlGetHashes::RetrieveHash() const {
  return shash::MkFromHexPtr(shash::HexPtr(RetrieveString(0)),
                             shash::kSuffixCatalog);
}


//------------------------------------------------------------------------------


bool SqlRollback::BindTargetTag(const History::Tag &target_tag) {
  return BindInt64(1, target_tag.revision) &&
         BindText (2, target_tag.name)     &&
         BindInt64(3, target_tag.channel);
}

const std::string SqlRollback::rollback_condition =
                                             "(revision > :target_rev  OR "
                                             " name     = :target_name)   "
                                             "AND channel  = :target_chan ";

std::string SqlRollback::GetRollbackCondition() const {
  return SqlRollback::rollback_condition;
}


//------------------------------------------------------------------------------


SqlRollbackTag::SqlRollbackTag(const HistoryDatabase *database) {
  const bool success = Init(database->sqlite_db(),
                            "DELETE FROM tags WHERE "
                             + GetRollbackCondition() + ";");
  assert (success);
}


//------------------------------------------------------------------------------


SqlListRollbackTags::SqlListRollbackTags(const HistoryDatabase *database) {
  const bool success = Init(database->sqlite_db(),
                            "SELECT " + GetDatabaseFields() + " FROM tags "
                            "WHERE " + GetRollbackCondition() + " "
                            "ORDER BY revision DESC;");
  assert (success);
}

}; /* namespace history */
