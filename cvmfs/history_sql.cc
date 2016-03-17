/**
 * This file is part of the CernVM File System.
 */

#include "history_sql.h"

#include <cassert>

#include "util/string.h"

namespace history {

const float    HistoryDatabase::kLatestSchema          = 1.0;
const float    HistoryDatabase::kLatestSupportedSchema = 1.0;
const unsigned HistoryDatabase::kLatestSchemaRevision  = 2;

/**
 * Database Schema ChangeLog:
 *
 * Schema Version 1.0
 *   -> Revision 2: add table 'recycle_bin'
 *   -> Revision 1: add field 'size'
 *
 */

const std::string HistoryDatabase::kFqrnKey = "fqrn";


/**
 * This method creates a new database file and initializes the database schema.
 */
bool HistoryDatabase::CreateEmptyDatabase() {
  assert(read_write());

  return CreateTagsTable() &&
         CreateRecycleBinTable();
}


bool HistoryDatabase::CreateTagsTable() {
  assert(read_write());
  return sqlite::Sql(sqlite_db(),
    "CREATE TABLE tags (name TEXT, hash TEXT, revision INTEGER, "
    "  timestamp INTEGER, channel INTEGER, description TEXT, size INTEGER, "
    "  CONSTRAINT pk_tags PRIMARY KEY (name))").Execute();
}


bool HistoryDatabase::CreateRecycleBinTable() {
  assert(read_write());
  return sqlite::Sql(sqlite_db(),
    "CREATE TABLE recycle_bin (hash TEXT, flags INTEGER, "
    "  CONSTRAINT pk_hash PRIMARY KEY (hash))").Execute();
}


bool HistoryDatabase::InsertInitialValues(const std::string &repository_name) {
  assert(read_write());
  return this->SetProperty(kFqrnKey, repository_name);
}


bool HistoryDatabase::ContainsRecycleBin() const {
  return schema_version() >= 1.0 - kSchemaEpsilon && schema_revision() >= 2;
}


bool HistoryDatabase::CheckSchemaCompatibility() {
  return !((schema_version() < kLatestSupportedSchema - kSchemaEpsilon) ||
           (schema_version() > kLatestSchema          + kSchemaEpsilon));
}


bool HistoryDatabase::LiveSchemaUpgradeIfNecessary() {
  assert(read_write());
  assert(IsEqualSchema(schema_version(), 1.0));

  if (schema_revision() == kLatestSchemaRevision) {
    return true;
  }

  LogCvmfs(kLogHistory, kLogDebug, "upgrading history schema revision "
                                   "%.2f (Rev: %d) to %.2f (Rev: %d)",
           schema_version(), schema_revision(),
           kLatestSchema, kLatestSchemaRevision);

  const bool success = UpgradeSchemaRevision_10_1() &&
                       UpgradeSchemaRevision_10_2();

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


bool HistoryDatabase::UpgradeSchemaRevision_10_2() {
  if (schema_revision() > 1) {
    return true;
  }

  if (!CreateRecycleBinTable()) {
    LogCvmfs(kLogHistory, kLogStderr, "failed to upgrade history database");
    return false;
  }

  set_schema_revision(2);
  return true;
}


//------------------------------------------------------------------------------

#define DB_FIELDS_V1R0  "name, hash, revision, timestamp, channel, description"
#define DB_FIELDS_V1R1  "name, hash, revision, timestamp, channel, " \
                        "description,  size"
#define DB_PLACEHOLDERS ":name, :hash, :revision, :timestamp, :channel, " \
                        ":description, :size"
#define ROLLBACK_COND   "(revision > :target_rev  OR " \
                        " name     = :target_name)   " \
                        "AND channel  = :target_chan "

#define MAKE_STATEMENT(STMT_TMPL, REV)       \
static const std::string REV =               \
  ReplaceAll(                                \
    ReplaceAll(                              \
      ReplaceAll(STMT_TMPL,                  \
        "@DB_FIELDS@", DB_FIELDS_ ## REV),   \
      "@DB_PLACEHOLDERS@", DB_PLACEHOLDERS), \
    "@ROLLBACK_COND@", ROLLBACK_COND)

#define MAKE_STATEMENTS(STMT_TMPL) \
  MAKE_STATEMENT(STMT_TMPL, V1R0); \
  MAKE_STATEMENT(STMT_TMPL, V1R1)

#define DEFERRED_INIT(DB, REV) \
  DeferredInit((DB)->sqlite_db(), (REV).c_str())

#define DEFERRED_INITS(DB) \
  if ((DB)->IsEqualSchema((DB)->schema_version(), 1.0f) && \
      (DB)->schema_revision() == 0) {                      \
    DEFERRED_INIT((DB), V1R0);                             \
  } else {                                                 \
    DEFERRED_INIT((DB), V1R1);                             \
  }

SqlInsertTag::SqlInsertTag(const HistoryDatabase *database) {
  MAKE_STATEMENTS("INSERT INTO tags (@DB_FIELDS@) VALUES (@DB_PLACEHOLDERS@);");
  DEFERRED_INITS(database);
}


bool SqlInsertTag::BindTag(const History::Tag &tag) {
  return
    BindText(1, tag.name) &&
    BindTextTransient(2, tag.root_hash.ToString()) &&  // temporary (ToString)
    BindInt64(3, tag.revision) &&
    BindInt64(4, tag.timestamp) &&
    BindInt64(5, tag.channel) &&
    BindText(6, tag.description) &&
    BindInt64(7, tag.size);
}


//------------------------------------------------------------------------------


SqlRemoveTag::SqlRemoveTag(const HistoryDatabase *database) {
  DeferredInit(database->sqlite_db(), "DELETE FROM tags WHERE name = :name;");
}

bool SqlRemoveTag::BindName(const std::string &name) {
  return BindText(1, name);
}


//------------------------------------------------------------------------------


SqlFindTag::SqlFindTag(const HistoryDatabase *database) {
  MAKE_STATEMENTS("SELECT @DB_FIELDS@ FROM tags WHERE name = :name LIMIT 1;");
  DEFERRED_INITS(database);
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
  MAKE_STATEMENTS("SELECT @DB_FIELDS@ FROM tags "
                  "WHERE timestamp <= :timestamp "
                  "ORDER BY revision DESC LIMIT 1;");
  DEFERRED_INITS(database);
}

bool SqlFindTagByDate::BindTimestamp(const time_t timestamp) {
  return BindInt64(1, timestamp);
}


//------------------------------------------------------------------------------


SqlCountTags::SqlCountTags(const HistoryDatabase *database) {
  DeferredInit(database->sqlite_db(), "SELECT count(*) FROM tags;");
}

unsigned SqlCountTags::RetrieveCount() const {
  int64_t count = RetrieveInt64(0);
  assert(count >= 0);
  return static_cast<uint64_t>(count);
}


//------------------------------------------------------------------------------


SqlListTags::SqlListTags(const HistoryDatabase *database) {
  MAKE_STATEMENTS("SELECT @DB_FIELDS@ FROM tags ORDER BY revision DESC;");
  DEFERRED_INITS(database);
}


//------------------------------------------------------------------------------


SqlGetChannelTips::SqlGetChannelTips(const HistoryDatabase *database) {
  MAKE_STATEMENTS("SELECT @DB_FIELDS@, MAX(revision) AS max_rev "
                  "FROM tags "
                  "GROUP BY channel;");
  DEFERRED_INITS(database);
}

SqlGetHashes::SqlGetHashes(const HistoryDatabase *database) {
  DeferredInit(database->sqlite_db(), "SELECT DISTINCT hash FROM tags "
                                      "ORDER BY revision ASC");
}

shash::Any SqlGetHashes::RetrieveHash() const {
  return shash::MkFromHexPtr(shash::HexPtr(RetrieveString(0)),
                             shash::kSuffixCatalog);
}


//------------------------------------------------------------------------------


SqlRollbackTag::SqlRollbackTag(const HistoryDatabase *database) {
  MAKE_STATEMENTS("DELETE FROM tags WHERE @ROLLBACK_COND@;");
  DEFERRED_INITS(database);
}


//------------------------------------------------------------------------------


SqlListRollbackTags::SqlListRollbackTags(const HistoryDatabase *database) {
  MAKE_STATEMENTS("SELECT @DB_FIELDS@ FROM tags "
                  "WHERE @ROLLBACK_COND@ "
                  "ORDER BY revision DESC;");
  DEFERRED_INITS(database);
}


//------------------------------------------------------------------------------


bool SqlRecycleBin::CheckSchema(const HistoryDatabase *database) const {
  return (database->IsEqualSchema(database->schema_version(), 1.0)) &&
         (database->schema_revision() >= 2);
}


//------------------------------------------------------------------------------


SqlRecycleBinInsert::SqlRecycleBinInsert(const HistoryDatabase *database) {
  assert(CheckSchema(database));
  DeferredInit(database->sqlite_db(),
               "INSERT OR IGNORE INTO recycle_bin (hash, flags) "
               "VALUES (:hash, :flags)");
}


bool SqlRecycleBinInsert::BindTag(const History::Tag &condemned_tag) {
  const unsigned int flags = SqlRecycleBin::kFlagCatalog;
  return
    BindTextTransient(1, condemned_tag.root_hash.ToString()) &&
    BindInt64(2, flags);
}


//------------------------------------------------------------------------------


SqlRecycleBinList::SqlRecycleBinList(const HistoryDatabase *database) {
  assert(CheckSchema(database));
  DeferredInit(database->sqlite_db(), "SELECT hash, flags FROM recycle_bin;");
}


shash::Any SqlRecycleBinList::RetrieveHash() {
  const unsigned int flags = RetrieveInt64(1);
  shash::Suffix suffix = shash::kSuffixNone;
  if (flags & SqlRecycleBin::kFlagCatalog) {
    suffix = shash::kSuffixCatalog;
  }

  return shash::MkFromHexPtr(shash::HexPtr(RetrieveString(0)), suffix);
}


//------------------------------------------------------------------------------


SqlRecycleBinFlush::SqlRecycleBinFlush(const HistoryDatabase *database) {
  assert(CheckSchema(database));
  DeferredInit(database->sqlite_db(), "DELETE FROM recycle_bin;");
}


//------------------------------------------------------------------------------


SqlRecycleBinRollback::SqlRecycleBinRollback(const HistoryDatabase *database) {
  assert(CheckSchema(database));
  const bool success = Init(database->sqlite_db(),
                            "INSERT OR IGNORE INTO recycle_bin (hash, flags) "
                            "SELECT hash, :flags "
                            "FROM tags WHERE " + rollback_condition + ";");
  assert(success);
}

bool SqlRecycleBinRollback::BindFlags() {
  return BindInt64(1, SqlRecycleBin::kFlagCatalog);
}


}; /* namespace history */
