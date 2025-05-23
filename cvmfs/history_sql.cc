/**
 * This file is part of the CernVM File System.
 */

#include "history_sql.h"

#include <cassert>

#include "util/string.h"

namespace history {

const float HistoryDatabase::kLatestSchema = 1.0;
const float HistoryDatabase::kLatestSupportedSchema = 1.0;
const unsigned HistoryDatabase::kLatestSchemaRevision = 3;

/**
 * Database Schema ChangeLog:
 *
 * Schema Version 1.0
 *   -> Revision 3: deprecate (flush) table 'recycle_bin'
 *                  add table 'branches'
 *                  add column 'branch' to table tags
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

  sqlite::Sql sql_foreign_keys(sqlite_db(), "PRAGMA foreign_keys = ON;");
  if (!sql_foreign_keys.Execute())
    return false;

  return CreateBranchesTable() && CreateTagsTable() && CreateRecycleBinTable();
}


bool HistoryDatabase::CreateTagsTable() {
  assert(read_write());
  return sqlite::Sql(
             sqlite_db(),
             "CREATE TABLE tags (name TEXT, hash TEXT, revision INTEGER, "
             "  timestamp INTEGER, channel INTEGER, description TEXT, size "
             "INTEGER, "
             "  branch TEXT, CONSTRAINT pk_tags PRIMARY KEY (name), "
             "  FOREIGN KEY (branch) REFERENCES branches (branch));")
      .Execute();
}


bool HistoryDatabase::CreateRecycleBinTable() {
  assert(read_write());
  return sqlite::Sql(sqlite_db(),
                     "CREATE TABLE recycle_bin (hash TEXT, flags INTEGER, "
                     "  CONSTRAINT pk_hash PRIMARY KEY (hash))")
      .Execute();
}


bool HistoryDatabase::CreateBranchesTable() {
  assert(read_write());

  sqlite::Sql sql_create(sqlite_db(),
                         "CREATE TABLE branches (branch TEXT, parent TEXT, "
                         "initial_revision INTEGER,"
                         "  CONSTRAINT pk_branch PRIMARY KEY (branch), "
                         "  FOREIGN KEY (parent) REFERENCES branches (branch), "
                         "  CHECK ((branch <> '') OR (parent IS NULL)), "
                         "  CHECK ((branch = '') OR (parent IS NOT NULL)));");
  bool retval = sql_create.Execute();
  if (!retval)
    return false;

  sqlite::Sql sql_init(
      sqlite_db(),
      "INSERT INTO branches (branch, parent, initial_revision) "
      "VALUES ('', NULL, 0);");
  retval = sql_init.Execute();
  return retval;
}


bool HistoryDatabase::InsertInitialValues(const std::string &repository_name) {
  assert(read_write());
  return this->SetProperty(kFqrnKey, repository_name);
}


bool HistoryDatabase::ContainsRecycleBin() const {
  return schema_version() >= 1.0 - kSchemaEpsilon && schema_revision() >= 2;
}


bool HistoryDatabase::CheckSchemaCompatibility() {
  return !((schema_version() < kLatestSupportedSchema - kSchemaEpsilon)
           || (schema_version() > kLatestSchema + kSchemaEpsilon));
}


bool HistoryDatabase::LiveSchemaUpgradeIfNecessary() {
  assert(read_write());
  assert(IsEqualSchema(schema_version(), 1.0));

  sqlite::Sql sql_foreign_keys(sqlite_db(), "PRAGMA foreign_keys = ON;");
  if (!sql_foreign_keys.Execute())
    return false;
  if (schema_revision() == kLatestSchemaRevision) {
    return true;
  }

  LogCvmfs(kLogHistory, kLogDebug,
           "upgrading history schema revision "
           "%.2f (Rev: %d) to %.2f (Rev: %d)",
           schema_version(), schema_revision(), kLatestSchema,
           kLatestSchemaRevision);

  const bool success = UpgradeSchemaRevision_10_1()
                       && UpgradeSchemaRevision_10_2()
                       && UpgradeSchemaRevision_10_3();

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


bool HistoryDatabase::UpgradeSchemaRevision_10_3() {
  if (schema_revision() > 2) {
    return true;
  }

  if (!CreateBranchesTable()) {
    LogCvmfs(kLogHistory, kLogStderr, "failed to create branches table");
    return false;
  }

  sqlite::Sql sql_upgrade(
      sqlite_db(),
      "ALTER TABLE tags ADD branch TEXT REFERENCES branches (branch);");
  if (!sql_upgrade.Execute()) {
    LogCvmfs(kLogHistory, kLogStderr, "failed to upgrade tags table");
    return false;
  }

  sqlite::Sql sql_fill(sqlite_db(), "UPDATE tags SET branch = '';");
  if (!sql_fill.Execute()) {
    LogCvmfs(kLogHistory, kLogStderr, "failed to set branch default value");
    return false;
  }

  // We keep the table in the schema for backwards compatibility
  sqlite::Sql sql_flush(sqlite_db(), "DELETE FROM recycle_bin; VACUUM;");
  if (!sql_flush.Execute()) {
    LogCvmfs(kLogHistory, kLogStderr, "failed to flush recycle bin table");
    return false;
  }

  set_schema_revision(3);
  return true;
}


//------------------------------------------------------------------------------

#define DB_FIELDS_V1R0                         \
  "name, hash, revision, timestamp, channel, " \
  "description, 0, ''"
#define DB_FIELDS_V1R1                         \
  "name, hash, revision, timestamp, channel, " \
  "description, size, ''"
#define DB_FIELDS_V1R3                         \
  "name, hash, revision, timestamp, channel, " \
  "description, size, branch"
#define DB_PLACEHOLDERS                             \
  ":name, :hash, :revision, :timestamp, :channel, " \
  ":description, :size, :branch"
#define ROLLBACK_COND            \
  "(revision > :target_rev  OR " \
  " name = :target_name) "       \
  "AND branch = ''"

#define MAKE_STATEMENT(STMT_TMPL, REV)                                  \
  static const std::string REV = ReplaceAll(                            \
      ReplaceAll(ReplaceAll(STMT_TMPL, "@DB_FIELDS@", DB_FIELDS_##REV), \
                 "@DB_PLACEHOLDERS@", DB_PLACEHOLDERS),                 \
      "@ROLLBACK_COND@", ROLLBACK_COND)

#define MAKE_STATEMENTS(STMT_TMPL) \
  MAKE_STATEMENT(STMT_TMPL, V1R0); \
  MAKE_STATEMENT(STMT_TMPL, V1R1); \
  MAKE_STATEMENT(STMT_TMPL, V1R3)

#define DEFERRED_INIT(DB, REV) DeferredInit((DB)->sqlite_db(), (REV).c_str())

#define DEFERRED_INITS(DB)                              \
  if ((DB)->IsEqualSchema((DB)->schema_version(), 1.0f) \
      && (DB)->schema_revision() == 0) {                \
    DEFERRED_INIT((DB), V1R0);                          \
  } else if ((DB)->schema_revision() < 3) {             \
    DEFERRED_INIT((DB), V1R1);                          \
  } else {                                              \
    DEFERRED_INIT((DB), V1R3);                          \
  }

SqlInsertTag::SqlInsertTag(const HistoryDatabase *database) {
  MAKE_STATEMENTS("INSERT INTO tags (@DB_FIELDS@) VALUES (@DB_PLACEHOLDERS@);");
  DEFERRED_INITS(database);
}


bool SqlInsertTag::BindTag(const History::Tag &tag) {
  return BindText(1, tag.name) && BindTextTransient(2, tag.root_hash.ToString())
         &&  // temporary (ToString)
         BindInt64(3, tag.revision) && BindInt64(4, tag.timestamp) &&
         // Channels are no longer supported: store 0 (i.e. kChannelTrunk)
         // for backwards compatibility with existing databases
         //
         BindInt64(5, 0) && BindText(6, tag.description)
         && BindInt64(7, tag.size) && BindText(8, tag.branch);
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
  MAKE_STATEMENTS("SELECT @DB_FIELDS@ FROM tags WHERE name = :name;");
  DEFERRED_INITS(database);
}

bool SqlFindTag::BindName(const std::string &name) { return BindText(1, name); }


//------------------------------------------------------------------------------


SqlFindTagByDate::SqlFindTagByDate(const HistoryDatabase *database) {
  // figure out the tag that was HEAD to a given point in time
  //
  // conceptually goes back in the revision history  |  ORDER BY revision DESC
  // and picks the first tag                         |  LIMIT 1
  // that is older than the given timestamp          |  WHICH timestamp <= :ts
  MAKE_STATEMENTS("SELECT @DB_FIELDS@ FROM tags "
                  "WHERE (branch = '') AND (timestamp <= :timestamp) "
                  "ORDER BY revision DESC LIMIT 1;");
  DEFERRED_INITS(database);
}

bool SqlFindTagByDate::BindTimestamp(const time_t timestamp) {
  return BindInt64(1, timestamp);
}


//------------------------------------------------------------------------------


SqlFindBranchHead::SqlFindBranchHead(const HistoryDatabase *database) {
  // One of the tags with the highest revision on a given branch
  // Doesn't work on older database revisions
  MAKE_STATEMENTS("SELECT @DB_FIELDS@ FROM tags "
                  "WHERE (branch = :branch) "
                  "ORDER BY revision DESC LIMIT 1;");
  DEFERRED_INITS(database);
}

bool SqlFindBranchHead::BindBranchName(const std::string &branch_name) {
  return BindText(1, branch_name);
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
  MAKE_STATEMENTS(
      "SELECT @DB_FIELDS@ FROM tags ORDER BY timestamp DESC, revision DESC;");
  DEFERRED_INITS(database);
}


//------------------------------------------------------------------------------


SqlGetHashes::SqlGetHashes(const HistoryDatabase *database) {
  DeferredInit(database->sqlite_db(), "SELECT DISTINCT hash FROM tags "
                                      "ORDER BY timestamp, revision ASC");
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


SqlListBranches::SqlListBranches(const HistoryDatabase *database) {
  if (database->schema_revision() < 3)
    DeferredInit(database->sqlite_db(), "SELECT '', NULL, 0;");
  else
    DeferredInit(database->sqlite_db(),
                 "SELECT branch, parent, initial_revision FROM branches;");
}


History::Branch SqlListBranches::RetrieveBranch() const {
  std::string branch = RetrieveString(0);
  std::string parent = (RetrieveType(1) == SQLITE_NULL) ? ""
                                                        : RetrieveString(1);
  unsigned initial_revision = RetrieveInt64(2);
  return History::Branch(branch, parent, initial_revision);
}


//------------------------------------------------------------------------------


SqlInsertBranch::SqlInsertBranch(const HistoryDatabase *database) {
  DeferredInit(database->sqlite_db(),
               "INSERT INTO branches (branch, parent, initial_revision) "
               "VALUES (:branch, :parent, :initial_revision);");
}


bool SqlInsertBranch::BindBranch(const History::Branch &branch) {
  return BindText(1, branch.branch) && BindText(2, branch.parent)
         && BindInt64(3, branch.initial_revision);
}


//------------------------------------------------------------------------------


bool SqlRecycleBin::CheckSchema(const HistoryDatabase *database) const {
  return (database->IsEqualSchema(database->schema_version(), 1.0))
         && (database->schema_revision() >= 2);
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


}; /* namespace history */
