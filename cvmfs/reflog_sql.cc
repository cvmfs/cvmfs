/**
 * This file is part of the CernVM File System.
 */

#include "reflog_sql.h"

#include <cassert>
#include <limits>

#include "util/string.h"

const float ReflogDatabase::kLatestSchema = 1.0;
const float ReflogDatabase::kLatestSupportedSchema = 1.0;
const unsigned ReflogDatabase::kLatestSchemaRevision = 0;

/**
 * Database Schema ChangeLog:
 *
 * Schema Version 1.0
 *   -> Revision 0: initial revision
 */


const std::string ReflogDatabase::kFqrnKey = "fqrn";

bool ReflogDatabase::CreateEmptyDatabase() {
  return sqlite::Sql(sqlite_db(),
                     "CREATE TABLE refs (hash TEXT, type INTEGER, "
                     "timestamp INTEGER, "
                     "CONSTRAINT pk_refs PRIMARY KEY (hash));")
      .Execute();
}


bool ReflogDatabase::CheckSchemaCompatibility() {
  assert(IsEqualSchema(schema_version(), kLatestSupportedSchema));
  return true;  // only one schema version at the moment
}


bool ReflogDatabase::LiveSchemaUpgradeIfNecessary() {
  assert(schema_revision() == kLatestSchemaRevision);
  return true;  // only one schema revision at the moment, i.e. no migration...
}


bool ReflogDatabase::InsertInitialValues(const std::string &repo_name) {
  assert(read_write());
  return this->SetProperty(kFqrnKey, repo_name);
}


//------------------------------------------------------------------------------

#define DB_FIELDS_V1R0  "hash, type, timestamp"
#define DB_PLACEHOLDERS ":hash, :type, :timestamp"

#define MAKE_STATEMENT(STMT_TMPL, REV)                       \
  static const std::string REV = ReplaceAll(                 \
      ReplaceAll(STMT_TMPL, "@DB_FIELDS@", DB_FIELDS_##REV), \
      "@DB_PLACEHOLDERS@", DB_PLACEHOLDERS)

#define MAKE_STATEMENTS(STMT_TMPL) MAKE_STATEMENT(STMT_TMPL, V1R0)

#define DEFERRED_INIT(DB, REV) DeferredInit((DB)->sqlite_db(), (REV).c_str())

#define DEFERRED_INITS(DB) DEFERRED_INIT((DB), V1R0)


shash::Suffix SqlReflog::ToSuffix(const ReferenceType type) {
  switch (type) {
    case kRefCatalog:
      return shash::kSuffixCatalog;
    case kRefCertificate:
      return shash::kSuffixCertificate;
    case kRefHistory:
      return shash::kSuffixHistory;
    case kRefMetainfo:
      return shash::kSuffixMetainfo;
    default:
      assert(false && "unknown reference type");
  }
}


//------------------------------------------------------------------------------


SqlInsertReference::SqlInsertReference(const ReflogDatabase *database) {
  MAKE_STATEMENTS("INSERT OR REPLACE INTO refs (@DB_FIELDS@) "
                  "VALUES (@DB_PLACEHOLDERS@);");
  DEFERRED_INITS(database);
}

bool SqlInsertReference::BindReference(const shash::Any &reference_hash,
                                       const ReferenceType type) {
  return BindTextTransient(1, reference_hash.ToString())
         && BindInt64(2, static_cast<uint64_t>(type))
         && BindInt64(3, static_cast<uint64_t>(time(NULL)));
}


//------------------------------------------------------------------------------


SqlCountReferences::SqlCountReferences(const ReflogDatabase *database) {
  DeferredInit(database->sqlite_db(), "SELECT count(*) as count FROM refs;");
}

uint64_t SqlCountReferences::RetrieveCount() {
  return static_cast<uint64_t>(RetrieveInt64(0));
}


//------------------------------------------------------------------------------


SqlListReferences::SqlListReferences(const ReflogDatabase *database) {
  DeferredInit(database->sqlite_db(), "SELECT hash, type FROM refs "
                                      "WHERE type = :type AND "
                                      "timestamp < :timestamp "
                                      "ORDER BY timestamp DESC;");
}

bool SqlListReferences::BindType(const ReferenceType type) {
  return BindInt64(1, static_cast<uint64_t>(type));
}

bool SqlListReferences::BindOlderThan(const uint64_t timestamp) {
  int64_t sqlite_timestamp = static_cast<uint64_t>(timestamp);
  if (sqlite_timestamp < 0) {
    sqlite_timestamp = std::numeric_limits<int64_t>::max();
  }
  return BindInt64(2, sqlite_timestamp);
}

shash::Any SqlListReferences::RetrieveHash() const {
  const ReferenceType type = static_cast<ReferenceType>(RetrieveInt64(1));
  const shash::Suffix suffix = ToSuffix(type);
  return shash::MkFromHexPtr(shash::HexPtr(RetrieveString(0)), suffix);
}


//------------------------------------------------------------------------------


SqlRemoveReference::SqlRemoveReference(const ReflogDatabase *database) {
  DeferredInit(database->sqlite_db(), "DELETE FROM refs WHERE hash = :hash "
                                      "AND type = :type;");
}

bool SqlRemoveReference::BindReference(const shash::Any &reference_hash,
                                       const ReferenceType type) {
  return BindTextTransient(1, reference_hash.ToString())
         && BindInt64(2, static_cast<uint64_t>(type));
}


//------------------------------------------------------------------------------


SqlContainsReference::SqlContainsReference(const ReflogDatabase *database) {
  DeferredInit(database->sqlite_db(), "SELECT count(*) as answer FROM refs "
                                      "WHERE type = :type "
                                      "  AND hash = :hash");
}

bool SqlContainsReference::BindReference(const shash::Any &reference_hash,
                                         const ReferenceType type) {
  return BindInt64(1, static_cast<uint64_t>(type))
         && BindTextTransient(2, reference_hash.ToString());
}

bool SqlContainsReference::RetrieveAnswer() {
  const int64_t count = RetrieveInt64(0);
  assert(count == 0 || count == 1);
  return count > 0;
}


//------------------------------------------------------------------------------


SqlGetTimestamp::SqlGetTimestamp(const ReflogDatabase *database) {
  DeferredInit(database->sqlite_db(), "SELECT timestamp FROM refs "
                                      "WHERE type = :type "
                                      "  AND hash = :hash");
}

bool SqlGetTimestamp::BindReference(const shash::Any &reference_hash,
                                    const ReferenceType type) {
  return BindInt64(1, static_cast<uint64_t>(type))
         && BindTextTransient(2, reference_hash.ToString());
}

uint64_t SqlGetTimestamp::RetrieveTimestamp() { return RetrieveInt64(0); }
