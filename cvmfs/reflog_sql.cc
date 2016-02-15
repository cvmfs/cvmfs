/**
 * This file is part of the CernVM File System.
 */

#include "reflog_sql.h"

#include <cassert>

const float    ReflogDatabase::kLatestSchema          = 1.0;
const float    ReflogDatabase::kLatestSupportedSchema = 1.0;
const unsigned ReflogDatabase::kLatestSchemaRevision  = 0;

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
                     "CONSTRAINT pk_refs PRIMARY KEY (hash));").Execute();
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


std::string SqlReflog::db_fields(const ReflogDatabase *database) const {
  // only one schema revision/version currently... this might change
  return "hash, type, timestamp";
}

SqlInsertReference::SqlInsertReference(const ReflogDatabase *database) {
  const std::string stmt =
    "INSERT OR IGNORE INTO refs (" + db_fields(database) + ") "
    "VALUES (:hash, :type, :timestamp);";
  const bool success = Init(database->sqlite_db(), stmt);
  assert(success);
}

bool SqlInsertReference::BindReference(const shash::Any    &reference_hash,
                                       const ReferenceType  type) {
  return
    BindTextTransient(1, reference_hash.ToString()) &&
    BindInt64(2, static_cast<uint64_t>(type))       &&
    BindInt64(3, static_cast<uint64_t>(time(NULL)));
}
