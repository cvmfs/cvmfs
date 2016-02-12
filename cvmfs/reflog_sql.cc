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
                     "CREATE TABLE refs (hash      TEXT, "
                     "                   timestamp INTEGER,"
                     "                   flags     INTEGER);").Execute();
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
